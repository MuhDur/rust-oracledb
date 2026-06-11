#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use asupersync::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use asupersync::net::{OwnedReadHalf, OwnedWriteHalf, TcpStream};
use asupersync::runtime::{reactor, Runtime, RuntimeBuilder};
use asupersync::sync::Mutex as AsyncMutex;
use asupersync::{time, Cx};
use oracledb_protocol::thin::{
    build_auth_phase_two_payload_with_context_with_seq, build_connect_packet_payload,
    build_define_fetch_payload_with_seq, build_execute_payload_with_bind_rows_with_seq,
    build_execute_payload_with_binds_with_seq, build_execute_payload_with_seq,
    build_fast_auth_phase_one_payload, build_fetch_payload_with_seq,
    build_function_payload_with_seq, build_lob_create_temp_payload_with_seq,
    build_lob_free_temp_payload_with_seq, build_lob_read_payload_with_seq,
    build_lob_trim_payload_with_seq, build_lob_write_payload_with_seq, parse_accept_payload,
    parse_auth_response, parse_fetch_response_with_context, parse_lob_create_temp_response,
    parse_lob_free_temp_response, parse_lob_read_response, parse_lob_trim_response,
    parse_lob_write_response, parse_query_response, parse_query_response_with_binds, BindValue,
    ClientCapabilities, ColumnMetadata, LobReadResult, QueryResult, TNS_FUNC_COMMIT,
    TNS_FUNC_LOGOFF, TNS_FUNC_PING, TNS_FUNC_ROLLBACK, TNS_MSG_TYPE_END_OF_RESPONSE,
    TNS_MSG_TYPE_FLUSH_OUT_BINDS, TNS_PACKET_TYPE_ACCEPT, TNS_PACKET_TYPE_CONNECT,
    TNS_PACKET_TYPE_DATA, TNS_PACKET_TYPE_REDIRECT, TNS_PACKET_TYPE_REFUSE,
};
use oracledb_protocol::wire::{encode_packet, PacketLengthWidth};
use oracledb_protocol::{net::EasyConnect, ClientIdentity};

const PYTHON_ORACLEDB_COMPAT_VERSION_NUM: u32 = 0x0400_1000;
const DEFAULT_SDU: usize = 8192;
const TNS_DATA_PACKET_OVERHEAD: usize = 10;

pub use oracledb_protocol as protocol;

type SharedWriteHalf = Arc<AsyncMutex<OwnedWriteHalf>>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Protocol(#[from] oracledb_protocol::ProtocolError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("asupersync runtime error: {0}")]
    Runtime(String),
    #[error("listener redirected this connection; redirect handling is not implemented yet")]
    RedirectUnsupported,
    #[error("listener refused connection: {0}")]
    ListenerRefused(String),
    #[error("server did not advertise fast authentication")]
    FastAuthRequired,
    #[error("server response did not contain {0}")]
    MissingSessionField(&'static str),
    #[error("call timeout of {0} ms exceeded")]
    CallTimeout(u32),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub struct ConnectOptions {
    pub connect_string: String,
    pub user: String,
    pub password: String,
    pub identity: ClientIdentity,
    pub app_context: Vec<(String, String, String)>,
    pub sdu: u16,
}

impl ConnectOptions {
    pub fn new(
        connect_string: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
        identity: ClientIdentity,
    ) -> Self {
        Self {
            connect_string: connect_string.into(),
            user: user.into(),
            password: password.into(),
            identity,
            app_context: Vec::new(),
            sdu: 8192,
        }
    }

    pub fn with_app_context(mut self, app_context: Vec<(String, String, String)>) -> Self {
        self.app_context = app_context;
        self
    }

    pub fn with_sdu(mut self, sdu: u32) -> Self {
        let clamped = sdu.clamp(512, u32::from(u16::MAX));
        self.sdu = u16::try_from(clamped).unwrap_or(u16::MAX);
        self
    }
}

#[derive(Debug)]
pub struct Connection {
    descriptor: EasyConnect,
    identity: ClientIdentity,
    read: OwnedReadHalf,
    write: SharedWriteHalf,
    session_id: u32,
    serial_num: u16,
    server_version: Option<String>,
    capabilities: ClientCapabilities,
    ttc_seq_num: u8,
    sdu: usize,
    cursor_columns: BTreeMap<u32, Vec<ColumnMetadata>>,
}

#[derive(Debug)]
pub struct CancelHandle {
    write: SharedWriteHalf,
}

impl Connection {
    pub async fn connect(cx: &Cx, options: ConnectOptions) -> Result<Self> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let descriptor = EasyConnect::parse(&options.connect_string)?;
        let identity = options.identity;
        trace_connect_step("tcp connect");
        let stream = TcpStream::connect_timeout(
            (descriptor.host.clone(), descriptor.port),
            Duration::from_secs(20),
        )
        .await?;
        stream.set_nodelay(true)?;
        let (mut read, write) = stream.into_split();
        let write = Arc::new(AsyncMutex::with_name("oracle_tcp_write", write));
        trace_connect_step("tcp connected");

        let connect_descriptor = listener_connect_descriptor(&descriptor, &identity);
        trace_connect_value("CONNECT descriptor", &connect_descriptor);
        let connect_payload = build_connect_packet_payload(&connect_descriptor, options.sdu)?;
        let packet = encode_packet(
            TNS_PACKET_TYPE_CONNECT,
            0,
            None,
            &connect_payload,
            PacketLengthWidth::Legacy16,
        )?;
        trace_connect_bytes("CONNECT packet", &packet);
        trace_connect_step("send CONNECT");
        write_all_shared(cx, &write, &packet).await?;

        trace_connect_step("read ACCEPT");
        let accept = read_packet(&mut read, PacketLengthWidth::Legacy16).await?;
        match accept.packet_type {
            TNS_PACKET_TYPE_ACCEPT => {}
            TNS_PACKET_TYPE_REDIRECT => return Err(Error::RedirectUnsupported),
            TNS_PACKET_TYPE_REFUSE => {
                return Err(Error::ListenerRefused(
                    String::from_utf8_lossy(&accept.payload).to_string(),
                ))
            }
            other => {
                return Err(oracledb_protocol::ProtocolError::UnknownMessageType {
                    message_type: other,
                    position: 4,
                }
                .into())
            }
        }
        let accept_info = parse_accept_payload(&accept.payload)?;
        if !accept_info.supports_fast_auth {
            return Err(Error::FastAuthRequired);
        }
        let sdu = usize::try_from(accept_info.sdu)
            .unwrap_or(DEFAULT_SDU)
            .max(TNS_DATA_PACKET_OVERHEAD + 1);

        let client_pid = process::id();
        let auth_one = build_fast_auth_phase_one_payload(
            &options.user,
            &identity.program,
            &identity.machine,
            &identity.osuser,
            &identity.terminal,
            client_pid,
        )?;
        trace_connect_bytes("AUTH phase one payload", &auth_one);
        trace_connect_step("send AUTH phase one");
        send_data_packet_shared(cx, &write, &auth_one, sdu).await?;
        trace_connect_step("read AUTH phase one");
        let auth_one_response = read_data_response(&mut read, cx, &write).await?;
        trace_connect_bytes("AUTH phase one response", &auth_one_response);
        let auth_one = parse_auth_response(&auth_one_response)?;
        let capabilities = auth_one.capabilities.unwrap_or_default();
        let mut ttc_seq_num = 1;
        let verifier_type = auth_one
            .verifier_type
            .ok_or(Error::MissingSessionField("AUTH_VFR_DATA verifier type"))?;
        let encrypted = oracledb_protocol::crypto::generate_verifier(
            options.password.as_bytes(),
            &auth_one.session_data,
            verifier_type,
        )?;
        let auth_connect_string = auth_connect_descriptor(&descriptor);
        let auth_two = build_auth_phase_two_payload_with_context_with_seq(
            &options.user,
            &encrypted,
            &identity.driver_name,
            PYTHON_ORACLEDB_COMPAT_VERSION_NUM,
            &auth_connect_string,
            next_ttc_sequence(&mut ttc_seq_num),
            &options.app_context,
        )?;
        trace_connect_bytes("AUTH phase two payload", &auth_two);
        trace_connect_step("send AUTH phase two");
        send_data_packet_shared(cx, &write, &auth_two, sdu).await?;
        trace_connect_step("read AUTH phase two");
        let auth_two_response = read_data_response(&mut read, cx, &write).await?;
        trace_connect_bytes("AUTH phase two response", &auth_two_response);
        let auth_two = parse_auth_response(&auth_two_response)?;
        oracledb_protocol::crypto::verify_server_response(
            &encrypted.combo_key,
            &auth_two.session_data,
        )?;

        let session_id = parse_session_u32(&auth_two.session_data, "AUTH_SESSION_ID")?;
        let serial_num = parse_session_u16(&auth_two.session_data, "AUTH_SERIAL_NUM")?;
        let server_version = auth_two.session_data.get("AUTH_VERSION_STRING").cloned();

        Ok(Self {
            descriptor,
            identity,
            read,
            write,
            session_id,
            serial_num,
            server_version,
            capabilities,
            ttc_seq_num,
            sdu,
            cursor_columns: BTreeMap::new(),
        })
    }

    pub fn descriptor(&self) -> &EasyConnect {
        &self.descriptor
    }

    pub fn identity(&self) -> &ClientIdentity {
        &self.identity
    }

    pub fn session_id(&self) -> u32 {
        self.session_id
    }

    pub fn serial_num(&self) -> u16 {
        self.serial_num
    }

    pub fn server_version(&self) -> Option<&str> {
        self.server_version.as_deref()
    }

    pub fn sdu(&self) -> usize {
        self.sdu
    }

    pub fn cancel_handle(&self) -> Result<CancelHandle> {
        Ok(CancelHandle {
            write: Arc::clone(&self.write),
        })
    }

    pub async fn ping(&mut self, cx: &Cx) -> Result<()> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        send_data_packet_shared(
            cx,
            &self.write,
            &build_function_payload_with_seq(TNS_FUNC_PING, seq_num),
            self.sdu,
        )
        .await?;
        let _ = read_data_response(&mut self.read, cx, &self.write).await?;
        Ok(())
    }

    pub async fn commit(&mut self, cx: &Cx) -> Result<()> {
        self.send_function(cx, TNS_FUNC_COMMIT).await
    }

    pub async fn rollback(&mut self, cx: &Cx) -> Result<()> {
        self.send_function(cx, TNS_FUNC_ROLLBACK).await
    }

    pub async fn execute_query(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
    ) -> Result<QueryResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload =
            build_execute_payload_with_seq(sql, prefetch_rows, seq_num, statement_is_query(sql))?;
        trace_query_bytes("EXECUTE query payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("EXECUTE query response", &response);
        let result = parse_query_response(&response, self.capabilities).map_err(Error::from)?;
        self.remember_cursor_columns(&result);
        Ok(result)
    }

    pub async fn execute_query_with_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        self.execute_query_call_timeout(cx, sql, prefetch_rows, timeout_ms)
            .await
    }

    pub async fn execute_query_with_binds(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        binds: &[BindValue],
    ) -> Result<QueryResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let has_ref_cursor_output = binds.iter().any(|value| {
            matches!(
                value,
                BindValue::Output {
                    ora_type_num: oracledb_protocol::thin::ORA_TYPE_NUM_CURSOR,
                    ..
                }
            )
        });
        if has_ref_cursor_output {
            // python-oracledb reserves this sequence slot for a close-cursor piggyback.
            let _ = next_ttc_sequence(&mut self.ttc_seq_num);
        }
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_execute_payload_with_binds_with_seq(
            sql,
            prefetch_rows,
            seq_num,
            statement_is_query(sql),
            binds,
        )?;
        trace_query_bytes("EXECUTE query payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response =
            read_data_response_flushing_out_binds(&mut self.read, cx, &self.write, self.sdu)
                .await?;
        trace_query_bytes("EXECUTE query response", &response);
        let result = parse_query_response_with_binds(&response, self.capabilities, binds)
            .map_err(Error::from)?;
        self.remember_cursor_columns(&result);
        Ok(result)
    }

    pub async fn execute_query_with_binds_and_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        binds: &[BindValue],
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        self.execute_query_with_binds_call_timeout(cx, sql, prefetch_rows, binds, timeout_ms)
            .await
    }

    pub async fn execute_query_with_bind_rows(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
    ) -> Result<QueryResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_execute_payload_with_bind_rows_with_seq(
            sql,
            prefetch_rows,
            seq_num,
            statement_is_query(sql),
            bind_rows,
        )?;
        trace_query_bytes("EXECUTE query payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response =
            read_data_response_flushing_out_binds(&mut self.read, cx, &self.write, self.sdu)
                .await?;
        trace_query_bytes("EXECUTE query response", &response);
        let result = parse_query_response_with_binds(
            &response,
            self.capabilities,
            bind_rows.first().map(Vec::as_slice).unwrap_or(&[]),
        )
        .map_err(Error::from)?;
        self.remember_cursor_columns(&result);
        Ok(result)
    }

    pub async fn execute_query_with_bind_rows_and_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        self.execute_query_with_bind_rows_call_timeout(
            cx,
            sql,
            prefetch_rows,
            bind_rows,
            timeout_ms,
        )
        .await
    }

    pub async fn fetch_rows(
        &mut self,
        cx: &Cx,
        cursor_id: u32,
        arraysize: u32,
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        self.fetch_rows_with_columns(cx, cursor_id, arraysize, &[], previous_row)
            .await
    }

    pub async fn fetch_rows_with_columns(
        &mut self,
        cx: &Cx,
        cursor_id: u32,
        arraysize: u32,
        known_columns: &[ColumnMetadata],
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_fetch_payload_with_seq(cursor_id, arraysize, seq_num);
        trace_query_bytes("FETCH payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("FETCH response", &response);
        let columns = self
            .cursor_columns
            .get(&cursor_id)
            .cloned()
            .unwrap_or_else(|| known_columns.to_vec());
        let result =
            parse_fetch_response_with_context(&response, self.capabilities, &columns, previous_row)
                .map_err(Error::from)?;
        self.remember_cursor_columns(&result);
        Ok(result)
    }

    pub async fn define_and_fetch_rows_with_columns(
        &mut self,
        cx: &Cx,
        cursor_id: u32,
        arraysize: u32,
        define_columns: &[ColumnMetadata],
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload =
            build_define_fetch_payload_with_seq(cursor_id, arraysize, seq_num, define_columns)?;
        trace_query_bytes("DEFINE FETCH payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("DEFINE FETCH response", &response);
        let result = parse_fetch_response_with_context(
            &response,
            self.capabilities,
            define_columns,
            previous_row,
        )
        .map_err(Error::from)?;
        self.cursor_columns
            .insert(cursor_id, define_columns.to_vec());
        self.remember_cursor_columns(&result);
        Ok(result)
    }

    pub async fn read_lob(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        offset: u64,
        amount: u64,
    ) -> Result<LobReadResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_read_payload_with_seq(
            locator,
            offset,
            amount,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB READ payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("LOB READ response", &response);
        parse_lob_read_response(&response, self.capabilities, locator).map_err(Error::from)
    }

    pub async fn read_lob_with_timeout(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        offset: u64,
        amount: u64,
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        self.read_lob_call_timeout(cx, locator, offset, amount, timeout_ms)
            .await
    }

    pub async fn create_temp_lob(
        &mut self,
        cx: &Cx,
        ora_type_num: u8,
        csfrm: u8,
    ) -> Result<LobReadResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_create_temp_payload_with_seq(
            ora_type_num,
            csfrm,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB CREATE TEMP payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("LOB CREATE TEMP response", &response);
        parse_lob_create_temp_response(&response, self.capabilities).map_err(Error::from)
    }

    pub async fn write_lob(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        offset: u64,
        data: &[u8],
    ) -> Result<LobReadResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_write_payload_with_seq(
            locator,
            offset,
            data,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB WRITE payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("LOB WRITE response", &response);
        parse_lob_write_response(&response, self.capabilities, locator).map_err(Error::from)
    }

    pub async fn write_lob_with_timeout(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        offset: u64,
        data: &[u8],
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        self.write_lob_call_timeout(cx, locator, offset, data, timeout_ms)
            .await
    }

    pub async fn trim_lob(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        new_size: u64,
    ) -> Result<LobReadResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_trim_payload_with_seq(
            locator,
            new_size,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB TRIM payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("LOB TRIM response", &response);
        parse_lob_trim_response(&response, self.capabilities, locator).map_err(Error::from)
    }

    pub async fn trim_lob_with_timeout(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        new_size: u64,
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        self.trim_lob_call_timeout(cx, locator, new_size, timeout_ms)
            .await
    }

    pub async fn free_temp_lobs(&mut self, cx: &Cx, locators: &[Vec<u8>]) -> Result<()> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        if locators.is_empty() {
            return Ok(());
        }
        let returned_parameter_len = locators.iter().map(Vec::len).sum();
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_free_temp_payload_with_seq(
            locators,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB FREE TEMP payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("LOB FREE TEMP response", &response);
        parse_lob_free_temp_response(&response, self.capabilities, returned_parameter_len)
            .map_err(Error::from)
    }

    pub async fn free_temp_lobs_with_timeout(
        &mut self,
        cx: &Cx,
        locators: &[Vec<u8>],
        timeout_ms: Option<u32>,
    ) -> Result<()> {
        self.free_temp_lobs_call_timeout(cx, locators, timeout_ms)
            .await
    }

    async fn execute_query_call_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self.execute_query(cx, sql, prefetch_rows).await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.execute_query(cx, sql, prefetch_rows),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    async fn execute_query_with_binds_call_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        binds: &[BindValue],
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self
                .execute_query_with_binds(cx, sql, prefetch_rows, binds)
                .await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.execute_query_with_binds(cx, sql, prefetch_rows, binds),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    async fn execute_query_with_bind_rows_call_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self
                .execute_query_with_bind_rows(cx, sql, prefetch_rows, bind_rows)
                .await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.execute_query_with_bind_rows(cx, sql, prefetch_rows, bind_rows),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    async fn read_lob_call_timeout(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        offset: u64,
        amount: u64,
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self.read_lob(cx, locator, offset, amount).await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.read_lob(cx, locator, offset, amount),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    async fn write_lob_call_timeout(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        offset: u64,
        data: &[u8],
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self.write_lob(cx, locator, offset, data).await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.write_lob(cx, locator, offset, data),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    async fn trim_lob_call_timeout(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        new_size: u64,
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self.trim_lob(cx, locator, new_size).await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.trim_lob(cx, locator, new_size),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    async fn free_temp_lobs_call_timeout(
        &mut self,
        cx: &Cx,
        locators: &[Vec<u8>],
        timeout_ms: Option<u32>,
    ) -> Result<()> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self.free_temp_lobs(cx, locators).await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.free_temp_lobs(cx, locators),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    async fn drain_cancel_response(&mut self, cx: &Cx) -> Result<()> {
        match time::timeout(
            time::wall_now(),
            Duration::from_secs(5),
            read_data_response(&mut self.read, cx, &self.write),
        )
        .await
        {
            Ok(response) => {
                let response = response?;
                trace_query_bytes("CANCEL drain response", &response);
                Ok(())
            }
            Err(_) => Ok(()),
        }
    }

    fn remember_cursor_columns(&mut self, result: &QueryResult) {
        if result.cursor_id != 0 && !result.columns.is_empty() {
            self.cursor_columns
                .insert(result.cursor_id, result.columns.clone());
        }
    }

    pub async fn close(mut self, cx: &Cx) -> Result<()> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        match time::timeout(time::wall_now(), Duration::from_secs(5), self.rollback(cx)).await {
            Ok(result) => result?,
            Err(_) => {
                let eof = encode_packet(
                    TNS_PACKET_TYPE_DATA,
                    0,
                    Some(oracledb_protocol::thin::TNS_DATA_FLAGS_EOF),
                    &[],
                    PacketLengthWidth::Large32,
                )?;
                let _ = write_all_shared(cx, &self.write, &eof).await;
                let _ = shutdown_write_shared(cx, &self.write).await;
                return Ok(());
            }
        }
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        send_data_packet_shared(
            cx,
            &self.write,
            &build_function_payload_with_seq(TNS_FUNC_LOGOFF, seq_num),
            self.sdu,
        )
        .await?;
        if let Ok(response) = time::timeout(
            time::wall_now(),
            Duration::from_secs(5),
            read_data_response(&mut self.read, cx, &self.write),
        )
        .await
        {
            let _ = response?;
        }
        let eof = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(oracledb_protocol::thin::TNS_DATA_FLAGS_EOF),
            &[],
            PacketLengthWidth::Large32,
        )?;
        write_all_shared(cx, &self.write, &eof).await?;
        let _ = shutdown_write_shared(cx, &self.write).await;
        Ok(())
    }

    async fn send_function(&mut self, cx: &Cx, function_code: u8) -> Result<()> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        send_data_packet_shared(
            cx,
            &self.write,
            &build_function_payload_with_seq(function_code, seq_num),
            self.sdu,
        )
        .await?;
        let _ = read_data_response(&mut self.read, cx, &self.write).await?;
        Ok(())
    }
}

impl CancelHandle {
    pub fn cancel(&mut self) -> Result<()> {
        let runtime = build_io_runtime()?;
        let write = Arc::clone(&self.write);
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            send_marker_shared(&cx, &write, TNS_MARKER_TYPE_BREAK).await
        })
    }
}

pub struct BlockingConnection;

impl BlockingConnection {
    pub fn connect(options: ConnectOptions) -> Result<Connection> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            Connection::connect(&cx, options).await
        })
    }

    pub fn ping(connection: &mut Connection) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.ping(&cx).await
        })
    }

    pub fn commit(connection: &mut Connection) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.commit(&cx).await
        })
    }

    pub fn rollback(connection: &mut Connection) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.rollback(&cx).await
        })
    }

    pub fn execute_query(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.execute_query(&cx, sql, prefetch_rows).await
        })
    }

    pub fn execute_query_with_timeout(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_call_timeout(&cx, sql, prefetch_rows, timeout_ms)
                .await
        })
    }

    pub fn execute_query_with_binds(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
        binds: &[BindValue],
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_binds(&cx, sql, prefetch_rows, binds)
                .await
        })
    }

    pub fn execute_query_with_binds_and_timeout(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
        binds: &[BindValue],
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_binds_call_timeout(&cx, sql, prefetch_rows, binds, timeout_ms)
                .await
        })
    }

    pub fn execute_query_with_bind_rows(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_bind_rows(&cx, sql, prefetch_rows, bind_rows)
                .await
        })
    }

    pub fn execute_query_with_bind_rows_and_timeout(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_bind_rows_call_timeout(
                    &cx,
                    sql,
                    prefetch_rows,
                    bind_rows,
                    timeout_ms,
                )
                .await
        })
    }

    pub fn fetch_rows(
        connection: &mut Connection,
        cursor_id: u32,
        arraysize: u32,
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .fetch_rows(&cx, cursor_id, arraysize, previous_row)
                .await
        })
    }

    pub fn fetch_rows_with_columns(
        connection: &mut Connection,
        cursor_id: u32,
        arraysize: u32,
        known_columns: &[ColumnMetadata],
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .fetch_rows_with_columns(&cx, cursor_id, arraysize, known_columns, previous_row)
                .await
        })
    }

    pub fn define_and_fetch_rows_with_columns(
        connection: &mut Connection,
        cursor_id: u32,
        arraysize: u32,
        define_columns: &[ColumnMetadata],
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .define_and_fetch_rows_with_columns(
                    &cx,
                    cursor_id,
                    arraysize,
                    define_columns,
                    previous_row,
                )
                .await
        })
    }

    pub fn read_lob(
        connection: &mut Connection,
        locator: &[u8],
        offset: u64,
        amount: u64,
    ) -> Result<LobReadResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.read_lob(&cx, locator, offset, amount).await
        })
    }

    pub fn read_lob_with_timeout(
        connection: &mut Connection,
        locator: &[u8],
        offset: u64,
        amount: u64,
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .read_lob_call_timeout(&cx, locator, offset, amount, timeout_ms)
                .await
        })
    }

    pub fn create_temp_lob(
        connection: &mut Connection,
        ora_type_num: u8,
        csfrm: u8,
    ) -> Result<LobReadResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.create_temp_lob(&cx, ora_type_num, csfrm).await
        })
    }

    pub fn write_lob(
        connection: &mut Connection,
        locator: &[u8],
        offset: u64,
        data: &[u8],
    ) -> Result<LobReadResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.write_lob(&cx, locator, offset, data).await
        })
    }

    pub fn write_lob_with_timeout(
        connection: &mut Connection,
        locator: &[u8],
        offset: u64,
        data: &[u8],
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .write_lob_call_timeout(&cx, locator, offset, data, timeout_ms)
                .await
        })
    }

    pub fn trim_lob_with_timeout(
        connection: &mut Connection,
        locator: &[u8],
        new_size: u64,
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .trim_lob_call_timeout(&cx, locator, new_size, timeout_ms)
                .await
        })
    }

    pub fn free_temp_lobs_with_timeout(
        connection: &mut Connection,
        locators: &[Vec<u8>],
        timeout_ms: Option<u32>,
    ) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .free_temp_lobs_call_timeout(&cx, locators, timeout_ms)
                .await
        })
    }

    pub fn drain_cancel_response(connection: &mut Connection) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.drain_cancel_response(&cx).await
        })
    }

    pub fn close(connection: Connection) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.close(&cx).await
        })
    }
}

fn build_io_runtime() -> Result<Runtime> {
    let reactor = reactor::create_reactor()?;
    RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .map_err(|err| Error::Runtime(err.to_string()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IncomingPacket {
    packet_type: u8,
    payload: Vec<u8>,
}

async fn lock_write<'a>(
    cx: &Cx,
    write: &'a SharedWriteHalf,
) -> Result<asupersync::sync::MutexGuard<'a, OwnedWriteHalf>> {
    write
        .lock(cx)
        .await
        .map_err(|err| Error::Runtime(err.to_string()))
}

async fn write_all_shared(cx: &Cx, write: &SharedWriteHalf, packet: &[u8]) -> Result<()> {
    let mut guard = lock_write(cx, write).await?;
    guard.write_all(packet).await?;
    guard.flush().await?;
    Ok(())
}

async fn shutdown_write_shared(cx: &Cx, write: &SharedWriteHalf) -> Result<()> {
    let mut guard = lock_write(cx, write).await?;
    guard.shutdown().await?;
    Ok(())
}

async fn send_data_packet_shared(
    cx: &Cx,
    write: &SharedWriteHalf,
    payload: &[u8],
    sdu: usize,
) -> Result<()> {
    let mut guard = lock_write(cx, write).await?;
    send_data_packet(&mut *guard, payload, sdu).await
}

async fn send_marker_shared(cx: &Cx, write: &SharedWriteHalf, marker_type: u8) -> Result<()> {
    let mut guard = lock_write(cx, write).await?;
    send_marker(&mut *guard, marker_type).await
}

async fn send_data_packet<W>(stream: &mut W, payload: &[u8], sdu: usize) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let max_payload = sdu.saturating_sub(TNS_DATA_PACKET_OVERHEAD).max(1);
    for chunk in payload.chunks(max_payload) {
        let packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            chunk,
            PacketLengthWidth::Large32,
        )?;
        stream.write_all(&packet).await?;
    }
    stream.flush().await?;
    Ok(())
}

struct DataResponse {
    payload: Vec<u8>,
    flush_out_binds: bool,
}

async fn read_data_response(
    read: &mut OwnedReadHalf,
    cx: &Cx,
    write: &SharedWriteHalf,
) -> Result<Vec<u8>> {
    Ok(read_data_response_boundary(read, cx, write).await?.payload)
}

async fn read_data_response_flushing_out_binds(
    read: &mut OwnedReadHalf,
    cx: &Cx,
    write: &SharedWriteHalf,
    sdu: usize,
) -> Result<Vec<u8>> {
    let mut response = read_data_response_boundary(read, cx, write).await?;
    let mut payload = response.payload;
    while response.flush_out_binds {
        if matches!(payload.last(), Some(&TNS_MSG_TYPE_FLUSH_OUT_BINDS)) {
            payload.pop();
        }
        send_data_packet_shared(cx, write, &[TNS_MSG_TYPE_FLUSH_OUT_BINDS], sdu).await?;
        response = read_data_response_boundary(read, cx, write).await?;
        payload.extend_from_slice(&response.payload);
    }
    Ok(payload)
}

async fn read_data_response_boundary(
    read: &mut OwnedReadHalf,
    cx: &Cx,
    write: &SharedWriteHalf,
) -> Result<DataResponse> {
    let mut response = Vec::new();
    let mut flush_out_binds = false;
    let mut pending_packet = None;
    loop {
        let packet = match pending_packet.take() {
            Some(packet) => packet,
            None => read_packet(read, PacketLengthWidth::Large32).await?,
        };
        if packet.packet_type == TNS_PACKET_TYPE_MARKER {
            pending_packet = reset_after_marker(read, cx, write, &packet).await?;
            continue;
        }
        if packet.packet_type != TNS_PACKET_TYPE_DATA {
            return Err(oracledb_protocol::ProtocolError::UnknownMessageType {
                message_type: packet.packet_type,
                position: 4,
            }
            .into());
        }
        let (data_flags, payload) = packet.payload.split_at_checked(2).ok_or(
            oracledb_protocol::ProtocolError::TtcDecode("missing data packet flags"),
        )?;
        let flags = u16::from_be_bytes(
            data_flags
                .try_into()
                .map_err(|_| oracledb_protocol::ProtocolError::TtcDecode("invalid flags"))?,
        );
        response.extend_from_slice(payload);
        if matches!(payload.last(), Some(&TNS_MSG_TYPE_FLUSH_OUT_BINDS)) {
            flush_out_binds = true;
            break;
        }
        if flags & oracledb_protocol::thin::TNS_DATA_FLAGS_END_OF_RESPONSE != 0 {
            break;
        }
        if matches!(payload.last(), Some(&TNS_MSG_TYPE_END_OF_RESPONSE)) {
            break;
        }
    }
    Ok(DataResponse {
        payload: response,
        flush_out_binds,
    })
}

const TNS_PACKET_TYPE_MARKER: u8 = 12;
const TNS_MARKER_TYPE_BREAK: u8 = 1;
const TNS_MARKER_TYPE_RESET: u8 = 2;

async fn reset_after_marker(
    read: &mut OwnedReadHalf,
    cx: &Cx,
    write: &SharedWriteHalf,
    initial_marker: &IncomingPacket,
) -> Result<Option<IncomingPacket>> {
    trace_connect_bytes("MARKER packet", &initial_marker.payload);
    send_marker_shared(cx, write, TNS_MARKER_TYPE_RESET).await?;
    loop {
        let packet = read_packet(read, PacketLengthWidth::Large32).await?;
        if packet.packet_type != TNS_PACKET_TYPE_MARKER {
            return Ok(Some(packet));
        }
        trace_connect_bytes("MARKER reset response", &packet.payload);
        if matches!(packet.payload.get(2), Some(&TNS_MARKER_TYPE_RESET)) {
            return Ok(None);
        }
    }
}

async fn send_marker<W>(stream: &mut W, marker_type: u8) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let packet = encode_packet(
        TNS_PACKET_TYPE_MARKER,
        0,
        None,
        &[1, 0, marker_type],
        PacketLengthWidth::Large32,
    )?;
    trace_connect_bytes("send MARKER", &packet);
    stream.write_all(&packet).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_packet<R>(stream: &mut R, width: PacketLengthWidth) -> Result<IncomingPacket>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await?;
    let [len0, len1, len2, len3, packet_type, _, _, _] = header;
    let declared = match width {
        PacketLengthWidth::Legacy16 => usize::from(u16::from_be_bytes([len0, len1])),
        PacketLengthWidth::Large32 => {
            usize::try_from(u32::from_be_bytes([len0, len1, len2, len3])).unwrap_or(usize::MAX)
        }
    };
    if declared < header.len() {
        return Err(oracledb_protocol::ProtocolError::InvalidPacketLength {
            length: declared,
            minimum: header.len(),
        }
        .into());
    }
    let mut payload = vec![0u8; declared - header.len()];
    stream.read_exact(&mut payload).await?;
    Ok(IncomingPacket {
        packet_type,
        payload,
    })
}

fn listener_connect_descriptor(descriptor: &EasyConnect, identity: &ClientIdentity) -> String {
    format!(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST={})(PORT={}))(CONNECT_DATA=(SERVICE_NAME={})(CID=(PROGRAM={})(HOST={})(USER={}))))",
        descriptor.host,
        descriptor.port,
        descriptor.service_name,
        identity.program,
        identity.machine,
        identity.osuser,
    )
}

fn auth_connect_descriptor(descriptor: &EasyConnect) -> String {
    format!(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST={})(PORT={}))(CONNECT_DATA=(SERVICE_NAME={})))",
        descriptor.host, descriptor.port, descriptor.service_name
    )
}

fn parse_session_u32(
    data: &std::collections::BTreeMap<String, String>,
    key: &'static str,
) -> Result<u32> {
    data.get(key)
        .ok_or(Error::MissingSessionField(key))?
        .parse::<u32>()
        .map_err(|_| Error::MissingSessionField(key))
}

fn parse_session_u16(
    data: &std::collections::BTreeMap<String, String>,
    key: &'static str,
) -> Result<u16> {
    data.get(key)
        .ok_or(Error::MissingSessionField(key))?
        .parse::<u16>()
        .map_err(|_| Error::MissingSessionField(key))
}

fn next_ttc_sequence(seq_num: &mut u8) -> u8 {
    *seq_num = seq_num.wrapping_add(1);
    if *seq_num == 0 {
        *seq_num = 1;
    }
    *seq_num
}

fn statement_is_query(sql: &str) -> bool {
    sql.trim_start()
        .split(|ch: char| !ch.is_ascii_alphabetic())
        .next()
        .is_some_and(|keyword| keyword.eq_ignore_ascii_case("select"))
}

fn trace_connect_step(step: &'static str) {
    if std::env::var_os("ORACLEDB_TRACE_CONNECT").is_some() {
        eprintln!("oracledb::connect: {step}");
    }
}

fn trace_connect_value(label: &'static str, value: &str) {
    if std::env::var_os("ORACLEDB_TRACE_CONNECT").is_some() {
        eprintln!("oracledb::connect: {label}: {value}");
    }
}

fn trace_connect_bytes(label: &'static str, bytes: &[u8]) {
    if std::env::var_os("ORACLEDB_TRACE_CONNECT").is_some() {
        let mut hex = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
        }
        eprintln!("oracledb::connect: {label} len={} hex={hex}", bytes.len());
    }
}

fn trace_query_bytes(label: &'static str, bytes: &[u8]) {
    if std::env::var_os("ORACLEDB_TRACE_QUERY").is_some() {
        let mut hex = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
        }
        eprintln!("oracledb::query: {label} len={} hex={hex}", bytes.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    fn identity() -> ClientIdentity {
        ClientIdentity::new("program", "machine", "osuser", "terminal", "driver")
            .expect("test identity should be valid")
    }

    #[test]
    fn descriptor_builder_uses_identity_in_listener_cid() {
        let options = ConnectOptions::new("localhost/FREEPDB1", "user", "password", identity());
        let descriptor =
            EasyConnect::parse(&options.connect_string).expect("test connect string should parse");
        let built = listener_connect_descriptor(&descriptor, &options.identity);
        assert!(built.contains("(PROGRAM=program)"));
        assert!(built.contains("(HOST=machine)"));
        assert!(built.contains("(USER=osuser)"));
    }

    #[test]
    fn cancel_handle_sends_tns_break_marker() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            let mut packet = [0u8; 11];
            socket.read_exact(&mut packet).expect("read marker packet");
            packet
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let mut handle = runtime.block_on(async {
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (_read, write) = stream.into_split();
            CancelHandle {
                write: Arc::new(AsyncMutex::with_name("oracle_tcp_write_test", write)),
            }
        });

        handle.cancel().expect("cancel marker write");

        let packet = server.join().expect("server thread joins");
        assert_eq!(
            packet,
            [
                0,
                0,
                0,
                11,
                TNS_PACKET_TYPE_MARKER,
                0,
                0,
                0,
                1,
                0,
                TNS_MARKER_TYPE_BREAK
            ]
        );
    }
}
