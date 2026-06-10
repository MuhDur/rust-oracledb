#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::process;
use std::time::Duration;

use asupersync::{runtime::RuntimeBuilder, Cx};
use oracledb_protocol::thin::{
    build_auth_phase_two_payload_with_context_with_seq, build_connect_packet_payload,
    build_execute_payload_with_bind_rows_with_seq, build_execute_payload_with_binds_with_seq,
    build_execute_payload_with_seq, build_fast_auth_phase_one_payload,
    build_fetch_payload_with_seq, build_function_payload_with_seq, parse_accept_payload,
    parse_auth_response, parse_query_response, parse_query_response_with_context, BindValue,
    ClientCapabilities, ColumnMetadata, QueryResult, TNS_FUNC_COMMIT, TNS_FUNC_LOGOFF,
    TNS_FUNC_PING, TNS_FUNC_ROLLBACK, TNS_PACKET_TYPE_ACCEPT, TNS_PACKET_TYPE_CONNECT,
    TNS_PACKET_TYPE_DATA, TNS_PACKET_TYPE_REDIRECT, TNS_PACKET_TYPE_REFUSE,
};
use oracledb_protocol::wire::{encode_packet, PacketLengthWidth};
use oracledb_protocol::{net::EasyConnect, ClientIdentity};

const PYTHON_ORACLEDB_COMPAT_VERSION_NUM: u32 = 0x0400_1000;
const DEFAULT_SDU: usize = 8192;
const TNS_DATA_PACKET_OVERHEAD: usize = 10;

pub use oracledb_protocol as protocol;

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
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub struct ConnectOptions {
    pub connect_string: String,
    pub user: String,
    pub password: String,
    pub identity: ClientIdentity,
    pub app_context: Vec<(String, String, String)>,
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
        }
    }

    pub fn with_app_context(mut self, app_context: Vec<(String, String, String)>) -> Self {
        self.app_context = app_context;
        self
    }
}

#[derive(Debug)]
pub struct Connection {
    descriptor: EasyConnect,
    identity: ClientIdentity,
    stream: TcpStream,
    session_id: u32,
    serial_num: u16,
    server_version: Option<String>,
    capabilities: ClientCapabilities,
    ttc_seq_num: u8,
    sdu: usize,
    cursor_columns: BTreeMap<u32, Vec<ColumnMetadata>>,
}

impl Connection {
    pub async fn connect(cx: &Cx, options: ConnectOptions) -> Result<Self> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let descriptor = EasyConnect::parse(&options.connect_string)?;
        let identity = options.identity;
        trace_connect_step("tcp connect");
        let mut stream = TcpStream::connect((descriptor.host.as_str(), descriptor.port))?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(20)))?;
        stream.set_write_timeout(Some(Duration::from_secs(20)))?;
        trace_connect_step("tcp connected");

        let connect_descriptor = listener_connect_descriptor(&descriptor, &identity);
        trace_connect_value("CONNECT descriptor", &connect_descriptor);
        let connect_payload = build_connect_packet_payload(&connect_descriptor, 8192)?;
        let packet = encode_packet(
            TNS_PACKET_TYPE_CONNECT,
            0,
            None,
            &connect_payload,
            PacketLengthWidth::Legacy16,
        )?;
        trace_connect_bytes("CONNECT packet", &packet);
        trace_connect_step("send CONNECT");
        stream.write_all(&packet)?;
        stream.flush()?;

        trace_connect_step("read ACCEPT");
        let accept = read_packet(&mut stream, PacketLengthWidth::Legacy16)?;
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
        send_data_packet(&mut stream, &auth_one, sdu)?;
        trace_connect_step("read AUTH phase one");
        let auth_one_response = read_data_response(&mut stream)?;
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
        send_data_packet(&mut stream, &auth_two, sdu)?;
        trace_connect_step("read AUTH phase two");
        let auth_two_response = read_data_response(&mut stream)?;
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
            stream,
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

    pub async fn ping(&mut self, cx: &Cx) -> Result<()> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        send_data_packet(
            &mut self.stream,
            &build_function_payload_with_seq(TNS_FUNC_PING, seq_num),
            self.sdu,
        )?;
        let _ = read_data_response(&mut self.stream)?;
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
        send_data_packet(&mut self.stream, &payload, self.sdu)?;
        let response = read_data_response(&mut self.stream)?;
        trace_query_bytes("EXECUTE query response", &response);
        let result = parse_query_response(&response, self.capabilities).map_err(Error::from)?;
        self.remember_cursor_columns(&result);
        Ok(result)
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
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_execute_payload_with_binds_with_seq(
            sql,
            prefetch_rows,
            seq_num,
            statement_is_query(sql),
            binds,
        )?;
        trace_query_bytes("EXECUTE query payload", &payload);
        send_data_packet(&mut self.stream, &payload, self.sdu)?;
        let response = read_data_response(&mut self.stream)?;
        trace_query_bytes("EXECUTE query response", &response);
        let result = parse_query_response(&response, self.capabilities).map_err(Error::from)?;
        self.remember_cursor_columns(&result);
        Ok(result)
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
        send_data_packet(&mut self.stream, &payload, self.sdu)?;
        let response = read_data_response(&mut self.stream)?;
        trace_query_bytes("EXECUTE query response", &response);
        let result = parse_query_response(&response, self.capabilities).map_err(Error::from)?;
        self.remember_cursor_columns(&result);
        Ok(result)
    }

    pub async fn fetch_rows(
        &mut self,
        cx: &Cx,
        cursor_id: u32,
        arraysize: u32,
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_fetch_payload_with_seq(cursor_id, arraysize, seq_num);
        trace_query_bytes("FETCH payload", &payload);
        send_data_packet(&mut self.stream, &payload, self.sdu)?;
        let response = read_data_response(&mut self.stream)?;
        trace_query_bytes("FETCH response", &response);
        let columns = self
            .cursor_columns
            .get(&cursor_id)
            .cloned()
            .unwrap_or_default();
        let result =
            parse_query_response_with_context(&response, self.capabilities, &columns, previous_row)
                .map_err(Error::from)?;
        self.remember_cursor_columns(&result);
        Ok(result)
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
        self.rollback(cx).await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        send_data_packet(
            &mut self.stream,
            &build_function_payload_with_seq(TNS_FUNC_LOGOFF, seq_num),
            self.sdu,
        )?;
        let _ = read_data_response(&mut self.stream)?;
        let eof = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(oracledb_protocol::thin::TNS_DATA_FLAGS_EOF),
            &[],
            PacketLengthWidth::Large32,
        )?;
        self.stream.write_all(&eof)?;
        self.stream.flush()?;
        let _ = self.stream.shutdown(Shutdown::Both);
        Ok(())
    }

    async fn send_function(&mut self, cx: &Cx, function_code: u8) -> Result<()> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        send_data_packet(
            &mut self.stream,
            &build_function_payload_with_seq(function_code, seq_num),
            self.sdu,
        )?;
        let _ = read_data_response(&mut self.stream)?;
        Ok(())
    }
}

pub struct BlockingConnection;

impl BlockingConnection {
    pub fn connect(options: ConnectOptions) -> Result<Connection> {
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            Connection::connect(&cx, options).await
        })
    }

    pub fn ping(connection: &mut Connection) -> Result<()> {
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.ping(&cx).await
        })
    }

    pub fn commit(connection: &mut Connection) -> Result<()> {
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.commit(&cx).await
        })
    }

    pub fn rollback(connection: &mut Connection) -> Result<()> {
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .map_err(|err| Error::Runtime(err.to_string()))?;
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
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.execute_query(&cx, sql, prefetch_rows).await
        })
    }

    pub fn execute_query_with_binds(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
        binds: &[BindValue],
    ) -> Result<QueryResult> {
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_binds(&cx, sql, prefetch_rows, binds)
                .await
        })
    }

    pub fn execute_query_with_bind_rows(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
    ) -> Result<QueryResult> {
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_bind_rows(&cx, sql, prefetch_rows, bind_rows)
                .await
        })
    }

    pub fn fetch_rows(
        connection: &mut Connection,
        cursor_id: u32,
        arraysize: u32,
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .fetch_rows(&cx, cursor_id, arraysize, previous_row)
                .await
        })
    }

    pub fn close(connection: Connection) -> Result<()> {
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.close(&cx).await
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IncomingPacket {
    packet_type: u8,
    payload: Vec<u8>,
}

fn send_data_packet(stream: &mut TcpStream, payload: &[u8], sdu: usize) -> Result<()> {
    let max_payload = sdu.saturating_sub(TNS_DATA_PACKET_OVERHEAD).max(1);
    for chunk in payload.chunks(max_payload) {
        let packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            chunk,
            PacketLengthWidth::Large32,
        )?;
        stream.write_all(&packet)?;
    }
    stream.flush()?;
    Ok(())
}

fn read_data_response(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut response = Vec::new();
    loop {
        let packet = read_packet(stream, PacketLengthWidth::Large32)?;
        if packet.packet_type == TNS_PACKET_TYPE_MARKER {
            reset_after_marker(stream, &packet)?;
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
        if flags & oracledb_protocol::thin::TNS_DATA_FLAGS_END_OF_RESPONSE != 0 {
            break;
        }
        if payload.last() == Some(&oracledb_protocol::thin::TNS_MSG_TYPE_END_OF_RESPONSE) {
            break;
        }
    }
    Ok(response)
}

const TNS_PACKET_TYPE_MARKER: u8 = 12;
const TNS_MARKER_TYPE_RESET: u8 = 2;

fn reset_after_marker(stream: &mut TcpStream, initial_marker: &IncomingPacket) -> Result<()> {
    trace_connect_bytes("MARKER packet", &initial_marker.payload);
    send_marker(stream, TNS_MARKER_TYPE_RESET)?;
    loop {
        let packet = read_packet(stream, PacketLengthWidth::Large32)?;
        if packet.packet_type != TNS_PACKET_TYPE_MARKER {
            return Err(oracledb_protocol::ProtocolError::UnknownMessageType {
                message_type: packet.packet_type,
                position: 4,
            }
            .into());
        }
        trace_connect_bytes("MARKER reset response", &packet.payload);
        if matches!(packet.payload.get(2), Some(&TNS_MARKER_TYPE_RESET)) {
            return Ok(());
        }
    }
}

fn send_marker(stream: &mut TcpStream, marker_type: u8) -> Result<()> {
    let packet = encode_packet(
        TNS_PACKET_TYPE_MARKER,
        0,
        None,
        &[1, 0, marker_type],
        PacketLengthWidth::Large32,
    )?;
    trace_connect_bytes("send MARKER", &packet);
    stream.write_all(&packet)?;
    stream.flush()?;
    Ok(())
}

fn read_packet(stream: &mut TcpStream, width: PacketLengthWidth) -> Result<IncomingPacket> {
    let mut header = [0u8; 8];
    stream.read_exact(&mut header)?;
    let declared = match width {
        PacketLengthWidth::Legacy16 => usize::from(u16::from_be_bytes([header[0], header[1]])),
        PacketLengthWidth::Large32 => {
            u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize
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
    stream.read_exact(&mut payload)?;
    Ok(IncomingPacket {
        packet_type: header[4],
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
}
