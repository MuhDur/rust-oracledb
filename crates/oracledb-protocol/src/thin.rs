#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use crate::wire::{TtcReader, TtcWriter};
use crate::{ProtocolError, Result, TNS_VERSION_DESIRED, TNS_VERSION_MIN};
use hex::FromHex;

pub const TNS_PACKET_TYPE_CONNECT: u8 = 1;
pub const TNS_PACKET_TYPE_ACCEPT: u8 = 2;
pub const TNS_PACKET_TYPE_REFUSE: u8 = 4;
pub const TNS_PACKET_TYPE_REDIRECT: u8 = 5;
pub const TNS_PACKET_TYPE_DATA: u8 = 6;

pub const TNS_DATA_FLAGS_EOF: u16 = 0x0040;
pub const TNS_DATA_FLAGS_END_OF_RESPONSE: u16 = 0x2000;

pub const TNS_MSG_TYPE_PROTOCOL: u8 = 1;
pub const TNS_MSG_TYPE_DATA_TYPES: u8 = 2;
pub const TNS_MSG_TYPE_FUNCTION: u8 = 3;
pub const TNS_MSG_TYPE_ERROR: u8 = 4;
pub const TNS_MSG_TYPE_ROW_HEADER: u8 = 6;
pub const TNS_MSG_TYPE_ROW_DATA: u8 = 7;
pub const TNS_MSG_TYPE_PARAMETER: u8 = 8;
pub const TNS_MSG_TYPE_STATUS: u8 = 9;
pub const TNS_MSG_TYPE_BIT_VECTOR: u8 = 21;
pub const TNS_MSG_TYPE_DESCRIBE_INFO: u8 = 16;
pub const TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK: u8 = 23;
pub const TNS_MSG_TYPE_END_OF_RESPONSE: u8 = 29;

pub const TNS_FUNC_AUTH_PHASE_ONE: u8 = 118;
pub const TNS_FUNC_AUTH_PHASE_TWO: u8 = 115;
pub const TNS_FUNC_COMMIT: u8 = 14;
pub const TNS_FUNC_EXECUTE: u8 = 94;
pub const TNS_FUNC_FETCH: u8 = 5;
pub const TNS_FUNC_LOGOFF: u8 = 9;
pub const TNS_FUNC_PING: u8 = 147;
pub const TNS_FUNC_ROLLBACK: u8 = 15;

pub const TNS_AUTH_MODE_LOGON: u32 = 0x0000_0001;
pub const TNS_AUTH_MODE_WITH_PASSWORD: u32 = 0x0000_0100;

pub const TNS_VERIFIER_TYPE_11G_1: u32 = 0xb152;
pub const TNS_VERIFIER_TYPE_11G_2: u32 = 0x1b25;
pub const TNS_VERIFIER_TYPE_12C: u32 = 0x4815;

pub const ORA_TYPE_NUM_VARCHAR: u8 = 1;
pub const ORA_TYPE_NUM_NUMBER: u8 = 2;
pub const ORA_TYPE_NUM_LONG: u8 = 8;
pub const ORA_TYPE_NUM_RAW: u8 = 23;
pub const ORA_TYPE_NUM_LONG_RAW: u8 = 24;
pub const ORA_TYPE_NUM_CHAR: u8 = 96;

pub const CS_FORM_IMPLICIT: u8 = 1;
pub const CS_FORM_NCHAR: u8 = 2;

const TNS_GSO_DONT_CARE: u16 = 0x0001;
const TNS_NSI_DISABLE_NA: u8 = 0x04;
const TNS_NSI_NA_REQUIRED: u8 = 0x10;
const TNS_NSI_SUPPORT_SECURITY_RENEG: u8 = 0x80;
const TNS_PROTOCOL_CHARACTERISTICS: u16 = 0x4f98;
const TNS_ACCEPT_FLAG_FAST_AUTH: u32 = 0x1000_0000;
const TNS_ACCEPT_FLAG_HAS_END_OF_RESPONSE: u32 = 0x0200_0000;
const TNS_ACCEPT_FLAG_CHECK_OOB: u32 = 0x0000_0001;
const TNS_SERVER_PIGGYBACK_QUERY_CACHE_INVALIDATION: u8 = 1;
const TNS_SERVER_PIGGYBACK_OS_PID_MTS: u8 = 2;
const TNS_SERVER_PIGGYBACK_TRACE_EVENT: u8 = 3;
const TNS_SERVER_PIGGYBACK_SESS_RET: u8 = 4;
const TNS_SERVER_PIGGYBACK_SYNC: u8 = 5;
const TNS_SERVER_PIGGYBACK_LTXID: u8 = 7;
const TNS_SERVER_PIGGYBACK_AC_REPLAY_CONTEXT: u8 = 8;
const TNS_SERVER_PIGGYBACK_EXT_SYNC: u8 = 9;
const TNS_SERVER_PIGGYBACK_SESS_SIGNATURE: u8 = 10;
const TNS_CCAP_FIELD_VERSION: usize = 7;
const TNS_CCAP_FIELD_VERSION_12_2: u8 = 8;
const TNS_CCAP_FIELD_VERSION_20_1: u8 = 14;
const TNS_CCAP_FIELD_VERSION_23_1: u8 = 17;
const TNS_CCAP_FIELD_VERSION_23_1_EXT_3: u8 = 20;
const TNS_CCAP_FIELD_VERSION_23_4: u8 = 24;
const TNS_RCAP_TTC: usize = 6;
const TNS_RCAP_TTC_32K: u8 = 0x04;
const TNS_EXEC_OPTION_PARSE: u32 = 0x01;
const TNS_EXEC_OPTION_BIND: u32 = 0x08;
const TNS_EXEC_OPTION_EXECUTE: u32 = 0x20;
const TNS_EXEC_OPTION_FETCH: u32 = 0x40;
const TNS_EXEC_OPTION_NOT_PLSQL: u32 = 0x8000;
const TNS_EXEC_FLAGS_IMPLICIT_RESULTSET: u32 = 0x8000;
const TNS_BIND_USE_INDICATORS: u8 = 0x01;
const TNS_CHARSET_UTF8: u16 = 873;
const TNS_MAX_LONG_LENGTH: u32 = 0x7fff_ffff;
const TNS_ERR_NO_DATA_FOUND: u32 = 1403;
const TNS_UDS_FLAGS_IS_JSON: u32 = 0x01;
const TNS_UDS_FLAGS_IS_OSON: u32 = 0x02;
const ORA_TYPE_SIZE_NUMBER: u32 = 22;
const NUMBER_AS_TEXT_CHARS: usize = 172;
const NUMBER_MAX_DIGITS: usize = 40;

const FAST_AUTH_PREFIX_HEX: &str = concat!(
    "22010100010600707974686f6e2d6f7261636c6564620000000000000d0269036903033506000000",
    "ea180018010100000000002990030703000100cf000004010000001000000c2000b80008640005003",
    "e02000000000000030b0200000000000500000000000100010001000000020002000a000000080008",
    "00010000000c000c000a0000001700170001000000180018000100000019001900010000001a001a",
    "00010000001b001b000a0000001c001c00010000001d001d00010000001e001e00010000001f001f",
    "0001000000200020000100000021002100010000000a000a00010000000b000b0001000000280028",
    "00010000002900290001000000750075000100000078007800010000012201220001000001230123",
    "00010000012401240001000001250125000100000126012600010000012a012a00010000012b012b",
    "00010000012c012c00010000012d012d00010000012e012e00010000012f012f0001000001300130",
    "00010000013101310001000001320132000100000133013300010000013401340001000001350135",
    "000100000136013600010000013701370001000001380138000100000139013900010000013b013b",
    "00010000013c013c00010000013d013d00010000013e013e00010000013f013f0001000001400140",
    "00010000014101410001000001420142000100000143014300010000014701470001000001480148",
    "000100000149014900010000014b014b00010000014d014d00010000014e014e00010000014f014f",
    "00010000015001500001000001510151000100000152015200010000015301530001000001540154",
    "00010000015501550001000001560156000100000157015700010000015801580001000001590159",
    "00010000015a015a00010000015c015c00010000015d015d00010000016201620001000001630163",
    "000100000167016700010000016b016b00010000017c017c00010000017d017d00010000017e017e",
    "00010000017f017f0001000001800180000100000181018100010000018201820001000001830183",
    "00010000018401840001000001850185000100000186018600010000018701870001000001890189",
    "00010000018a018a00010000018b018b00010000018c018c00010000018d018d00010000018e018e",
    "00010000018f018f0001000001900190000100000191019100010000019401940001000001950195",
    "0001000001960196000100000197019700010000019d019d00010000019e019e00010000019f019f",
    "0001000001a001a00001000001a101a10001000001a201a20001000001a301a30001000001a401a4",
    "0001000001a501a50001000001a601a60001000001a701a70001000001a801a80001000001a901a9",
    "0001000001aa01aa0001000001ab01ab0001000001ad01ad0001000001ae01ae0001000001af01af",
    "0001000001b001b00001000001b101b10001000001c101c10001000001c201c20001000001c601c6",
    "0001000001c701c70001000001c801c80001000001c901c90001000001ca01ca0001000001cb01cb",
    "0001000001cc01cc0001000001cd01cd0001000001ce01ce0001000001cf01cf0001000001d201d2",
    "0001000001d301d30001000001d401d40001000001d501d50001000001d601d60001000001d701d7",
    "0001000001d801d80001000001d901d90001000001da01da0001000001db01db0001000001dc01dc",
    "0001000001dd01dd0001000001de01de0001000001df01df0001000001e001e00001000001e101e1",
    "0001000001e201e20001000001e301e30001000001e401e40001000001e501e50001000001e601e6",
    "0001000001ea01ea0001000001eb01eb0001000001ec01ec0001000001ed01ed0001000001ee01ee",
    "0001000001ef01ef0001000001f001f00001000001f201f20001000001f301f30001000001f401f4",
    "0001000001f501f50001000001f601f60001000001fd01fd0001000001fe01fe0001000002010201",
    "00010000020202020001000002040204000100000205020500010000020602060001000002070207",
    "0001000002080208000100000209020900010000020a020a00010000020b020b00010000020c020c",
    "00010000020d020d00010000020e020e00010000020f020f00010000021002100001000002110211",
    "00010000021202120001000002130213000100000214021400010000021502150001000002160216",
    "00010000021702170001000002180218000100000219021900010000021a021a00010000021b021b",
    "00010000021c021c00010000021d021d00010000021e021e00010000021f021f0001000002300230",
    "000100000235023500010000023c023c00010000023d023d00010000023e023e00010000023f023f",
    "00010000024002400001000002420242000100000233023300010000023402340001000002430243",
    "00010000024402440001000002450245000100000246024600010000024702470001000002480248",
    "00010000024902490001000000030002000a000000040002000a0000000500010001000000060002",
    "000a000000070002000a00000009000100010000000f000100010000002700270001000000440002",
    "000a0000005b0002000a0000005e000100010000005f001700010000006000600001000000610060",
    "000100000064006400010000006500650001000000660066000100000068000b00010000006a006a",
    "00010000006c006d00010000006d006d00010000006e006f00010000006f006f0001000000700070",
    "00010000007100710001000000720072000100000073007300010000007400660001000000770077",
    "0001000000c600c600010000009200920001000000980002000a000000990002000a0000009a0002",
    "000a0000009b000100010000009c000c000a000000ac0002000a000000b200b20001000000b300b3",
    "0001000000b400b40001000000b500b50001000000b600b60001000000b700b70001000000b8000c",
    "000a000000b900b90001000000ba00ba0001000000bb00bb0001000000bc00bc0001000000bd00bd",
    "0001000000be00be0001000000c300700001000000c400710001000000c500720001000000d000d0",
    "0001000000e700e70001000000e800e70001000000e900e90001000000f1006d0001000000fc00fc",
    "00010000024e024e00010000024f024f000100000250025000010000026502650001000002660266",
    "00010000026702670001000002680268000100000263026300010000026402640001000002510251",
    "00010000025202520001000002530253000100000254025400010000025502550001000002560256",
    "00010000025702570001000002580258000100000259025900010000025a025a00010000025b025b",
    "00010000025c025c00010000025d025d00010000026e026e00010000026f026f0001000002700270",
    "00010000027102710001000002720272000100000273027300010000027402740001000002750275",
    "00010000027602760001000002770277000100000278027800010000027d027d00010000027e027e",
    "00010000027c027c00010000027f027f0001000002970297000100000280028000010000028c028c",
    "0001000002860286000100000287028700010000007f007f00010000029402940001000002950295",
    "000100000299029900010000029d029d00010000029e029e000100000000"
);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptInfo {
    pub protocol_version: u16,
    pub protocol_options: u16,
    pub sdu: u32,
    pub supports_fast_auth: bool,
    pub supports_oob_check: bool,
    pub supports_end_of_response: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AuthResponse {
    pub session_data: BTreeMap<String, String>,
    pub verifier_type: Option<u32>,
    pub capabilities: Option<ClientCapabilities>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientCapabilities {
    pub ttc_field_version: u8,
    pub max_string_size: u32,
}

impl Default for ClientCapabilities {
    fn default() -> Self {
        Self {
            ttc_field_version: 24,
            max_string_size: 32_767,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnMetadata {
    pub name: String,
    pub ora_type_num: u8,
    pub csfrm: u8,
    pub precision: i8,
    pub scale: i8,
    pub buffer_size: u32,
    pub max_size: u32,
    pub nulls_allowed: bool,
    pub is_json: bool,
    pub is_oson: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryValue {
    Text(String),
    Raw(Vec<u8>),
    Number { text: String, is_integer: bool },
}

#[derive(Clone, Debug, PartialEq)]
pub enum BindValue {
    Null,
    Text(String),
    Raw(Vec<u8>),
    Number(String),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<ColumnMetadata>,
    pub rows: Vec<Vec<Option<QueryValue>>>,
    pub cursor_id: u32,
    pub row_count: u64,
    pub more_rows: bool,
}

pub fn build_connect_packet_payload(connect_data: &str, sdu: u16) -> Result<Vec<u8>> {
    let connect_bytes = connect_data.as_bytes();
    let connect_len =
        u16::try_from(connect_bytes.len()).map_err(|_| ProtocolError::PacketTooLarge {
            length: connect_bytes.len(),
        })?;

    let mut writer = TtcWriter::new();
    writer.write_u16be(TNS_VERSION_DESIRED);
    writer.write_u16be(TNS_VERSION_MIN);
    writer.write_u16be(TNS_GSO_DONT_CARE);
    writer.write_u16be(sdu);
    writer.write_u16be(sdu);
    writer.write_u16be(TNS_PROTOCOL_CHARACTERISTICS);
    writer.write_u16be(0);
    writer.write_u16be(1);
    writer.write_u16be(connect_len);
    writer.write_u16be(74);
    writer.write_u32be(0);
    let nsi_flags = TNS_NSI_SUPPORT_SECURITY_RENEG | TNS_NSI_DISABLE_NA;
    writer.write_u8(nsi_flags);
    writer.write_u8(nsi_flags);
    writer.write_u64be(0);
    writer.write_u64be(0);
    writer.write_u64be(0);
    writer.write_u32be(u32::from(sdu));
    writer.write_u32be(u32::from(sdu));
    writer.write_u32be(0);
    writer.write_u32be(0);
    writer.write_raw(connect_bytes);
    Ok(writer.into_bytes())
}

pub fn parse_accept_payload(payload: &[u8]) -> Result<AcceptInfo> {
    let mut reader = TtcReader::new(payload);
    let protocol_version = reader.read_u16be()?;
    let protocol_options = reader.read_u16be()?;
    reader.skip(10)?;
    let flags1 = reader.read_u8()?;
    if has_u8_flag(flags1, TNS_NSI_NA_REQUIRED) {
        return Err(ProtocolError::UnsupportedFeature(
            "Native Network Encryption and Data Integrity",
        ));
    }
    reader.skip(9)?;
    let sdu = reader.read_u32be()?;
    let mut flags2 = 0;
    if protocol_version >= 318 {
        reader.skip(5)?;
        flags2 = reader.read_u32be()?;
    }

    Ok(AcceptInfo {
        protocol_version,
        protocol_options,
        sdu,
        supports_fast_auth: has_u32_flag(flags2, TNS_ACCEPT_FLAG_FAST_AUTH),
        supports_oob_check: has_u32_flag(flags2, TNS_ACCEPT_FLAG_CHECK_OOB),
        supports_end_of_response: protocol_version >= 319
            && has_u32_flag(flags2, TNS_ACCEPT_FLAG_HAS_END_OF_RESPONSE),
    })
}

pub fn build_fast_auth_phase_one_payload(
    user: &str,
    program: &str,
    machine: &str,
    osuser: &str,
    terminal: &str,
    pid: u32,
) -> Result<Vec<u8>> {
    let mut out = Vec::from_hex(FAST_AUTH_PREFIX_HEX)
        .map_err(|_| ProtocolError::TtcDecode("invalid static fast-auth prefix"))?;
    append_auth_phase_one(&mut out, user, program, machine, osuser, terminal, pid)?;
    Ok(out)
}

pub fn build_function_payload(function_code: u8) -> Vec<u8> {
    build_function_payload_with_seq(function_code, 1)
}

pub fn build_function_payload_with_seq(function_code: u8, seq_num: u8) -> Vec<u8> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(function_code, seq_num);
    writer.write_ub8(0);
    writer.into_bytes()
}

pub fn build_execute_query_payload(sql: &str, prefetch_rows: u32) -> Result<Vec<u8>> {
    build_execute_query_payload_with_seq(sql, prefetch_rows, 1)
}

pub fn build_execute_query_payload_with_seq(
    sql: &str,
    prefetch_rows: u32,
    seq_num: u8,
) -> Result<Vec<u8>> {
    build_execute_payload_with_seq(sql, prefetch_rows, seq_num, true)
}

pub fn build_execute_payload_with_seq(
    sql: &str,
    prefetch_rows: u32,
    seq_num: u8,
    is_query: bool,
) -> Result<Vec<u8>> {
    build_execute_payload_with_binds_with_seq(sql, prefetch_rows, seq_num, is_query, &[])
}

pub fn build_execute_payload_with_binds_with_seq(
    sql: &str,
    prefetch_rows: u32,
    seq_num: u8,
    is_query: bool,
    binds: &[BindValue],
) -> Result<Vec<u8>> {
    let bind_rows = if binds.is_empty() {
        Vec::new()
    } else {
        vec![binds.to_vec()]
    };
    build_execute_payload_with_bind_rows_with_seq(sql, prefetch_rows, seq_num, is_query, &bind_rows)
}

pub fn build_execute_payload_with_bind_rows_with_seq(
    sql: &str,
    prefetch_rows: u32,
    seq_num: u8,
    is_query: bool,
    bind_rows: &[Vec<BindValue>],
) -> Result<Vec<u8>> {
    let sql_bytes = sql.as_bytes();
    let sql_len =
        u32::try_from(sql_bytes.len()).map_err(|_| ProtocolError::InvalidPacketLength {
            length: sql_bytes.len(),
            minimum: 0,
        })?;
    let bind_count = bind_rows.first().map_or(0, Vec::len);
    for row in bind_rows {
        if row.len() != bind_count {
            return Err(ProtocolError::TtcDecode("inconsistent bind row width"));
        }
    }
    let bind_count = u32::try_from(bind_count).map_err(|_| ProtocolError::InvalidPacketLength {
        length: bind_count,
        minimum: 0,
    })?;
    let bind_row_count =
        u32::try_from(bind_rows.len()).map_err(|_| ProtocolError::InvalidPacketLength {
            length: bind_rows.len(),
            minimum: 0,
        })?;
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_EXECUTE, seq_num);
    writer.write_ub8(0);

    let mut options = TNS_EXEC_OPTION_PARSE | TNS_EXEC_OPTION_EXECUTE | TNS_EXEC_OPTION_NOT_PLSQL;
    if is_query {
        options |= TNS_EXEC_OPTION_FETCH;
    }
    if bind_count > 0 {
        options |= TNS_EXEC_OPTION_BIND;
    }
    let num_iters = if is_query { prefetch_rows } else { 1 };
    let exec_count = if is_query { 0 } else { bind_row_count.max(1) };
    let query_flag = u32::from(is_query);
    let exec_flags = if is_query {
        TNS_EXEC_FLAGS_IMPLICIT_RESULTSET
    } else {
        0
    };
    writer.write_ub4(options);
    writer.write_ub4(0);
    writer.write_u8(1);
    writer.write_ub4(sql_len);
    writer.write_u8(1);
    writer.write_ub4(13);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_ub4(num_iters);
    writer.write_ub4(TNS_MAX_LONG_LENGTH);
    if bind_count == 0 {
        writer.write_u8(0);
        writer.write_ub4(0);
    } else {
        writer.write_u8(1);
        writer.write_ub4(bind_count);
    }
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_u8(1);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(0);

    writer.write_bytes_with_length(sql_bytes)?;
    writer.write_ub4(1);
    writer.write_ub4(exec_count);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(query_flag);
    writer.write_ub4(0);
    writer.write_ub4(exec_flags);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    if !bind_rows.is_empty() {
        write_bind_params(&mut writer, bind_rows)?;
    }
    Ok(writer.into_bytes())
}

fn write_bind_params(writer: &mut TtcWriter, bind_rows: &[Vec<BindValue>]) -> Result<()> {
    let Some(first_row) = bind_rows.first() else {
        return Ok(());
    };
    for value in first_row {
        write_bind_metadata(writer, value);
    }
    for row in bind_rows {
        writer.write_u8(TNS_MSG_TYPE_ROW_DATA);
        for value in row {
            write_bind_value(writer, value)?;
        }
    }
    Ok(())
}

fn write_bind_metadata(writer: &mut TtcWriter, value: &BindValue) {
    let (ora_type_num, csfrm, buffer_size) = bind_metadata(value);
    writer.write_u8(ora_type_num);
    writer.write_u8(TNS_BIND_USE_INDICATORS);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(buffer_size);
    writer.write_ub4(0);
    writer.write_ub8(0);
    writer.write_ub4(0);
    writer.write_ub2(0);
    if csfrm != 0 {
        writer.write_ub2(TNS_CHARSET_UTF8);
    } else {
        writer.write_ub2(0);
    }
    writer.write_u8(csfrm);
    writer.write_ub4(0);
    writer.write_ub4(0);
}

fn bind_metadata(value: &BindValue) -> (u8, u8, u32) {
    match value {
        BindValue::Null => (ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 1),
        BindValue::Text(value) => (
            ORA_TYPE_NUM_VARCHAR,
            CS_FORM_IMPLICIT,
            u32::try_from(value.len()).unwrap_or(u32::MAX).max(1),
        ),
        BindValue::Raw(value) => (
            ORA_TYPE_NUM_RAW,
            0,
            u32::try_from(value.len()).unwrap_or(u32::MAX).max(1),
        ),
        BindValue::Number(_) => (ORA_TYPE_NUM_NUMBER, 0, ORA_TYPE_SIZE_NUMBER),
    }
}

fn write_bind_value(writer: &mut TtcWriter, value: &BindValue) -> Result<()> {
    match value {
        BindValue::Null => {
            writer.write_u8(0);
            Ok(())
        }
        BindValue::Text(value) => writer.write_bytes_with_length(value.as_bytes()),
        BindValue::Raw(value) => writer.write_bytes_with_length(value),
        BindValue::Number(value) => {
            let bytes = encode_number_text(value)?;
            writer.write_bytes_with_length(&bytes)
        }
    }
}

pub fn build_fetch_payload(cursor_id: u32, arraysize: u32) -> Vec<u8> {
    build_fetch_payload_with_seq(cursor_id, arraysize, 1)
}

pub fn build_fetch_payload_with_seq(cursor_id: u32, arraysize: u32, seq_num: u8) -> Vec<u8> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_FETCH, seq_num);
    writer.write_ub8(0);
    writer.write_ub4(cursor_id);
    writer.write_ub4(arraysize);
    writer.into_bytes()
}

pub fn parse_query_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
) -> Result<QueryResult> {
    parse_query_response_with_previous(payload, capabilities, None)
}

pub fn parse_query_response_with_previous(
    payload: &[u8],
    capabilities: ClientCapabilities,
    previous_row: Option<&[Option<QueryValue>]>,
) -> Result<QueryResult> {
    parse_query_response_with_context(payload, capabilities, &[], previous_row)
}

pub fn parse_query_response_with_context(
    payload: &[u8],
    capabilities: ClientCapabilities,
    previous_columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
) -> Result<QueryResult> {
    let mut reader = TtcReader::new(payload);
    let mut result = QueryResult {
        columns: previous_columns.to_vec(),
        more_rows: true,
        ..QueryResult::default()
    };
    let mut bit_vector: Option<Vec<u8>> = None;
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            TNS_MSG_TYPE_DESCRIBE_INFO => {
                let _describe_name = reader.read_bytes()?;
                result.columns.clear();
                parse_describe_info(&mut reader, capabilities, &mut result)?;
            }
            TNS_MSG_TYPE_ROW_HEADER => {
                bit_vector = parse_row_header(&mut reader)?;
            }
            TNS_MSG_TYPE_ROW_DATA => {
                parse_row_data(
                    &mut reader,
                    &mut result,
                    bit_vector.as_deref(),
                    previous_row,
                )?;
                bit_vector = None;
            }
            TNS_MSG_TYPE_BIT_VECTOR => {
                bit_vector = Some(parse_bit_vector(&mut reader, result.columns.len())?);
            }
            TNS_MSG_TYPE_PARAMETER => skip_query_return_parameters(&mut reader)?,
            TNS_MSG_TYPE_STATUS => {
                let _call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => skip_server_side_piggyback(&mut reader)?,
            TNS_MSG_TYPE_END_OF_RESPONSE => break,
            TNS_MSG_TYPE_ERROR => {
                let info = parse_server_error_info(&mut reader, capabilities.ttc_field_version)?;
                if info.cursor_id != 0 {
                    result.cursor_id = u32::from(info.cursor_id);
                }
                result.row_count = info.row_count;
                if info.number == TNS_ERR_NO_DATA_FOUND {
                    result.more_rows = false;
                } else if info.number != 0 {
                    return Err(ProtocolError::ServerError(info.message));
                }
            }
            _ => {
                let position = reader.position().saturating_sub(1);
                if let Some(message) =
                    find_embedded_server_error(payload, capabilities.ttc_field_version, position)
                {
                    return Err(ProtocolError::ServerError(message));
                }
                return Err(ProtocolError::UnknownMessageType {
                    message_type,
                    position,
                });
            }
        }
    }
    Ok(result)
}

fn find_embedded_server_error(
    payload: &[u8],
    ttc_field_version: u8,
    position: usize,
) -> Option<String> {
    let start = position.saturating_sub(64);
    for candidate in start..=position {
        if !matches!(payload.get(candidate).copied(), Some(TNS_MSG_TYPE_ERROR)) {
            continue;
        }
        let mut reader = TtcReader::new(payload.get(candidate + 1..)?);
        let info = parse_server_error_info(&mut reader, ttc_field_version).ok()?;
        if info.number != 0 && info.message.starts_with("ORA-") {
            return Some(info.message);
        }
    }
    None
}

pub fn append_auth_phase_one(
    out: &mut Vec<u8>,
    user: &str,
    program: &str,
    machine: &str,
    osuser: &str,
    terminal: &str,
    pid: u32,
) -> Result<()> {
    let mut writer = TtcWriter::new();
    writer.write_function_code(TNS_FUNC_AUTH_PHASE_ONE);
    write_auth_header(&mut writer, user, TNS_AUTH_MODE_LOGON, 5)?;
    write_key_value(&mut writer, "AUTH_TERMINAL", terminal, 0)?;
    write_key_value(&mut writer, "AUTH_PROGRAM_NM", program, 0)?;
    write_key_value(&mut writer, "AUTH_MACHINE", machine, 0)?;
    write_key_value(&mut writer, "AUTH_PID", &pid.to_string(), 0)?;
    write_key_value(&mut writer, "AUTH_SID", osuser, 0)?;
    out.extend_from_slice(&writer.into_bytes());
    Ok(())
}

pub fn build_auth_phase_two_payload(
    user: &str,
    encrypted: &crate::crypto::EncryptedPassword,
    driver_name: &str,
    version_num: u32,
    connect_string: &str,
) -> Result<Vec<u8>> {
    build_auth_phase_two_payload_with_seq(
        user,
        encrypted,
        driver_name,
        version_num,
        connect_string,
        1,
    )
}

pub fn build_auth_phase_two_payload_with_seq(
    user: &str,
    encrypted: &crate::crypto::EncryptedPassword,
    driver_name: &str,
    version_num: u32,
    connect_string: &str,
    seq_num: u8,
) -> Result<Vec<u8>> {
    build_auth_phase_two_payload_with_context_with_seq(
        user,
        encrypted,
        driver_name,
        version_num,
        connect_string,
        seq_num,
        &[],
    )
}

pub fn build_auth_phase_two_payload_with_context_with_seq(
    user: &str,
    encrypted: &crate::crypto::EncryptedPassword,
    driver_name: &str,
    version_num: u32,
    connect_string: &str,
    seq_num: u8,
    app_context: &[(String, String, String)],
) -> Result<Vec<u8>> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_AUTH_PHASE_TWO, seq_num);
    writer.write_ub8(0);
    let mut num_pairs = 6u32;
    if encrypted.speedy_key.is_some() {
        num_pairs += 1;
    }
    if !connect_string.is_empty() {
        num_pairs += 1;
    }
    let app_context_pairs =
        app_context
            .len()
            .checked_mul(3)
            .ok_or(ProtocolError::InvalidPacketLength {
                length: app_context.len(),
                minimum: 0,
            })?;
    num_pairs +=
        u32::try_from(app_context_pairs).map_err(|_| ProtocolError::InvalidPacketLength {
            length: app_context.len(),
            minimum: 0,
        })?;
    write_auth_header(
        &mut writer,
        user,
        TNS_AUTH_MODE_LOGON | TNS_AUTH_MODE_WITH_PASSWORD,
        num_pairs,
    )?;
    write_key_value(&mut writer, "AUTH_SESSKEY", &encrypted.session_key, 1)?;
    if let Some(speedy_key) = &encrypted.speedy_key {
        write_key_value(&mut writer, "AUTH_PBKDF2_SPEEDY_KEY", speedy_key, 0)?;
    }
    write_key_value(&mut writer, "AUTH_PASSWORD", &encrypted.password, 0)?;
    write_key_value(&mut writer, "SESSION_CLIENT_CHARSET", "873", 0)?;
    write_key_value(&mut writer, "SESSION_CLIENT_DRIVER_NAME", driver_name, 0)?;
    write_key_value(
        &mut writer,
        "SESSION_CLIENT_VERSION",
        &version_num.to_string(),
        0,
    )?;
    write_key_value(
        &mut writer,
        "AUTH_ALTER_SESSION",
        "ALTER SESSION SET TIME_ZONE='+00:00'\0",
        1,
    )?;
    for (namespace, name, value) in app_context {
        write_key_value(&mut writer, "AUTH_APPCTX_NSPACE\0", namespace, 0)?;
        write_key_value(&mut writer, "AUTH_APPCTX_ATTR\0", name, 0)?;
        write_key_value(&mut writer, "AUTH_APPCTX_VALUE\0", value, 0)?;
    }
    if !connect_string.is_empty() {
        write_key_value(&mut writer, "AUTH_CONNECT_STRING", connect_string, 0)?;
    }
    Ok(writer.into_bytes())
}

pub fn parse_auth_response(payload: &[u8]) -> Result<AuthResponse> {
    let mut reader = TtcReader::new(payload);
    let mut response = AuthResponse::default();
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            TNS_MSG_TYPE_PROTOCOL => {
                if let Some(capabilities) = skip_protocol_message(&mut reader)? {
                    response.capabilities = Some(capabilities);
                }
            }
            TNS_MSG_TYPE_DATA_TYPES => skip_data_types_response(&mut reader)?,
            TNS_MSG_TYPE_PARAMETER => {
                let mut parsed = parse_return_parameters(&mut reader)?;
                response.session_data.append(&mut parsed.session_data);
                if parsed.verifier_type.is_some() {
                    response.verifier_type = parsed.verifier_type;
                }
            }
            TNS_MSG_TYPE_STATUS => {
                let _call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => skip_server_side_piggyback(&mut reader)?,
            TNS_MSG_TYPE_END_OF_RESPONSE => break,
            TNS_MSG_TYPE_ERROR => {
                if let Some(message) = parse_server_error(&mut reader, 13)? {
                    return Err(ProtocolError::ServerError(message));
                }
            }
            _ => {
                return Err(ProtocolError::UnknownMessageType {
                    message_type,
                    position: reader.position().saturating_sub(1),
                })
            }
        }
    }
    Ok(response)
}

fn write_auth_header(
    writer: &mut TtcWriter,
    user: &str,
    auth_mode: u32,
    num_pairs: u32,
) -> Result<()> {
    let user_bytes = user.as_bytes();
    writer.write_u8(u8::from(!user_bytes.is_empty()));
    writer.write_ub4(u32::try_from(user_bytes.len()).map_err(|_| {
        ProtocolError::InvalidPacketLength {
            length: user_bytes.len(),
            minimum: 0,
        }
    })?);
    writer.write_ub4(auth_mode);
    writer.write_u8(1);
    writer.write_ub4(num_pairs);
    writer.write_u8(1);
    writer.write_u8(1);
    if !user_bytes.is_empty() {
        writer.write_bytes_with_length(user_bytes)?;
    }
    Ok(())
}

fn write_key_value(writer: &mut TtcWriter, key: &str, value: &str, flags: u32) -> Result<()> {
    writer.write_str_two_lengths(key)?;
    writer.write_str_two_lengths(value)?;
    writer.write_ub4(flags);
    Ok(())
}

fn skip_protocol_message(reader: &mut TtcReader<'_>) -> Result<Option<ClientCapabilities>> {
    let _server_version = reader.read_u8()?;
    reader.skip(1)?;
    loop {
        if reader.read_u8()? == 0 {
            break;
        }
    }
    let _charset = reader.read_u16le()?;
    let _server_flags = reader.read_u8()?;
    let num_elem = reader.read_u16le()?;
    reader.skip(usize::from(num_elem) * 5)?;
    let fdo_len = reader.read_u16be()?;
    reader.skip(usize::from(fdo_len))?;
    let compile_caps = reader.read_bytes()?;
    let runtime_caps = reader.read_bytes()?;
    let Some(compile_caps) = compile_caps else {
        return Ok(None);
    };
    let server_ttc_field_version = compile_caps
        .get(TNS_CCAP_FIELD_VERSION)
        .copied()
        .unwrap_or_else(|| ClientCapabilities::default().ttc_field_version);
    let ttc_field_version =
        server_ttc_field_version.max(ClientCapabilities::default().ttc_field_version);
    let max_string_size = if runtime_caps
        .as_deref()
        .and_then(|caps| caps.get(TNS_RCAP_TTC))
        .is_some_and(|flags| flags & TNS_RCAP_TTC_32K != 0)
    {
        32_767
    } else {
        4_000
    };
    Ok(Some(ClientCapabilities {
        ttc_field_version,
        max_string_size,
    }))
}

fn skip_data_types_response(reader: &mut TtcReader<'_>) -> Result<()> {
    loop {
        let data_type = reader.read_u16be()?;
        if data_type == 0 {
            break;
        }
        let conv_data_type = reader.read_u16be()?;
        if conv_data_type != 0 {
            reader.skip(4)?;
        }
    }
    Ok(())
}

fn parse_describe_info(
    reader: &mut TtcReader<'_>,
    capabilities: ClientCapabilities,
    result: &mut QueryResult,
) -> Result<()> {
    let _max_row_size = reader.read_ub4()?;
    let num_columns = reader.read_ub4()?;
    result.columns.clear();
    if num_columns > 0 {
        reader.skip(1)?;
    }
    for _ in 0..num_columns {
        result
            .columns
            .push(parse_column_metadata(reader, capabilities)?);
    }
    let _current_date = reader.read_bytes_with_length()?;
    let _dcbflag = reader.read_ub4()?;
    let _dcbmdbz = reader.read_ub4()?;
    let _dcbmnpr = reader.read_ub4()?;
    let _dcbmxpr = reader.read_ub4()?;
    let _dcbqcky = reader.read_bytes_with_length()?;
    Ok(())
}

fn parse_column_metadata(
    reader: &mut TtcReader<'_>,
    capabilities: ClientCapabilities,
) -> Result<ColumnMetadata> {
    let ora_type_num = reader.read_u8()?;
    reader.skip(1)?;
    let precision = reader.read_i8()?;
    let scale = reader.read_i8()?;
    let buffer_size = reader.read_ub4()?;
    let _max_array_elements = reader.read_ub4()?;
    let _cont_flags = reader.read_ub8()?;
    let _oid = reader.read_bytes_with_length()?;
    let _version = reader.read_ub2()?;
    let _charset_id = reader.read_ub2()?;
    let csfrm = reader.read_u8()?;
    let mut max_size = reader.read_ub4()?;
    if ora_type_num == ORA_TYPE_NUM_RAW {
        max_size = buffer_size;
    }
    if capabilities.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2 {
        let _oaccolid = reader.read_ub4()?;
    }
    let nulls_allowed = reader.read_u8()? != 0;
    reader.skip(1)?;
    let name = reader.read_string_with_length()?.unwrap_or_default();
    let _schema = reader.read_string_with_length()?;
    let _type_name = reader.read_string_with_length()?;
    let _column_position = reader.read_ub2()?;
    let uds_flags = reader.read_ub4()?;
    if capabilities.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1 {
        let _domain_schema = reader.read_string_with_length()?;
        let _domain_name = reader.read_string_with_length()?;
    }
    if capabilities.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_3 {
        let num_annotations = reader.read_ub4()?;
        if num_annotations > 0 {
            reader.skip(1)?;
            let num_annotations = reader.read_ub4()?;
            reader.skip(1)?;
            for _ in 0..num_annotations {
                let _key = reader.read_string_with_length()?;
                let _value = reader.read_string_with_length()?;
                let _flags = reader.read_ub4()?;
            }
            let _flags = reader.read_ub4()?;
        }
    }
    if capabilities.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_4 {
        let _vector_dimensions = reader.read_ub4()?;
        reader.skip(2)?;
    }

    Ok(ColumnMetadata {
        name,
        ora_type_num,
        csfrm,
        precision,
        scale,
        buffer_size,
        max_size,
        nulls_allowed,
        is_json: uds_flags & TNS_UDS_FLAGS_IS_JSON != 0,
        is_oson: uds_flags & TNS_UDS_FLAGS_IS_OSON != 0,
    })
}

fn parse_row_header(reader: &mut TtcReader<'_>) -> Result<Option<Vec<u8>>> {
    reader.skip(1)?;
    let _num_requests = reader.read_ub2()?;
    let _iteration_number = reader.read_ub4()?;
    let _num_iters = reader.read_ub4()?;
    let _buffer_length = reader.read_ub2()?;
    let num_bytes = reader.read_ub4()?;
    let bit_vector = if num_bytes > 0 {
        reader.skip(1)?;
        Some(reader.read_raw(num_bytes as usize)?.to_vec())
    } else {
        None
    };
    let _rxhrid = reader.read_bytes_with_length()?;
    Ok(bit_vector)
}

fn parse_bit_vector(reader: &mut TtcReader<'_>, num_columns: usize) -> Result<Vec<u8>> {
    let _num_columns_sent = reader.read_ub2()?;
    let num_bytes = num_columns.div_ceil(8);
    Ok(reader.read_raw(num_bytes)?.to_vec())
}

fn parse_row_data(
    reader: &mut TtcReader<'_>,
    result: &mut QueryResult,
    bit_vector: Option<&[u8]>,
    previous_row: Option<&[Option<QueryValue>]>,
) -> Result<()> {
    let mut row = Vec::with_capacity(result.columns.len());
    for (index, metadata) in result.columns.iter().enumerate() {
        if is_duplicate_column(bit_vector, index) {
            let previous = result
                .rows
                .last()
                .map(Vec::as_slice)
                .or(previous_row)
                .and_then(|last| last.get(index))
                .cloned()
                .ok_or(ProtocolError::TtcDecode(
                    "duplicate row data without previous row",
                ))?;
            row.push(previous);
            continue;
        }
        row.push(parse_column_value(reader, metadata)?);
    }
    result.rows.push(row);
    Ok(())
}

fn is_duplicate_column(bit_vector: Option<&[u8]>, column_num: usize) -> bool {
    let Some(bit_vector) = bit_vector else {
        return false;
    };
    let byte_num = column_num / 8;
    let bit_num = column_num % 8;
    bit_vector
        .get(byte_num)
        .is_some_and(|byte| byte & (1 << bit_num) == 0)
}

fn parse_column_value(
    reader: &mut TtcReader<'_>,
    metadata: &ColumnMetadata,
) -> Result<Option<QueryValue>> {
    if metadata.buffer_size == 0
        && !matches!(
            metadata.ora_type_num,
            ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_LONG_RAW
        )
    {
        return Ok(None);
    }
    match metadata.ora_type_num {
        ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG => {
            let Some(bytes) = reader.read_bytes()? else {
                return Ok(None);
            };
            decode_text_value(&bytes, metadata.csfrm).map(|value| Some(QueryValue::Text(value)))
        }
        ORA_TYPE_NUM_RAW | ORA_TYPE_NUM_LONG_RAW => Ok(reader.read_bytes()?.map(QueryValue::Raw)),
        ORA_TYPE_NUM_NUMBER => {
            let Some(bytes) = reader.read_bytes()? else {
                return Ok(None);
            };
            decode_number_value(&bytes).map(Some)
        }
        _ => Err(ProtocolError::UnsupportedFeature("query column type")),
    }
}

fn encode_number_text(value: &str) -> Result<Vec<u8>> {
    let value = value.as_bytes();
    if value.is_empty() {
        return Err(ProtocolError::TtcDecode("empty NUMBER bind"));
    }
    if value.len() > NUMBER_AS_TEXT_CHARS {
        return Err(ProtocolError::TtcDecode("NUMBER bind text too long"));
    }

    let mut pos = 0;
    let mut is_negative = false;
    if value[pos] == b'-' {
        is_negative = true;
        pos += 1;
    }

    let mut digits = Vec::with_capacity(NUMBER_AS_TEXT_CHARS);
    while pos < value.len() {
        if matches!(value[pos], b'.' | b'e' | b'E') {
            break;
        }
        if !value[pos].is_ascii_digit() {
            return Err(ProtocolError::TtcDecode("invalid NUMBER bind"));
        }
        let digit = value[pos] - b'0';
        pos += 1;
        if digit == 0 && digits.is_empty() {
            continue;
        }
        digits.push(digit);
    }
    let mut decimal_point_index = i32::try_from(digits.len()).unwrap_or(i32::MAX);

    if pos < value.len() && value[pos] == b'.' {
        pos += 1;
        while pos < value.len() {
            if matches!(value[pos], b'e' | b'E') {
                break;
            }
            if !value[pos].is_ascii_digit() {
                return Err(ProtocolError::TtcDecode("invalid NUMBER bind"));
            }
            let digit = value[pos] - b'0';
            pos += 1;
            if digit == 0 && digits.is_empty() {
                decimal_point_index -= 1;
                continue;
            }
            digits.push(digit);
        }
    }

    if pos < value.len() && matches!(value[pos], b'e' | b'E') {
        pos += 1;
        let mut exponent_is_negative = false;
        if pos < value.len() {
            if value[pos] == b'-' {
                exponent_is_negative = true;
                pos += 1;
            } else if value[pos] == b'+' {
                pos += 1;
            }
        }
        let exponent_start = pos;
        while pos < value.len() {
            if !value[pos].is_ascii_digit() {
                return Err(ProtocolError::TtcDecode("invalid NUMBER exponent"));
            }
            pos += 1;
        }
        if exponent_start == pos {
            return Err(ProtocolError::TtcDecode("empty NUMBER exponent"));
        }
        let exponent_text = std::str::from_utf8(&value[exponent_start..pos])
            .map_err(|_| ProtocolError::TtcDecode("invalid NUMBER exponent"))?;
        let mut exponent = exponent_text
            .parse::<i32>()
            .map_err(|_| ProtocolError::TtcDecode("invalid NUMBER exponent"))?;
        if exponent_is_negative {
            exponent = -exponent;
        }
        decimal_point_index += exponent;
    }

    if pos < value.len() {
        return Err(ProtocolError::TtcDecode("invalid NUMBER bind suffix"));
    }

    while digits.last().is_some_and(|digit| *digit == 0) {
        digits.pop();
    }
    if digits.len() > NUMBER_MAX_DIGITS || decimal_point_index > 126 || decimal_point_index < -129 {
        return Err(ProtocolError::TtcDecode("NUMBER bind out of range"));
    }

    let mut prepend_zero = false;
    if decimal_point_index % 2 != 0 {
        prepend_zero = true;
        if !digits.is_empty() {
            digits.push(0);
            decimal_point_index += 1;
        }
    }
    if digits.len() % 2 == 1 {
        digits.push(0);
    }

    if digits.is_empty() {
        return Ok(vec![128]);
    }

    let mut encoded = Vec::with_capacity(digits.len() / 2 + 2);
    let exponent_on_wire = decimal_point_index / 2 + 192;
    if !(0..=255).contains(&exponent_on_wire) {
        return Err(ProtocolError::TtcDecode(
            "NUMBER bind exponent out of range",
        ));
    }
    let exponent_byte = exponent_on_wire as u8;
    encoded.push(if is_negative {
        !exponent_byte
    } else {
        exponent_byte
    });

    let mut digit_pos = 0;
    for pair_num in 0..(digits.len() / 2) {
        let mut digit = if pair_num == 0 && prepend_zero {
            let digit = digits[digit_pos];
            digit_pos += 1;
            digit
        } else {
            let digit = digits[digit_pos] * 10 + digits[digit_pos + 1];
            digit_pos += 2;
            digit
        };
        if is_negative {
            digit = 101 - digit;
        } else {
            digit += 1;
        }
        encoded.push(digit);
    }

    if is_negative && digits.len() < NUMBER_MAX_DIGITS {
        encoded.push(102);
    }

    Ok(encoded)
}

fn decode_number_value(bytes: &[u8]) -> Result<QueryValue> {
    if bytes.len() > 21 {
        return Err(ProtocolError::TtcDecode("encoded NUMBER too long"));
    }
    let Some(&first) = bytes.first() else {
        return Err(ProtocolError::TtcDecode("empty NUMBER"));
    };
    let is_positive = first & 0x80 != 0;
    if bytes.len() == 1 {
        if is_positive {
            return Ok(QueryValue::Number {
                text: "0".into(),
                is_integer: true,
            });
        }
        return Ok(QueryValue::Number {
            text: "-1e126".into(),
            is_integer: true,
        });
    }

    let exponent_byte = if is_positive { first } else { !first };
    let exponent = i16::from(exponent_byte) - 193;
    let mut decimal_point_index = exponent * 2 + 2;
    let mut end = bytes.len();
    if !is_positive && bytes[end - 1] == 102 {
        end -= 1;
    }

    let mut digits = Vec::with_capacity((end.saturating_sub(1)) * 2);
    for (index, encoded) in bytes.iter().enumerate().take(end).skip(1) {
        let value = if is_positive {
            encoded.saturating_sub(1)
        } else {
            101u8.saturating_sub(*encoded)
        };

        let first_digit = value / 10;
        if first_digit == 0 && digits.is_empty() {
            decimal_point_index -= 1;
        } else if first_digit == 10 {
            digits.push(1);
            digits.push(0);
            decimal_point_index += 1;
        } else if first_digit != 0 || index > 0 {
            digits.push(first_digit);
        }

        let second_digit = value % 10;
        if second_digit != 0 || index < end - 1 {
            digits.push(second_digit);
        }
    }

    let mut text = String::with_capacity(digits.len() + 4);
    let mut is_integer = true;
    if !is_positive {
        text.push('-');
    }
    if decimal_point_index <= 0 {
        text.push_str("0.");
        is_integer = false;
        for _ in decimal_point_index..0 {
            text.push('0');
        }
    }
    for (index, digit) in digits.iter().enumerate() {
        if index > 0 && i16::try_from(index).unwrap_or(i16::MAX) == decimal_point_index {
            text.push('.');
            is_integer = false;
        }
        text.push(char::from(b'0' + *digit));
    }
    if decimal_point_index > i16::try_from(digits.len()).unwrap_or(i16::MAX) {
        for _ in i16::try_from(digits.len()).unwrap_or(i16::MAX)..decimal_point_index {
            text.push('0');
        }
    }

    Ok(QueryValue::Number { text, is_integer })
}

fn decode_text_value(bytes: &[u8], csfrm: u8) -> Result<String> {
    if csfrm == CS_FORM_NCHAR {
        let units = bytes
            .chunks_exact(2)
            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
            .collect::<Vec<_>>();
        if units.len() * 2 != bytes.len() {
            return Err(ProtocolError::TtcDecode("invalid UTF-16 text length"));
        }
        String::from_utf16(&units).map_err(|_| ProtocolError::TtcDecode("invalid UTF-16 text"))
    } else {
        String::from_utf8(bytes.to_vec())
            .map_err(|_| ProtocolError::TtcDecode("invalid UTF-8 text"))
    }
}

fn skip_query_return_parameters(reader: &mut TtcReader<'_>) -> Result<()> {
    let num_params = reader.read_ub2()?;
    for _ in 0..num_params {
        let _value = reader.read_ub4()?;
    }
    let num_bytes = reader.read_ub2()?;
    if num_bytes > 0 {
        reader.skip(usize::from(num_bytes))?;
    }
    let num_pairs = reader.read_ub2()?;
    skip_keyword_value_pairs(reader, num_pairs)?;
    let num_bytes = reader.read_ub2()?;
    if num_bytes > 0 {
        reader.skip(usize::from(num_bytes))?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ServerErrorInfo {
    number: u32,
    message: String,
    cursor_id: u16,
    row_count: u64,
}

fn parse_server_error(reader: &mut TtcReader<'_>, ttc_field_version: u8) -> Result<Option<String>> {
    let info = parse_server_error_info(reader, ttc_field_version)?;
    if info.number == 0 {
        Ok(None)
    } else if info.message.is_empty() {
        Ok(Some(format!("ORA-{:05}", info.number)))
    } else {
        Ok(Some(info.message))
    }
}

fn parse_server_error_info(
    reader: &mut TtcReader<'_>,
    ttc_field_version: u8,
) -> Result<ServerErrorInfo> {
    let _call_status = reader.read_ub4()?;
    let _seq = reader.read_ub2()?;
    let _current_row = reader.read_ub4()?;
    let _error_number = reader.read_ub2()?;
    let _array_elem_error_1 = reader.read_ub2()?;
    let _array_elem_error_2 = reader.read_ub2()?;
    let cursor_id = reader.read_ub2()?;
    skip_sb2(reader)?;
    reader.skip(5)?;
    let _flags = reader.read_u8()?;
    skip_rowid(reader)?;
    let _os_error = reader.read_ub4()?;
    reader.skip(2)?;
    let _padding = reader.read_ub2()?;
    let _success_iters = reader.read_ub4()?;
    reader.read_bytes_with_length()?;

    let batch_error_count = reader.read_ub2()?;
    if batch_error_count > 0 {
        skip_packed_ub2_array(reader, batch_error_count)?;
    }

    let batch_offset_count = reader.read_ub4()?;
    if batch_offset_count > 0 {
        skip_packed_ub4_array(reader, batch_offset_count)?;
    }

    let batch_message_count = reader.read_ub2()?;
    if batch_message_count > 0 {
        reader.skip(1)?;
        for _ in 0..batch_message_count {
            let chunk_len = reader.read_ub2()?;
            reader.skip(usize::from(chunk_len))?;
            reader.skip(2)?;
        }
    }

    let error_number = reader.read_ub4()?;
    let row_count = reader.read_ub8()?;
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_20_1
        || (reader.remaining() > 2 && reader.peek_u8()? == 0)
    {
        let _sql_type = reader.read_ub4()?;
        let _server_checksum = reader.read_ub4()?;
    }
    let message = if error_number != 0 {
        reader
            .read_bytes()?
            .map(|bytes| String::from_utf8_lossy(&bytes).trim().to_string())
            .unwrap_or_else(|| format!("ORA-{error_number:05}"))
    } else {
        String::new()
    };

    Ok(ServerErrorInfo {
        number: error_number,
        message,
        cursor_id,
        row_count,
    })
}

fn skip_sb2(reader: &mut TtcReader<'_>) -> Result<()> {
    let len = reader.read_u8()?;
    reader.skip(usize::from(len & 0x7f))
}

fn skip_rowid(reader: &mut TtcReader<'_>) -> Result<()> {
    let _rba = reader.read_ub4()?;
    let _partition_id = reader.read_ub2()?;
    reader.skip(1)?;
    let _block_num = reader.read_ub4()?;
    let _slot_num = reader.read_ub2()?;
    Ok(())
}

fn skip_packed_ub2_array(reader: &mut TtcReader<'_>, count: u16) -> Result<()> {
    let first_byte = reader.read_u8()?;
    for _ in 0..count {
        if first_byte == crate::wire::TNS_LONG_LENGTH_INDICATOR {
            let _chunk_len = reader.read_ub4()?;
        }
        let _value = reader.read_ub2()?;
    }
    if first_byte == crate::wire::TNS_LONG_LENGTH_INDICATOR {
        reader.skip(1)?;
    }
    Ok(())
}

fn skip_packed_ub4_array(reader: &mut TtcReader<'_>, count: u32) -> Result<()> {
    let first_byte = reader.read_u8()?;
    for _ in 0..count {
        if first_byte == crate::wire::TNS_LONG_LENGTH_INDICATOR {
            let _chunk_len = reader.read_ub4()?;
        }
        let _value = reader.read_ub4()?;
    }
    if first_byte == crate::wire::TNS_LONG_LENGTH_INDICATOR {
        reader.skip(1)?;
    }
    Ok(())
}

fn parse_return_parameters(reader: &mut TtcReader<'_>) -> Result<AuthResponse> {
    let num_params = reader.read_ub2()?;
    let mut response = AuthResponse::default();
    for _ in 0..num_params {
        let key = reader
            .read_string_with_length()?
            .ok_or(ProtocolError::TtcDecode("missing auth response key"))?;
        let value = reader.read_string_with_length()?.unwrap_or_default();
        if key == "AUTH_VFR_DATA" {
            response.verifier_type = Some(reader.read_ub4()?);
        } else {
            let _flags = reader.read_ub4()?;
        }
        response.session_data.insert(key, value);
    }
    Ok(response)
}

fn skip_server_side_piggyback(reader: &mut TtcReader<'_>) -> Result<()> {
    let opcode = reader.read_u8()?;
    match opcode {
        TNS_SERVER_PIGGYBACK_LTXID => {
            let _ltxid = reader.read_bytes_with_length()?;
        }
        TNS_SERVER_PIGGYBACK_QUERY_CACHE_INVALIDATION | TNS_SERVER_PIGGYBACK_TRACE_EVENT => {}
        TNS_SERVER_PIGGYBACK_OS_PID_MTS => {
            let _pid = reader.read_ub2()?;
            let _mts = reader.read_bytes()?;
        }
        TNS_SERVER_PIGGYBACK_SYNC => {
            let _num_dtys = reader.read_ub2()?;
            reader.skip(1)?;
            let num_elements = reader.read_ub2()?;
            reader.skip(1)?;
            skip_keyword_value_pairs(reader, num_elements)?;
            let _flags = reader.read_ub4()?;
        }
        TNS_SERVER_PIGGYBACK_EXT_SYNC => {
            let _num_dtys = reader.read_ub2()?;
            reader.skip(1)?;
        }
        TNS_SERVER_PIGGYBACK_AC_REPLAY_CONTEXT => {
            let _num_dtys = reader.read_ub2()?;
            reader.skip(1)?;
            let _flags = reader.read_ub4()?;
            let _error_code = reader.read_ub4()?;
            reader.skip(1)?;
            let _replay_context = reader.read_bytes_with_length()?;
        }
        TNS_SERVER_PIGGYBACK_SESS_RET => {
            let _num_dtys = reader.read_ub2()?;
            reader.skip(1)?;
            let num_elements = reader.read_ub2()?;
            if num_elements > 0 {
                reader.skip(1)?;
                for _ in 0..num_elements {
                    if reader.read_ub2()? > 0 {
                        let _key = reader.read_bytes()?;
                    }
                    if reader.read_ub2()? > 0 {
                        let _value = reader.read_bytes()?;
                    }
                    let _flags = reader.read_ub2()?;
                }
            }
            let _flags = reader.read_ub4()?;
            let _session_id = reader.read_ub4()?;
            let _serial_num = reader.read_ub2()?;
        }
        TNS_SERVER_PIGGYBACK_SESS_SIGNATURE => {
            let _num_dtys = reader.read_ub2()?;
            reader.skip(1)?;
            let _signature_flags = reader.read_ub8()?;
            let _client_signature = reader.read_ub8()?;
            let _server_signature = reader.read_ub8()?;
        }
        _ => return Err(ProtocolError::UnsupportedFeature("server-side piggyback")),
    }
    Ok(())
}

fn skip_keyword_value_pairs(reader: &mut TtcReader<'_>, num_pairs: u16) -> Result<()> {
    for _ in 0..num_pairs {
        if reader.read_ub2()? > 0 {
            let _text_value = reader.read_bytes()?;
        }
        if reader.read_ub2()? > 0 {
            let _binary_value = reader.read_bytes()?;
        }
        let _keyword_num = reader.read_ub2()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_payload_matches_reference_shape() {
        let payload = build_connect_packet_payload(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=localhost)(PORT=1522))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))",
            8192,
        )
        .expect("connect payload should encode");
        assert_eq!(&payload[..8], &[0x01, 0x3f, 0x01, 0x2c, 0, 1, 0x20, 0]);
    }

    #[test]
    fn auth_phase_one_contains_identity_keys() {
        let payload = build_fast_auth_phase_one_payload("u", "p", "m", "o", "t", 42)
            .expect("auth packet should encode");
        let text = String::from_utf8_lossy(&payload);
        assert!(text.contains("AUTH_PROGRAM_NM"));
        assert!(text.contains("AUTH_MACHINE"));
        assert!(text.contains("AUTH_SID"));
    }

    #[test]
    fn query_response_decodes_prefetched_text_row_with_no_data_eof() {
        let payload = Vec::from_hex(concat!(
            "101710740fb986350b6010fbcb6e06a74ed0787e060a110328014001018201800000",
            "014000000000020369010140023ffe010501050556414c554500000000000000000000",
            "010707787e060a110b1000021fe8010a010a00062201010001020000000708414c33",
            "32555446380801060323a4d500010100000000000004010102013b010102057b0000",
            "01010003000000000000000000000000030001010000000002057b0101010300194f",
            "52412d30313430333a206e6f206461746120666f756e640a1d",
        ))
        .expect("fixture response should be valid hex");

        let parsed = parse_query_response(&payload, ClientCapabilities::default())
            .expect("accepted execute response should decode");

        assert_eq!(parsed.columns.len(), 1);
        assert_eq!(parsed.columns[0].name, "VALUE");
        assert_eq!(
            parsed.rows,
            vec![vec![Some(QueryValue::Text("AL32UTF8".into()))]]
        );
        assert!(!parsed.more_rows);
    }

    #[test]
    fn fetch_response_decodes_rows_with_previous_cursor_metadata() {
        let payload = Vec::from_hex("06020101000205dc0001010101000702c1041d")
            .expect("fixture response should be valid hex");
        let columns = vec![number_column("INTCOL"), number_column("NUMBERCOL")];
        let previous_row = vec![
            Some(QueryValue::Number {
                text: "2".into(),
                is_integer: true,
            }),
            Some(QueryValue::Number {
                text: "0.5".into(),
                is_integer: false,
            }),
        ];

        let parsed = parse_query_response_with_context(
            &payload,
            ClientCapabilities::default(),
            &columns,
            Some(&previous_row),
        )
        .expect("fetch response should decode using cached cursor metadata");

        assert_eq!(parsed.columns, columns);
        assert_eq!(
            parsed.rows,
            vec![vec![
                Some(QueryValue::Number {
                    text: "3".into(),
                    is_integer: true,
                }),
                previous_row[1].clone(),
            ]]
        );
    }

    #[test]
    fn fetch_response_decodes_mid_row_oracle_error() {
        let payload = Vec::from_hex(concat!(
            "150101010703c20401010205100205db0205c400000106018f030000000000",
            "0301214d0118000293b60201c60000080000000000000205db0205c40103",
            "00244f52412d30313437363a2064697669736f7220697320657175616c20",
            "746f207a65726f0a1d",
        ))
        .expect("fixture response should be valid hex");
        let columns = vec![number_column("INTCOL"), number_column("NUMBERCOL")];
        let previous_row = vec![
            Some(QueryValue::Number {
                text: "1499".into(),
                is_integer: true,
            }),
            Some(QueryValue::Number {
                text: "0.5".into(),
                is_integer: false,
            }),
        ];

        let err = parse_query_response_with_context(
            &payload,
            ClientCapabilities::default(),
            &columns,
            Some(&previous_row),
        )
        .expect_err("mid-row error info should surface as a server error");

        assert_eq!(
            err.to_string(),
            "server returned Oracle error: ORA-01476: divisor is equal to zero"
        );
    }

    fn number_column(name: &str) -> ColumnMetadata {
        ColumnMetadata {
            name: name.into(),
            ora_type_num: ORA_TYPE_NUM_NUMBER,
            csfrm: CS_FORM_IMPLICIT,
            precision: 0,
            scale: 0,
            buffer_size: ORA_TYPE_SIZE_NUMBER,
            max_size: ORA_TYPE_SIZE_NUMBER,
            nulls_allowed: true,
            is_json: false,
            is_oson: false,
        }
    }
}

fn has_u8_flag(flags: u8, mask: u8) -> bool {
    flags & mask > 0
}

fn has_u32_flag(flags: u32, mask: u32) -> bool {
    flags & mask > 0
}
