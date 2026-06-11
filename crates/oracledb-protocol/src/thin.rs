#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use crate::sql::statement_is_plsql;
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
pub const TNS_MSG_TYPE_IO_VECTOR: u8 = 11;
pub const TNS_MSG_TYPE_LOB_DATA: u8 = 14;
pub const TNS_MSG_TYPE_FLUSH_OUT_BINDS: u8 = 19;
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
pub const TNS_FUNC_LOB_OP: u8 = 96;
pub const TNS_FUNC_PING: u8 = 147;
pub const TNS_FUNC_ROLLBACK: u8 = 15;

pub const TNS_AUTH_MODE_LOGON: u32 = 0x0000_0001;
pub const TNS_AUTH_MODE_WITH_PASSWORD: u32 = 0x0000_0100;

pub const TNS_VERIFIER_TYPE_11G_1: u32 = 0xb152;
pub const TNS_VERIFIER_TYPE_11G_2: u32 = 0x1b25;
pub const TNS_VERIFIER_TYPE_12C: u32 = 0x4815;

pub const ORA_TYPE_NUM_VARCHAR: u8 = 1;
pub const ORA_TYPE_NUM_NUMBER: u8 = 2;
pub const ORA_TYPE_NUM_BINARY_INTEGER: u8 = 3;
pub const ORA_TYPE_NUM_LONG: u8 = 8;
pub const ORA_TYPE_NUM_ROWID: u8 = 11;
pub const ORA_TYPE_NUM_DATE: u8 = 12;
pub const ORA_TYPE_NUM_RAW: u8 = 23;
pub const ORA_TYPE_NUM_BINARY_DOUBLE: u8 = 101;
pub const ORA_TYPE_NUM_CURSOR: u8 = 102;
pub const ORA_TYPE_NUM_LONG_RAW: u8 = 24;
pub const ORA_TYPE_NUM_CHAR: u8 = 96;
pub const ORA_TYPE_NUM_CLOB: u8 = 112;
pub const ORA_TYPE_NUM_BLOB: u8 = 113;
pub const ORA_TYPE_NUM_BFILE: u8 = 114;
pub const ORA_TYPE_NUM_OBJECT: u8 = 109;
pub const ORA_TYPE_NUM_TIMESTAMP: u8 = 180;
pub const ORA_TYPE_NUM_TIMESTAMP_TZ: u8 = 181;
pub const ORA_TYPE_NUM_UROWID: u8 = 208;
pub const ORA_TYPE_NUM_TIMESTAMP_LTZ: u8 = 231;
pub const TNS_OBJ_TOP_LEVEL: u32 = 0x01;

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
const TNS_CCAP_FIELD_VERSION_23_1_EXT_1: u8 = 18;
const TNS_CCAP_FIELD_VERSION_23_1_EXT_3: u8 = 20;
const TNS_CCAP_FIELD_VERSION_23_4: u8 = 24;
const TNS_RCAP_TTC: usize = 6;
const TNS_RCAP_TTC_32K: u8 = 0x04;
const TNS_EXEC_OPTION_PARSE: u32 = 0x01;
const TNS_EXEC_OPTION_BIND: u32 = 0x08;
const TNS_EXEC_OPTION_DEFINE: u32 = 0x10;
const TNS_EXEC_OPTION_EXECUTE: u32 = 0x20;
const TNS_EXEC_OPTION_FETCH: u32 = 0x40;
const TNS_EXEC_OPTION_PLSQL_BIND: u32 = 0x400;
const TNS_EXEC_OPTION_NOT_PLSQL: u32 = 0x8000;
const TNS_DURATION_SESSION: u32 = 10;
const TNS_LOB_OP_READ: u32 = 0x0002;
const TNS_LOB_OP_WRITE: u32 = 0x0040;
const TNS_LOB_OP_CREATE_TEMP: u32 = 0x0110;
const TNS_LOB_PREFETCH_FLAG: u64 = 0x0200_0000;
const TNS_EXEC_FLAGS_IMPLICIT_RESULTSET: u32 = 0x8000;
const TNS_BIND_USE_INDICATORS: u8 = 0x01;
const TNS_BIND_ARRAY: u8 = 0x40;
const TNS_BIND_DIR_INPUT: u8 = 32;
const TNS_CHARSET_UTF8: u16 = 873;
const TNS_MAX_LONG_LENGTH: u32 = 0x7fff_ffff;
const TNS_ERR_NO_DATA_FOUND: u32 = 1403;
const TNS_UDS_FLAGS_IS_JSON: u32 = 0x01;
const TNS_UDS_FLAGS_IS_OSON: u32 = 0x02;
const ORA_TYPE_SIZE_BINARY_DOUBLE: u32 = 8;
const ORA_TYPE_SIZE_NUMBER: u32 = 22;
const ORA_TYPE_SIZE_DATE: u32 = 7;
const ORA_TYPE_SIZE_ROWID: u32 = 18;
const ORA_TYPE_SIZE_TIMESTAMP: u32 = 11;
const ORA_TYPE_SIZE_TIMESTAMP_TZ: u32 = 13;
const TNS_BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const TNS_HAS_REGION_ID: u8 = 0x80;
const TZ_HOUR_OFFSET: u8 = 20;
const TZ_MINUTE_OFFSET: u8 = 60;
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
    pub object_schema: Option<String>,
    pub object_type_name: Option<String>,
    pub is_array: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BindTypeInfo {
    pub ora_type_num: u8,
    pub csfrm: u8,
    pub buffer_size: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryValue {
    Text(String),
    Raw(Vec<u8>),
    Rowid(String),
    BinaryDouble(String),
    Number {
        text: String,
        is_integer: bool,
    },
    Cursor {
        columns: Vec<ColumnMetadata>,
        cursor_id: u32,
    },
    DateTime {
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
    },
    Object {
        schema: Option<String>,
        type_name: Option<String>,
        packed_data: Vec<u8>,
    },
    Lob {
        ora_type_num: u8,
        csfrm: u8,
        locator: Vec<u8>,
        size: u64,
        chunk_size: u32,
    },
    Array(Vec<Option<QueryValue>>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum BindValue {
    Null,
    TypedNull {
        ora_type_num: u8,
        csfrm: u8,
        buffer_size: u32,
    },
    Output {
        ora_type_num: u8,
        csfrm: u8,
        buffer_size: u32,
    },
    ReturnOutput {
        ora_type_num: u8,
        csfrm: u8,
        buffer_size: u32,
    },
    ObjectOutput {
        schema: String,
        type_name: String,
        oid: Vec<u8>,
        version: u32,
        buffer_size: u32,
        is_return: bool,
    },
    Text(String),
    Raw(Vec<u8>),
    Lob {
        ora_type_num: u8,
        csfrm: u8,
        locator: Vec<u8>,
    },
    Number(String),
    BinaryInteger(String),
    BinaryDouble(f64),
    DateTime {
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
    },
    Timestamp {
        ora_type_num: u8,
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
    },
    Array {
        ora_type_num: u8,
        csfrm: u8,
        buffer_size: u32,
        max_elements: u32,
        values: Vec<Option<BindValue>>,
    },
    Cursor {
        cursor_id: u32,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<ColumnMetadata>,
    pub rows: Vec<Vec<Option<QueryValue>>>,
    pub out_values: Vec<(usize, Option<QueryValue>)>,
    pub return_values: Vec<(usize, Vec<Option<QueryValue>>)>,
    pub cursor_id: u32,
    pub row_count: u64,
    pub more_rows: bool,
    pub compilation_error_warning: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LobReadResult {
    pub data: Option<Vec<u8>>,
    pub locator: Vec<u8>,
    pub amount: u64,
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
    let has_ref_cursor_output = bind_rows.iter().any(|row| {
        row.iter().any(|value| {
            matches!(
                value,
                BindValue::Output {
                    ora_type_num: ORA_TYPE_NUM_CURSOR,
                    ..
                }
            )
        })
    });
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_EXECUTE, seq_num);
    writer.write_ub8(0);

    let is_plsql = statement_is_plsql(sql);
    let mut options = TNS_EXEC_OPTION_PARSE | TNS_EXEC_OPTION_EXECUTE;
    if is_query {
        options |= TNS_EXEC_OPTION_FETCH;
    }
    if bind_count > 0 {
        options |= TNS_EXEC_OPTION_BIND;
    }
    if is_plsql {
        if bind_count > 0 {
            options |= TNS_EXEC_OPTION_PLSQL_BIND;
        }
    } else {
        options |= TNS_EXEC_OPTION_NOT_PLSQL;
    }
    let num_iters = if is_query { prefetch_rows } else { 1 };
    let exec_count = if is_query { 0 } else { bind_row_count.max(1) };
    let query_flag = u32::from(is_query);
    let exec_flags = if is_query || has_ref_cursor_output {
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
        write_bind_params(&mut writer, bind_rows, is_plsql)?;
    }
    Ok(writer.into_bytes())
}

fn write_bind_params(
    writer: &mut TtcWriter,
    bind_rows: &[Vec<BindValue>],
    is_plsql: bool,
) -> Result<()> {
    let Some(first_row) = bind_rows.first() else {
        return Ok(());
    };
    let mut bind_metadata = Vec::with_capacity(first_row.len());
    for index in 0..first_row.len() {
        bind_metadata.push(write_bind_metadata_for_rows(writer, bind_rows, index)?);
    }
    for row in bind_rows {
        if !is_plsql && row.iter().all(BindValue::is_output_only) {
            continue;
        }
        writer.write_u8(TNS_MSG_TYPE_ROW_DATA);
        for (index, value) in row.iter().enumerate() {
            if !is_plsql && value.is_output_only() {
                continue;
            }
            let (_ora_type_num, csfrm) = bind_metadata
                .get(index)
                .copied()
                .unwrap_or((ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT));
            write_bind_value(writer, value, csfrm)?;
        }
    }
    Ok(())
}

fn write_bind_metadata_for_rows(
    writer: &mut TtcWriter,
    bind_rows: &[Vec<BindValue>],
    index: usize,
) -> Result<(u8, u8)> {
    let Some(first_row) = bind_rows.first() else {
        return Ok((ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT));
    };
    let Some(first_value) = first_row.get(index) else {
        return Ok((ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT));
    };
    let (ora_type_num, csfrm, mut buffer_size) = bind_metadata(first_value);
    for row in bind_rows.iter().skip(1) {
        let Some(value) = row.get(index) else {
            continue;
        };
        let (row_ora_type_num, row_csfrm, row_buffer_size) = bind_metadata(value);
        if row_ora_type_num == ora_type_num && row_csfrm == csfrm {
            buffer_size = buffer_size.max(row_buffer_size);
        }
    }
    write_bind_metadata_with_type(writer, first_value, ora_type_num, csfrm, buffer_size)?;
    Ok((ora_type_num, csfrm))
}

impl BindValue {
    fn is_output_only(&self) -> bool {
        matches!(self, BindValue::Output { .. })
            || matches!(self, BindValue::ReturnOutput { .. })
            || matches!(self, BindValue::ObjectOutput { .. })
            || matches!(self, BindValue::Array { values, .. } if values.is_empty())
    }

    fn is_return_output(&self) -> bool {
        matches!(self, BindValue::ReturnOutput { .. })
            || matches!(
                self,
                BindValue::ObjectOutput {
                    is_return: true,
                    ..
                }
            )
    }
}

fn write_bind_metadata_with_type(
    writer: &mut TtcWriter,
    value: &BindValue,
    ora_type_num: u8,
    csfrm: u8,
    buffer_size: u32,
) -> Result<()> {
    let (flags, max_elements) = match value {
        BindValue::Array { max_elements, .. } => {
            (TNS_BIND_USE_INDICATORS | TNS_BIND_ARRAY, *max_elements)
        }
        _ => (TNS_BIND_USE_INDICATORS, 0),
    };
    writer.write_u8(ora_type_num);
    writer.write_u8(flags);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(buffer_size);
    writer.write_ub4(max_elements);
    writer.write_ub8(0);
    if let BindValue::ObjectOutput { oid, version, .. } = value {
        writer.write_bytes_with_two_lengths(Some(oid))?;
        writer.write_ub4(*version);
    } else {
        writer.write_ub4(0);
        writer.write_ub2(0);
    }
    if csfrm != 0 {
        writer.write_ub2(TNS_CHARSET_UTF8);
    } else {
        writer.write_ub2(0);
    }
    writer.write_u8(csfrm);
    writer.write_ub4(0);
    writer.write_ub4(0);
    Ok(())
}

pub fn bind_value_type_info(value: &BindValue) -> Option<BindTypeInfo> {
    let (ora_type_num, csfrm, buffer_size) = match value {
        BindValue::Null => return None,
        BindValue::TypedNull {
            ora_type_num,
            csfrm,
            buffer_size,
        }
        | BindValue::Output {
            ora_type_num,
            csfrm,
            buffer_size,
        }
        | BindValue::ReturnOutput {
            ora_type_num,
            csfrm,
            buffer_size,
        } => (*ora_type_num, *csfrm, (*buffer_size).max(1)),
        BindValue::ObjectOutput { buffer_size, .. } => {
            (ORA_TYPE_NUM_OBJECT, 0, (*buffer_size).max(1))
        }
        BindValue::Text(value) => (
            ORA_TYPE_NUM_VARCHAR,
            CS_FORM_IMPLICIT,
            u32::try_from(value.chars().count())
                .unwrap_or(u32::MAX)
                .saturating_mul(4)
                .max(1),
        ),
        BindValue::Raw(value) => (
            ORA_TYPE_NUM_RAW,
            0,
            u32::try_from(value.len()).unwrap_or(u32::MAX).max(1),
        ),
        BindValue::Lob {
            ora_type_num,
            csfrm,
            ..
        } => (*ora_type_num, *csfrm, 1),
        BindValue::Number(_) => (ORA_TYPE_NUM_NUMBER, 0, ORA_TYPE_SIZE_NUMBER),
        BindValue::BinaryInteger(_) => (ORA_TYPE_NUM_BINARY_INTEGER, 0, ORA_TYPE_SIZE_NUMBER),
        BindValue::BinaryDouble(_) => (ORA_TYPE_NUM_BINARY_DOUBLE, 0, ORA_TYPE_SIZE_BINARY_DOUBLE),
        BindValue::DateTime { .. } => (ORA_TYPE_NUM_DATE, 0, ORA_TYPE_SIZE_DATE),
        BindValue::Timestamp { ora_type_num, .. } => (
            *ora_type_num,
            0,
            if *ora_type_num == ORA_TYPE_NUM_TIMESTAMP_TZ {
                ORA_TYPE_SIZE_TIMESTAMP_TZ
            } else {
                ORA_TYPE_SIZE_TIMESTAMP
            },
        ),
        BindValue::Array {
            ora_type_num,
            csfrm,
            buffer_size,
            ..
        } => (*ora_type_num, *csfrm, (*buffer_size).max(1)),
        BindValue::Cursor { .. } => (ORA_TYPE_NUM_CURSOR, 0, 4),
    };
    Some(BindTypeInfo {
        ora_type_num,
        csfrm,
        buffer_size,
    })
}

pub fn define_metadata_from_bind(source: &ColumnMetadata, value: &BindValue) -> ColumnMetadata {
    let Some(mut info) = bind_value_type_info(value) else {
        return source.clone();
    };
    if source.ora_type_num == ORA_TYPE_NUM_CLOB
        && matches!(
            info.ora_type_num,
            ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_VARCHAR
        )
    {
        info.ora_type_num = ORA_TYPE_NUM_LONG;
        if source.csfrm != 0 {
            info.csfrm = source.csfrm;
        }
    }
    let mut metadata = source.clone();
    metadata.ora_type_num = info.ora_type_num;
    metadata.csfrm = info.csfrm;
    if info.ora_type_num == ORA_TYPE_NUM_LONG {
        metadata.buffer_size = TNS_MAX_LONG_LENGTH;
        metadata.max_size = 0;
    } else {
        metadata.buffer_size = info.buffer_size.max(1);
        metadata.max_size = info.buffer_size.max(1);
    }
    metadata
}

pub fn output_bind(value: BindValue) -> BindValue {
    match value {
        BindValue::ObjectOutput {
            schema,
            type_name,
            oid,
            version,
            buffer_size,
            ..
        } => BindValue::ObjectOutput {
            schema,
            type_name,
            oid,
            version,
            buffer_size: buffer_size.max(1),
            is_return: false,
        },
        value => {
            let info = bind_value_type_info(&value).unwrap_or(BindTypeInfo {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_IMPLICIT,
                buffer_size: 1,
            });
            BindValue::Output {
                ora_type_num: info.ora_type_num,
                csfrm: info.csfrm,
                buffer_size: info.buffer_size,
            }
        }
    }
}

pub fn returning_output_bind(value: BindValue) -> BindValue {
    match value {
        BindValue::ObjectOutput {
            schema,
            type_name,
            oid,
            version,
            buffer_size,
            ..
        } => BindValue::ObjectOutput {
            schema,
            type_name,
            oid,
            version,
            buffer_size: buffer_size.max(1),
            is_return: true,
        },
        value => {
            let info = bind_value_type_info(&value).unwrap_or(BindTypeInfo {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_IMPLICIT,
                buffer_size: 1,
            });
            BindValue::ReturnOutput {
                ora_type_num: info.ora_type_num,
                csfrm: info.csfrm,
                buffer_size: info.buffer_size,
            }
        }
    }
}

pub fn cursor_bind_template() -> BindValue {
    BindValue::TypedNull {
        ora_type_num: ORA_TYPE_NUM_CURSOR,
        csfrm: 0,
        buffer_size: 4,
    }
}

pub fn is_cursor_bind_template(value: &BindValue) -> bool {
    matches!(
        value,
        BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_CURSOR,
            ..
        }
    )
}

pub fn public_dbtype_name_from_type_name(type_name: &str) -> &'static str {
    match type_name {
        "NUMBER" | "DB_TYPE_NUMBER" | "int" | "float" => "DB_TYPE_NUMBER",
        "NATIVE_INT" | "DB_TYPE_BINARY_INTEGER" => "DB_TYPE_BINARY_INTEGER",
        "NATIVE_FLOAT" | "DB_TYPE_BINARY_DOUBLE" => "DB_TYPE_BINARY_DOUBLE",
        "STRING" | "DB_TYPE_VARCHAR" | "str" => "DB_TYPE_VARCHAR",
        "DB_TYPE_CHAR" => "DB_TYPE_CHAR",
        "DB_TYPE_NCHAR" => "DB_TYPE_NCHAR",
        "DB_TYPE_NVARCHAR" => "DB_TYPE_NVARCHAR",
        "DB_TYPE_CLOB" | "CLOB" => "DB_TYPE_CLOB",
        "DB_TYPE_NCLOB" | "NCLOB" => "DB_TYPE_NCLOB",
        "DB_TYPE_LONG" | "LONG" | "LONG_STRING" => "DB_TYPE_LONG",
        "DB_TYPE_LONG_NVARCHAR" | "LONG NVARCHAR" => "DB_TYPE_LONG_NVARCHAR",
        "DB_TYPE_LONG_RAW" | "LONG RAW" | "LONG_BINARY" => "DB_TYPE_LONG_RAW",
        "DB_TYPE_RAW" | "bytes" => "DB_TYPE_RAW",
        "ROWID" | "DB_TYPE_ROWID" => "DB_TYPE_ROWID",
        "DB_TYPE_UROWID" => "DB_TYPE_UROWID",
        "DATETIME" | "DB_TYPE_DATE" | "date" | "datetime" => "DB_TYPE_DATE",
        "DB_TYPE_TIMESTAMP" | "TIMESTAMP" => "DB_TYPE_TIMESTAMP",
        "DB_TYPE_TIMESTAMP_LTZ" | "TIMESTAMP WITH LOCAL TIME ZONE" => "DB_TYPE_TIMESTAMP_LTZ",
        "DB_TYPE_TIMESTAMP_TZ" | "TIMESTAMP WITH TIME ZONE" => "DB_TYPE_TIMESTAMP_TZ",
        "DB_TYPE_CURSOR" | "CURSOR" => "DB_TYPE_CURSOR",
        _ => "DB_TYPE_VARCHAR",
    }
}

pub fn public_dbtype_name_from_bind(value: &BindValue) -> &'static str {
    match value {
        BindValue::TypedNull {
            ora_type_num,
            csfrm,
            ..
        }
        | BindValue::Output {
            ora_type_num,
            csfrm,
            ..
        }
        | BindValue::ReturnOutput {
            ora_type_num,
            csfrm,
            ..
        }
        | BindValue::Array {
            ora_type_num,
            csfrm,
            ..
        } => public_dbtype_name_from_type_info(*ora_type_num, *csfrm),
        BindValue::ObjectOutput { .. } => "DB_TYPE_OBJECT",
        BindValue::Text(_) => "DB_TYPE_VARCHAR",
        BindValue::Raw(_) => "DB_TYPE_RAW",
        BindValue::Lob {
            ora_type_num,
            csfrm,
            ..
        } => match (*ora_type_num, *csfrm) {
            (ORA_TYPE_NUM_BLOB, _) => "DB_TYPE_BLOB",
            (ORA_TYPE_NUM_CLOB, CS_FORM_NCHAR) => "DB_TYPE_NCLOB",
            (ORA_TYPE_NUM_CLOB, _) => "DB_TYPE_CLOB",
            _ => "DB_TYPE_CLOB",
        },
        BindValue::Number(_) => "DB_TYPE_NUMBER",
        BindValue::BinaryInteger(_) => "DB_TYPE_BINARY_INTEGER",
        BindValue::BinaryDouble(_) => "DB_TYPE_BINARY_DOUBLE",
        BindValue::DateTime { .. } => "DB_TYPE_DATE",
        BindValue::Timestamp { ora_type_num, .. } => match *ora_type_num {
            ORA_TYPE_NUM_TIMESTAMP_LTZ => "DB_TYPE_TIMESTAMP_LTZ",
            ORA_TYPE_NUM_TIMESTAMP_TZ => "DB_TYPE_TIMESTAMP_TZ",
            _ => "DB_TYPE_TIMESTAMP",
        },
        BindValue::Cursor { .. } => "DB_TYPE_CURSOR",
        BindValue::Null => "DB_TYPE_VARCHAR",
    }
}

pub fn bind_template_from_type_name(type_name: &str, size: u32) -> BindValue {
    let text_buffer_size = if size == 0 { 4000 } else { size.max(1) };
    let nchar_buffer_size = text_buffer_size.saturating_mul(4);
    match type_name {
        "NUMBER" | "DB_TYPE_NUMBER" | "int" | "float" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_NUMBER,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_NUMBER,
        },
        "NATIVE_INT" | "DB_TYPE_BINARY_INTEGER" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_BINARY_INTEGER,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_NUMBER,
        },
        "NATIVE_FLOAT" | "DB_TYPE_BINARY_DOUBLE" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_BINARY_DOUBLE,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_BINARY_DOUBLE,
        },
        "STRING" | "DB_TYPE_VARCHAR" | "DB_TYPE_CHAR" | "str" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_VARCHAR,
            csfrm: CS_FORM_IMPLICIT,
            buffer_size: text_buffer_size,
        },
        "DB_TYPE_NCHAR" | "DB_TYPE_NVARCHAR" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_VARCHAR,
            csfrm: CS_FORM_NCHAR,
            buffer_size: nchar_buffer_size,
        },
        "DB_TYPE_CLOB" | "CLOB" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_LONG,
            csfrm: CS_FORM_IMPLICIT,
            buffer_size: TNS_MAX_LONG_LENGTH,
        },
        "DB_TYPE_NCLOB" | "NCLOB" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_LONG,
            csfrm: CS_FORM_NCHAR,
            buffer_size: TNS_MAX_LONG_LENGTH,
        },
        "DB_TYPE_LONG" | "LONG" | "LONG_STRING" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_LONG,
            csfrm: CS_FORM_IMPLICIT,
            buffer_size: TNS_MAX_LONG_LENGTH,
        },
        "DB_TYPE_LONG_NVARCHAR" | "LONG NVARCHAR" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_LONG,
            csfrm: CS_FORM_NCHAR,
            buffer_size: TNS_MAX_LONG_LENGTH,
        },
        "DB_TYPE_LONG_RAW" | "LONG RAW" | "LONG_BINARY" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_LONG_RAW,
            csfrm: 0,
            buffer_size: TNS_MAX_LONG_LENGTH,
        },
        "DB_TYPE_RAW" | "bytes" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_RAW,
            csfrm: 0,
            buffer_size: size.max(1).max(4000),
        },
        "ROWID" | "DB_TYPE_ROWID" | "DB_TYPE_UROWID" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_VARCHAR,
            csfrm: CS_FORM_IMPLICIT,
            buffer_size: 5267,
        },
        "DATETIME" | "DB_TYPE_DATE" | "date" | "datetime" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_DATE,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_DATE,
        },
        "DB_TYPE_TIMESTAMP" | "TIMESTAMP" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_TIMESTAMP,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_TIMESTAMP,
        },
        "DB_TYPE_TIMESTAMP_LTZ" | "TIMESTAMP WITH LOCAL TIME ZONE" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_TIMESTAMP_LTZ,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_TIMESTAMP,
        },
        "DB_TYPE_TIMESTAMP_TZ" | "TIMESTAMP WITH TIME ZONE" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_TIMESTAMP_TZ,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_TIMESTAMP_TZ,
        },
        "DB_TYPE_CURSOR" | "CURSOR" => cursor_bind_template(),
        _ => BindValue::Null,
    }
}

pub fn dbobject_element_bind_type_info(dbtype_name: &str, max_size: u32) -> BindTypeInfo {
    let buffer_size = max_size.max(1);
    let (ora_type_num, csfrm, buffer_size) = match dbtype_name {
        "DB_TYPE_NUMBER" => (ORA_TYPE_NUM_NUMBER, 0, ORA_TYPE_SIZE_NUMBER),
        "DB_TYPE_RAW" | "DB_TYPE_BLOB" => (ORA_TYPE_NUM_RAW, 0, buffer_size.max(4000)),
        "DB_TYPE_NCHAR" | "DB_TYPE_NVARCHAR" | "DB_TYPE_NCLOB" => {
            (ORA_TYPE_NUM_VARCHAR, CS_FORM_NCHAR, buffer_size.max(4000))
        }
        "DB_TYPE_DATE" => (ORA_TYPE_NUM_DATE, 0, ORA_TYPE_SIZE_DATE),
        "DB_TYPE_TIMESTAMP" => (ORA_TYPE_NUM_TIMESTAMP, 0, ORA_TYPE_SIZE_TIMESTAMP),
        "DB_TYPE_TIMESTAMP_LTZ" => (ORA_TYPE_NUM_TIMESTAMP_LTZ, 0, ORA_TYPE_SIZE_TIMESTAMP),
        "DB_TYPE_TIMESTAMP_TZ" => (ORA_TYPE_NUM_TIMESTAMP_TZ, 0, ORA_TYPE_SIZE_TIMESTAMP_TZ),
        _ => (
            ORA_TYPE_NUM_VARCHAR,
            CS_FORM_IMPLICIT,
            buffer_size.max(4000),
        ),
    };
    BindTypeInfo {
        ora_type_num,
        csfrm,
        buffer_size,
    }
}

fn public_dbtype_name_from_type_info(ora_type_num: u8, csfrm: u8) -> &'static str {
    match (ora_type_num, csfrm) {
        (ORA_TYPE_NUM_BINARY_DOUBLE, _) => "DB_TYPE_BINARY_DOUBLE",
        (ORA_TYPE_NUM_BINARY_INTEGER, _) => "DB_TYPE_BINARY_INTEGER",
        (ORA_TYPE_NUM_NUMBER, _) => "DB_TYPE_NUMBER",
        (ORA_TYPE_NUM_CHAR, CS_FORM_NCHAR) | (ORA_TYPE_NUM_VARCHAR, CS_FORM_NCHAR) => {
            "DB_TYPE_NVARCHAR"
        }
        (ORA_TYPE_NUM_CHAR, _) => "DB_TYPE_CHAR",
        (ORA_TYPE_NUM_VARCHAR, _) => "DB_TYPE_VARCHAR",
        (ORA_TYPE_NUM_LONG, CS_FORM_NCHAR) => "DB_TYPE_LONG_NVARCHAR",
        (ORA_TYPE_NUM_LONG, _) => "DB_TYPE_LONG",
        (ORA_TYPE_NUM_LONG_RAW, _) => "DB_TYPE_LONG_RAW",
        (ORA_TYPE_NUM_RAW, _) => "DB_TYPE_RAW",
        (ORA_TYPE_NUM_DATE, _) => "DB_TYPE_DATE",
        (ORA_TYPE_NUM_TIMESTAMP, _) => "DB_TYPE_TIMESTAMP",
        (ORA_TYPE_NUM_TIMESTAMP_LTZ, _) => "DB_TYPE_TIMESTAMP_LTZ",
        (ORA_TYPE_NUM_TIMESTAMP_TZ, _) => "DB_TYPE_TIMESTAMP_TZ",
        (ORA_TYPE_NUM_CURSOR, _) => "DB_TYPE_CURSOR",
        (ORA_TYPE_NUM_OBJECT, _) => "DB_TYPE_OBJECT",
        _ => "DB_TYPE_VARCHAR",
    }
}

fn bind_metadata(value: &BindValue) -> (u8, u8, u32) {
    bind_value_type_info(value)
        .map(|info| (info.ora_type_num, info.csfrm, info.buffer_size))
        .unwrap_or((ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 1))
}

fn write_bind_value(writer: &mut TtcWriter, value: &BindValue, csfrm: u8) -> Result<()> {
    match value {
        BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_CURSOR,
            ..
        } => {
            writer.write_u8(1);
            writer.write_u8(0);
            Ok(())
        }
        BindValue::Null | BindValue::TypedNull { .. } => {
            writer.write_u8(0);
            Ok(())
        }
        BindValue::Output { .. } | BindValue::ReturnOutput { .. } => {
            writer.write_u8(0);
            Ok(())
        }
        BindValue::ObjectOutput { .. } => {
            writer.write_ub4(0);
            writer.write_ub4(0);
            writer.write_ub4(0);
            writer.write_ub2(0);
            writer.write_ub4(0);
            writer.write_ub4(TNS_OBJ_TOP_LEVEL);
            Ok(())
        }
        BindValue::Text(value) => {
            let bytes = encode_text_value(value, csfrm);
            writer.write_bytes_with_length(&bytes)
        }
        BindValue::Raw(value) => writer.write_bytes_with_length(value),
        BindValue::Lob { locator, .. } => writer.write_bytes_with_two_lengths(Some(locator)),
        BindValue::Number(value) | BindValue::BinaryInteger(value) => {
            let bytes = encode_number_text(value)?;
            writer.write_bytes_with_length(&bytes)
        }
        BindValue::BinaryDouble(value) => {
            let bytes = encode_binary_double(*value);
            writer.write_bytes_with_length(&bytes)
        }
        BindValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
        } => {
            let bytes = encode_oracle_date(*year, *month, *day, *hour, *minute, *second)?;
            writer.write_bytes_with_length(&bytes)
        }
        BindValue::Timestamp {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            ora_type_num,
        } => {
            let bytes = if matches!(*ora_type_num, ORA_TYPE_NUM_TIMESTAMP_TZ) {
                encode_oracle_timestamp_tz(
                    *year,
                    *month,
                    *day,
                    *hour,
                    *minute,
                    *second,
                    *nanosecond,
                )?
            } else {
                encode_oracle_timestamp(*year, *month, *day, *hour, *minute, *second, *nanosecond)?
            };
            writer.write_bytes_with_length(&bytes)
        }
        BindValue::Array {
            values,
            csfrm: array_csfrm,
            ..
        } => {
            writer.write_ub4(u32::try_from(values.len()).map_err(|_| {
                ProtocolError::InvalidPacketLength {
                    length: values.len(),
                    minimum: 0,
                }
            })?);
            for value in values {
                match value {
                    Some(value) => write_bind_value(writer, value, *array_csfrm)?,
                    None => writer.write_u8(0),
                }
            }
            Ok(())
        }
        BindValue::Cursor { cursor_id } => {
            if *cursor_id == 0 {
                writer.write_u8(1);
                writer.write_u8(0);
            } else {
                writer.write_ub4(1);
                writer.write_ub4(*cursor_id);
            }
            Ok(())
        }
    }
}

fn encode_text_value(value: &str, csfrm: u8) -> Vec<u8> {
    if csfrm == CS_FORM_NCHAR {
        let mut bytes = Vec::with_capacity(value.len().saturating_mul(2));
        for unit in value.encode_utf16() {
            bytes.extend_from_slice(&unit.to_be_bytes());
        }
        bytes
    } else {
        value.as_bytes().to_vec()
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

pub fn build_define_fetch_payload_with_seq(
    cursor_id: u32,
    arraysize: u32,
    seq_num: u8,
    define_columns: &[ColumnMetadata],
) -> Result<Vec<u8>> {
    let define_count =
        u32::try_from(define_columns.len()).map_err(|_| ProtocolError::InvalidPacketLength {
            length: define_columns.len(),
            minimum: 0,
        })?;
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_EXECUTE, seq_num);
    writer.write_ub8(0);
    writer.write_ub4(TNS_EXEC_OPTION_DEFINE | TNS_EXEC_OPTION_NOT_PLSQL);
    writer.write_ub4(cursor_id);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_u8(1);
    writer.write_ub4(13);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_ub4(arraysize);
    writer.write_ub4(TNS_MAX_LONG_LENGTH);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(1);
    writer.write_ub4(define_count);
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
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(arraysize);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(1);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    for metadata in define_columns {
        write_define_column_metadata(&mut writer, metadata);
    }
    Ok(writer.into_bytes())
}

fn write_define_column_metadata(writer: &mut TtcWriter, metadata: &ColumnMetadata) {
    writer.write_u8(metadata.ora_type_num);
    writer.write_u8(TNS_BIND_USE_INDICATORS);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(metadata.buffer_size.max(1));
    writer.write_ub4(0);
    let cont_flags = if matches!(metadata.ora_type_num, ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB) {
        TNS_LOB_PREFETCH_FLAG
    } else {
        0
    };
    writer.write_ub8(cont_flags);
    writer.write_ub4(0);
    writer.write_ub2(0);
    if metadata.csfrm != 0 {
        writer.write_ub2(TNS_CHARSET_UTF8);
    } else {
        writer.write_ub2(0);
    }
    writer.write_u8(metadata.csfrm);
    writer.write_ub4(0);
    writer.write_ub4(0);
}

pub fn build_lob_read_payload_with_seq(
    locator: &[u8],
    offset: u64,
    amount: u64,
    seq_num: u8,
    ttc_field_version: u8,
) -> Result<Vec<u8>> {
    let locator_len =
        u32::try_from(locator.len()).map_err(|_| ProtocolError::InvalidPacketLength {
            length: locator.len(),
            minimum: 0,
        })?;
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_LOB_OP, seq_num);
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_1 {
        writer.write_ub8(0);
    }
    writer.write_u8(1);
    writer.write_ub4(locator_len);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(TNS_LOB_OP_READ);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub8(offset);
    writer.write_ub8(0);
    writer.write_u8(1);
    for _ in 0..3 {
        writer.write_u16be(0);
    }
    writer.write_raw(locator);
    writer.write_ub8(amount);
    Ok(writer.into_bytes())
}

fn write_lob_op_header(
    writer: &mut TtcWriter,
    locator: &[u8],
    seq_num: u8,
    ttc_field_version: u8,
    operation: u32,
    dest_length: u32,
    source_offset: u64,
    dest_offset: u64,
    pointer_charset: bool,
    pointer_null_lob: bool,
    send_amount: bool,
) -> Result<()> {
    let locator_len =
        u32::try_from(locator.len()).map_err(|_| ProtocolError::InvalidPacketLength {
            length: locator.len(),
            minimum: 0,
        })?;
    writer.write_function_code_with_seq(TNS_FUNC_LOB_OP, seq_num);
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_1 {
        writer.write_ub8(0);
    }
    writer.write_u8(1);
    writer.write_ub4(locator_len);
    writer.write_u8(0);
    writer.write_ub4(dest_length);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_u8(u8::from(pointer_charset));
    writer.write_u8(0);
    writer.write_u8(u8::from(pointer_null_lob));
    writer.write_ub4(operation);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub8(source_offset);
    writer.write_ub8(dest_offset);
    writer.write_u8(u8::from(send_amount));
    for _ in 0..3 {
        writer.write_u16be(0);
    }
    writer.write_raw(locator);
    Ok(())
}

pub fn build_lob_create_temp_payload_with_seq(
    ora_type_num: u8,
    csfrm: u8,
    seq_num: u8,
    ttc_field_version: u8,
) -> Result<Vec<u8>> {
    let mut writer = TtcWriter::new();
    write_lob_op_header(
        &mut writer,
        &[0; 40],
        seq_num,
        ttc_field_version,
        TNS_LOB_OP_CREATE_TEMP,
        TNS_DURATION_SESSION,
        u64::from(csfrm),
        u64::from(ora_type_num),
        true,
        true,
        false,
    )?;
    writer.write_ub4(TNS_CHARSET_UTF8.into());
    Ok(writer.into_bytes())
}

pub fn build_lob_write_payload_with_seq(
    locator: &[u8],
    offset: u64,
    data: &[u8],
    seq_num: u8,
    ttc_field_version: u8,
) -> Result<Vec<u8>> {
    let mut writer = TtcWriter::new();
    write_lob_op_header(
        &mut writer,
        locator,
        seq_num,
        ttc_field_version,
        TNS_LOB_OP_WRITE,
        0,
        offset,
        0,
        false,
        false,
        false,
    )?;
    writer.write_u8(TNS_MSG_TYPE_LOB_DATA);
    writer.write_bytes_with_length(data)?;
    Ok(writer.into_bytes())
}

pub fn parse_query_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
) -> Result<QueryResult> {
    parse_query_response_with_previous(payload, capabilities, None)
}

pub fn parse_query_response_with_binds(
    payload: &[u8],
    capabilities: ClientCapabilities,
    binds: &[BindValue],
) -> Result<QueryResult> {
    let bind_columns = binds.iter().map(bind_column_metadata).collect::<Vec<_>>();
    let output_bind_indexes = binds
        .iter()
        .enumerate()
        .filter_map(|(index, value)| value.is_return_output().then_some(index))
        .collect::<Vec<_>>();
    parse_query_response_with_context_and_binds(
        payload,
        capabilities,
        &[],
        None,
        &bind_columns,
        &output_bind_indexes,
        false,
    )
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
    parse_query_response_with_context_and_binds(
        payload,
        capabilities,
        previous_columns,
        previous_row,
        &[],
        &[],
        false,
    )
}

pub fn parse_fetch_response_with_context(
    payload: &[u8],
    capabilities: ClientCapabilities,
    previous_columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
) -> Result<QueryResult> {
    parse_query_response_with_context_and_binds(
        payload,
        capabilities,
        previous_columns,
        previous_row,
        &[],
        &[],
        true,
    )
}

fn parse_query_response_with_context_and_binds(
    payload: &[u8],
    capabilities: ClientCapabilities,
    previous_columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
    bind_columns: &[ColumnMetadata],
    output_bind_indexes: &[usize],
    fetch_long_status: bool,
) -> Result<QueryResult> {
    let mut reader = TtcReader::new(payload);
    let mut result = QueryResult {
        columns: previous_columns.to_vec(),
        more_rows: true,
        ..QueryResult::default()
    };
    let mut bit_vector: Option<Vec<u8>> = None;
    let mut out_bind_indexes: Vec<usize> = Vec::new();
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_DESCRIBE_INFO => {
                let _describe_name = reader.read_bytes()?;
                result.columns.clear();
                parse_describe_info(&mut reader, capabilities, &mut result)?;
            }
            TNS_MSG_TYPE_ROW_HEADER => {
                bit_vector = parse_row_header(&mut reader)?;
            }
            TNS_MSG_TYPE_ROW_DATA => {
                if result.columns.is_empty() && !out_bind_indexes.is_empty() {
                    parse_out_bind_row_data(
                        &mut reader,
                        &mut result,
                        bind_columns,
                        &out_bind_indexes,
                    )?;
                } else if result.columns.is_empty() && !output_bind_indexes.is_empty() {
                    parse_returning_row_data(
                        &mut reader,
                        &mut result,
                        bind_columns,
                        output_bind_indexes,
                    )?;
                } else {
                    parse_row_data(
                        &mut reader,
                        &mut result,
                        bit_vector.as_deref(),
                        previous_row,
                        fetch_long_status,
                    )?;
                }
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
            TNS_MSG_TYPE_IO_VECTOR => {
                out_bind_indexes = parse_io_vector(&mut reader, bind_columns.len())?
                    .into_iter()
                    .filter(|index| !output_bind_indexes.contains(index))
                    .collect();
            }
            TNS_MSG_TYPE_FLUSH_OUT_BINDS => break,
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => skip_server_side_piggyback(&mut reader)?,
            TNS_MSG_TYPE_END_OF_RESPONSE => break,
            TNS_MSG_TYPE_ERROR => {
                let info = parse_server_error_info(&mut reader, capabilities.ttc_field_version)?;
                if info.cursor_id != 0 {
                    result.cursor_id = u32::from(info.cursor_id);
                }
                result.row_count = info.row_count;
                result.compilation_error_warning |= info.compilation_error_warning;
                if info.number == TNS_ERR_NO_DATA_FOUND && !result.columns.is_empty() {
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

fn bind_column_metadata(value: &BindValue) -> ColumnMetadata {
    let (ora_type_num, csfrm, buffer_size) = bind_metadata(value);
    let object_schema = match value {
        BindValue::ObjectOutput { schema, .. } => Some(schema.clone()),
        _ => None,
    };
    let object_type_name = match value {
        BindValue::ObjectOutput { type_name, .. } => Some(type_name.clone()),
        _ => None,
    };
    ColumnMetadata {
        name: String::new(),
        ora_type_num,
        csfrm,
        precision: 0,
        scale: 0,
        buffer_size,
        max_size: buffer_size,
        nulls_allowed: true,
        is_json: false,
        is_oson: false,
        object_schema,
        object_type_name,
        is_array: matches!(value, BindValue::Array { .. }),
    }
}

fn parse_io_vector(reader: &mut TtcReader<'_>, bind_count: usize) -> Result<Vec<usize>> {
    let _flags = reader.read_u8()?;
    let temp16 = reader.read_ub2()?;
    let temp32 = reader.read_ub4()?;
    let num_binds = usize::try_from(temp32)
        .map_err(|_| ProtocolError::InvalidPacketLength {
            length: usize::MAX,
            minimum: 0,
        })?
        .checked_mul(256)
        .and_then(|value| value.checked_add(usize::from(temp16)))
        .ok_or(ProtocolError::InvalidPacketLength {
            length: usize::MAX,
            minimum: 0,
        })?;
    let _num_iters_this_time = reader.read_ub4()?;
    let _uac_buffer_length = reader.read_ub2()?;
    let fast_fetch_len = reader.read_ub2()?;
    if fast_fetch_len > 0 {
        reader.skip(usize::from(fast_fetch_len))?;
    }
    let rowid_len = reader.read_ub2()?;
    if rowid_len > 0 {
        reader.skip(usize::from(rowid_len))?;
    }
    let mut out_indexes = Vec::new();
    for index in 0..num_binds {
        let direction = reader.read_u8()?;
        if index < bind_count && direction != TNS_BIND_DIR_INPUT {
            out_indexes.push(index);
        }
    }
    Ok(out_indexes)
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
    let object_schema = reader.read_string_with_length()?;
    let object_type_name = reader.read_string_with_length()?;
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
        object_schema,
        object_type_name,
        is_array: false,
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
    fetch_long_status: bool,
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
        if fetch_long_status
            && matches!(
                metadata.ora_type_num,
                ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_LONG_RAW
            )
        {
            let _null_indicator = reader.read_sb4()?;
            let _return_code = reader.read_ub4()?;
        }
    }
    result.rows.push(row);
    Ok(())
}

fn parse_out_bind_row_data(
    reader: &mut TtcReader<'_>,
    result: &mut QueryResult,
    bind_columns: &[ColumnMetadata],
    out_bind_indexes: &[usize],
) -> Result<()> {
    for index in out_bind_indexes {
        let metadata = bind_columns.get(*index).ok_or(ProtocolError::TtcDecode(
            "out bind index without bind metadata",
        ))?;
        if metadata.is_array {
            let num_elements = usize::try_from(reader.read_ub4()?).map_err(|_| {
                ProtocolError::InvalidPacketLength {
                    length: usize::MAX,
                    minimum: 0,
                }
            })?;
            let mut values = Vec::with_capacity(num_elements);
            for _ in 0..num_elements {
                let value = parse_column_value(reader, metadata)?;
                let actual_num_bytes = reader.read_sb4()?;
                if actual_num_bytes != 0 && value.is_some() {
                    return Err(ProtocolError::TtcDecode("truncated array OUT bind value"));
                }
                values.push(value);
            }
            result
                .out_values
                .push((*index, Some(QueryValue::Array(values))));
            continue;
        }
        let value = parse_column_value(reader, metadata)?;
        let actual_num_bytes = reader.read_sb4()?;
        if actual_num_bytes != 0 && value.is_some() {
            return Err(ProtocolError::TtcDecode("truncated OUT bind value"));
        }
        result.out_values.push((*index, value));
    }
    Ok(())
}

fn parse_returning_row_data(
    reader: &mut TtcReader<'_>,
    result: &mut QueryResult,
    bind_columns: &[ColumnMetadata],
    output_bind_indexes: &[usize],
) -> Result<()> {
    for index in output_bind_indexes {
        let metadata = bind_columns.get(*index).ok_or(ProtocolError::TtcDecode(
            "return bind index without bind metadata",
        ))?;
        let num_rows = usize::try_from(reader.read_ub4()?).map_err(|_| {
            ProtocolError::InvalidPacketLength {
                length: usize::MAX,
                minimum: 0,
            }
        })?;
        let mut values = Vec::with_capacity(num_rows);
        for _ in 0..num_rows {
            let value = parse_column_value(reader, metadata)?;
            let actual_num_bytes = reader.read_sb4()?;
            if actual_num_bytes != 0 && value.is_some() {
                return Err(ProtocolError::TtcDecode("truncated DML RETURNING value"));
            }
            values.push(value);
        }
        result.return_values.push((*index, values));
    }
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
        ORA_TYPE_NUM_ROWID => parse_rowid_value(reader).map(|value| value.map(QueryValue::Rowid)),
        ORA_TYPE_NUM_UROWID => parse_urowid_value(reader).map(|value| value.map(QueryValue::Rowid)),
        ORA_TYPE_NUM_NUMBER | ORA_TYPE_NUM_BINARY_INTEGER => {
            let Some(bytes) = reader.read_bytes()? else {
                return Ok(None);
            };
            decode_number_value(&bytes).map(Some)
        }
        ORA_TYPE_NUM_BINARY_DOUBLE => {
            let Some(bytes) = reader.read_bytes()? else {
                return Ok(None);
            };
            decode_binary_double(&bytes)
                .map(|value| Some(QueryValue::BinaryDouble(value.to_string())))
        }
        ORA_TYPE_NUM_DATE
        | ORA_TYPE_NUM_TIMESTAMP
        | ORA_TYPE_NUM_TIMESTAMP_LTZ
        | ORA_TYPE_NUM_TIMESTAMP_TZ => {
            let Some(bytes) = reader.read_bytes()? else {
                return Ok(None);
            };
            decode_datetime_value(&bytes).map(Some)
        }
        ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_BFILE => {
            parse_lob_value(reader, metadata)
        }
        ORA_TYPE_NUM_CURSOR => parse_cursor_value(reader).map(Some),
        ORA_TYPE_NUM_OBJECT => parse_object_value(reader, metadata),
        _ => Err(ProtocolError::UnsupportedFeature("query column type")),
    }
}

fn encode_rowid_component(mut value: u32, size: usize, output: &mut String) {
    let mut encoded = vec![b'A'; size];
    for index in 0..size {
        let alphabet_index = usize::try_from(value & 0x3f).unwrap_or(0);
        encoded[size - index - 1] = TNS_BASE64_ALPHABET[alphabet_index];
        value >>= 6;
    }
    output.extend(encoded.into_iter().map(char::from));
}

fn encode_physical_rowid(rba: u32, partition_id: u16, block_num: u32, slot_num: u16) -> String {
    let mut output = String::with_capacity(ORA_TYPE_SIZE_ROWID as usize);
    encode_rowid_component(rba, 6, &mut output);
    encode_rowid_component(u32::from(partition_id), 3, &mut output);
    encode_rowid_component(block_num, 6, &mut output);
    encode_rowid_component(u32::from(slot_num), 3, &mut output);
    output
}

fn parse_rowid_value(reader: &mut TtcReader<'_>) -> Result<Option<String>> {
    let len = reader.read_u8()?;
    if len == 0 || len == crate::wire::TNS_NULL_LENGTH_INDICATOR {
        return Ok(None);
    }
    let rba = reader.read_ub4()?;
    let partition_id = reader.read_ub2()?;
    reader.skip(1)?;
    let block_num = reader.read_ub4()?;
    let slot_num = reader.read_ub2()?;
    Ok(Some(encode_physical_rowid(
        rba,
        partition_id,
        block_num,
        slot_num,
    )))
}

fn encode_logical_urowid(bytes: &[u8]) -> String {
    let mut input_offset = 1;
    let mut input_len = bytes.len().saturating_sub(1);
    let mut output = String::with_capacity((bytes.len() / 3) * 4 + 4);
    output.push('*');
    while input_len > 0 {
        let mut pos = bytes[input_offset] >> 2;
        output.push(char::from(TNS_BASE64_ALPHABET[usize::from(pos)]));

        pos = (bytes[input_offset] & 0x03) << 4;
        if input_len == 1 {
            output.push(char::from(TNS_BASE64_ALPHABET[usize::from(pos)]));
            break;
        }
        input_offset += 1;
        pos |= (bytes[input_offset] & 0xf0) >> 4;
        output.push(char::from(TNS_BASE64_ALPHABET[usize::from(pos)]));

        pos = (bytes[input_offset] & 0x0f) << 2;
        if input_len == 2 {
            output.push(char::from(TNS_BASE64_ALPHABET[usize::from(pos)]));
            break;
        }
        input_offset += 1;
        pos |= (bytes[input_offset] & 0xc0) >> 6;
        output.push(char::from(TNS_BASE64_ALPHABET[usize::from(pos)]));

        pos = bytes[input_offset] & 0x3f;
        output.push(char::from(TNS_BASE64_ALPHABET[usize::from(pos)]));
        input_offset += 1;
        input_len -= 3;
    }
    output
}

fn parse_urowid_value(reader: &mut TtcReader<'_>) -> Result<Option<String>> {
    if reader.read_bytes()?.is_none() {
        return Ok(None);
    }
    let Some(bytes) = reader.read_bytes()? else {
        return Ok(None);
    };
    if bytes.len() < 13 {
        return Err(ProtocolError::TtcDecode("encoded UROWID too short"));
    }
    if bytes[0] == 1 {
        let rba = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        let partition_id = u16::from_be_bytes([bytes[5], bytes[6]]);
        let block_num = u32::from_be_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]);
        let slot_num = u16::from_be_bytes([bytes[11], bytes[12]]);
        Ok(Some(encode_physical_rowid(
            rba,
            partition_id,
            block_num,
            slot_num,
        )))
    } else {
        Ok(Some(encode_logical_urowid(&bytes)))
    }
}

fn parse_lob_value(
    reader: &mut TtcReader<'_>,
    metadata: &ColumnMetadata,
) -> Result<Option<QueryValue>> {
    let num_bytes = reader.read_ub4()?;
    if num_bytes == 0 {
        return Ok(None);
    }
    let (size, chunk_size) = if matches!(metadata.ora_type_num, ORA_TYPE_NUM_BFILE) {
        (0, 0)
    } else {
        (reader.read_ub8()?, reader.read_ub4()?)
    };
    let Some(locator) = reader.read_bytes()? else {
        return Ok(None);
    };
    Ok(Some(QueryValue::Lob {
        ora_type_num: metadata.ora_type_num,
        csfrm: metadata.csfrm,
        locator,
        size,
        chunk_size,
    }))
}

fn parse_object_value(
    reader: &mut TtcReader<'_>,
    metadata: &ColumnMetadata,
) -> Result<Option<QueryValue>> {
    let _toid = reader.read_bytes_with_length()?;
    let _oid = reader.read_bytes_with_length()?;
    let _snapshot = reader.read_bytes_with_length()?;
    let _version = reader.read_ub2()?;
    let num_bytes = reader.read_ub4()?;
    reader.skip(2)?;
    if num_bytes == 0 {
        return Ok(None);
    }
    let Some(packed_data) = reader.read_bytes()? else {
        return Ok(None);
    };
    Ok(Some(QueryValue::Object {
        schema: metadata.object_schema.clone(),
        type_name: metadata.object_type_name.clone(),
        packed_data,
    }))
}

fn parse_cursor_value(reader: &mut TtcReader<'_>) -> Result<QueryValue> {
    reader.skip(1)?;
    let mut result = QueryResult::default();
    parse_describe_info(reader, ClientCapabilities::default(), &mut result)?;
    let cursor_id = u32::from(reader.read_ub2()?);
    Ok(QueryValue::Cursor {
        columns: result.columns,
        cursor_id,
    })
}

pub fn parse_lob_read_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
    locator: &[u8],
) -> Result<LobReadResult> {
    parse_lob_op_response(payload, capabilities, locator, false, true)
}

pub fn parse_lob_create_temp_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
) -> Result<LobReadResult> {
    parse_lob_op_response(payload, capabilities, &[0; 40], true, false)
}

pub fn parse_lob_write_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
    locator: &[u8],
) -> Result<LobReadResult> {
    parse_lob_op_response(payload, capabilities, locator, false, false)
}

fn parse_lob_op_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
    locator: &[u8],
    is_create_temp: bool,
    read_amount: bool,
) -> Result<LobReadResult> {
    let mut reader = TtcReader::new(payload);
    let mut result = LobReadResult {
        locator: locator.to_vec(),
        ..LobReadResult::default()
    };
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_LOB_DATA => {
                result.data = reader.read_bytes()?;
            }
            TNS_MSG_TYPE_PARAMETER => {
                if !result.locator.is_empty() {
                    result.locator = reader.read_raw(result.locator.len())?.to_vec();
                }
                if is_create_temp {
                    let _charset = reader.read_ub2()?;
                    reader.skip(1)?;
                } else if read_amount {
                    let amount = reader.read_sb8()?;
                    if amount > 0 {
                        result.amount = amount as u64;
                    }
                }
            }
            TNS_MSG_TYPE_STATUS => {
                let _call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => skip_server_side_piggyback(&mut reader)?,
            TNS_MSG_TYPE_END_OF_RESPONSE => break,
            TNS_MSG_TYPE_ERROR => {
                let info = parse_server_error_info(&mut reader, capabilities.ttc_field_version)?;
                if info.number != 0 {
                    return Err(ProtocolError::ServerError(info.message));
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
    Ok(result)
}

fn encode_oracle_date(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
) -> Result<[u8; ORA_TYPE_SIZE_DATE as usize]> {
    if !(1..=9999).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(ProtocolError::TtcDecode("invalid DATE bind"));
    }
    let century = year / 100 + 100;
    let year_in_century = year % 100 + 100;
    Ok([
        u8::try_from(century).map_err(|_| ProtocolError::TtcDecode("invalid DATE century"))?,
        u8::try_from(year_in_century).map_err(|_| ProtocolError::TtcDecode("invalid DATE year"))?,
        month,
        day,
        hour + 1,
        minute + 1,
        second + 1,
    ])
}

fn encode_oracle_timestamp(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    nanosecond: u32,
) -> Result<Vec<u8>> {
    if nanosecond > 999_999_999 {
        return Err(ProtocolError::TtcDecode("invalid TIMESTAMP fraction"));
    }
    let date = encode_oracle_date(year, month, day, hour, minute, second)?;
    if nanosecond == 0 {
        return Ok(date.to_vec());
    }
    let mut bytes = Vec::with_capacity(ORA_TYPE_SIZE_TIMESTAMP as usize);
    bytes.extend_from_slice(&date);
    bytes.extend_from_slice(&nanosecond.to_be_bytes());
    Ok(bytes)
}

fn encode_oracle_timestamp_tz(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    nanosecond: u32,
) -> Result<Vec<u8>> {
    if nanosecond > 999_999_999 {
        return Err(ProtocolError::TtcDecode(
            "invalid TIMESTAMP WITH TIME ZONE fraction",
        ));
    }
    let mut bytes = Vec::with_capacity(ORA_TYPE_SIZE_TIMESTAMP_TZ as usize);
    let date = encode_oracle_date(year, month, day, hour, minute, second)?;
    bytes.extend_from_slice(&date);
    bytes.extend_from_slice(&nanosecond.to_be_bytes());
    bytes.push(TZ_HOUR_OFFSET);
    bytes.push(TZ_MINUTE_OFFSET);
    Ok(bytes)
}

pub fn decode_datetime_value(bytes: &[u8]) -> Result<QueryValue> {
    if bytes.len() < ORA_TYPE_SIZE_DATE as usize {
        return Err(ProtocolError::TtcDecode("DATE value too short"));
    }
    let mut year = (i32::from(bytes[0]) - 100) * 100 + i32::from(bytes[1]) - 100;
    let mut month = bytes[2];
    let mut day = bytes[3];
    let mut hour = bytes[4].saturating_sub(1);
    let mut minute = bytes[5].saturating_sub(1);
    let mut second = bytes[6].saturating_sub(1);
    let nanosecond = if bytes.len() >= ORA_TYPE_SIZE_TIMESTAMP as usize {
        u32::from_be_bytes(
            bytes[7..11]
                .try_into()
                .map_err(|_| ProtocolError::TtcDecode("invalid TIMESTAMP fraction"))?,
        )
    } else {
        0
    };
    if bytes.len() >= ORA_TYPE_SIZE_TIMESTAMP_TZ as usize && bytes[11] != 0 && bytes[12] != 0 {
        if bytes[11] & TNS_HAS_REGION_ID != 0 {
            return Err(ProtocolError::UnsupportedFeature(
                "named TIMESTAMP WITH TIME ZONE region",
            ));
        }
        let offset_minutes = (i32::from(bytes[11]) - i32::from(TZ_HOUR_OFFSET)) * 60
            + i32::from(bytes[12])
            - i32::from(TZ_MINUTE_OFFSET);
        (year, month, day, hour, minute, second) =
            adjust_datetime_by_minutes(year, month, day, hour, minute, second, offset_minutes)?;
    }
    Ok(QueryValue::DateTime {
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanosecond,
    })
}

fn adjust_datetime_by_minutes(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    offset_minutes: i32,
) -> Result<(i32, u8, u8, u8, u8, u8)> {
    let days = days_from_civil(year, month, day)?;
    let seconds_of_day = i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second);
    let total_seconds = days
        .checked_mul(86_400)
        .and_then(|value| value.checked_add(seconds_of_day))
        .and_then(|value| value.checked_add(i64::from(offset_minutes) * 60))
        .ok_or(ProtocolError::TtcDecode(
            "TIMESTAMP WITH TIME ZONE offset overflow",
        ))?;
    let adjusted_days = total_seconds.div_euclid(86_400);
    let adjusted_seconds = total_seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(adjusted_days)?;
    let hour = u8::try_from(adjusted_seconds / 3_600)
        .map_err(|_| ProtocolError::TtcDecode("invalid adjusted TIMESTAMP hour"))?;
    let minute = u8::try_from((adjusted_seconds % 3_600) / 60)
        .map_err(|_| ProtocolError::TtcDecode("invalid adjusted TIMESTAMP minute"))?;
    let second = u8::try_from(adjusted_seconds % 60)
        .map_err(|_| ProtocolError::TtcDecode("invalid adjusted TIMESTAMP second"))?;
    Ok((year, month, day, hour, minute, second))
}

fn days_from_civil(year: i32, month: u8, day: u8) -> Result<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(ProtocolError::TtcDecode("invalid TIMESTAMP date"));
    }
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i32::from(month);
    let day = i32::from(day);
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Ok(i64::from(era) * 146_097 + i64::from(day_of_era) - 719_468)
}

fn civil_from_days(days: i64) -> Result<(i32, u8, u8)> {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    Ok((
        i32::try_from(year)
            .map_err(|_| ProtocolError::TtcDecode("invalid adjusted TIMESTAMP year"))?,
        u8::try_from(month)
            .map_err(|_| ProtocolError::TtcDecode("invalid adjusted TIMESTAMP month"))?,
        u8::try_from(day)
            .map_err(|_| ProtocolError::TtcDecode("invalid adjusted TIMESTAMP day"))?,
    ))
}

fn encode_binary_double(value: f64) -> [u8; 8] {
    let mut bytes = value.to_bits().to_be_bytes();
    if bytes[0] & 0x80 == 0 {
        bytes[0] |= 0x80;
    } else {
        for byte in &mut bytes {
            *byte = !*byte;
        }
    }
    bytes
}

fn decode_binary_double(bytes: &[u8]) -> Result<f64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| ProtocolError::TtcDecode("invalid BINARY_DOUBLE length"))?;
    let mut decoded = bytes;
    if decoded[0] & 0x80 != 0 {
        decoded[0] &= 0x7f;
    } else {
        for byte in &mut decoded {
            *byte = !*byte;
        }
    }
    Ok(f64::from_bits(u64::from_be_bytes(decoded)))
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
    if matches!(value.first(), Some(&b'-')) {
        is_negative = true;
        pos += 1;
    }

    let mut digits = Vec::with_capacity(NUMBER_AS_TEXT_CHARS);
    while let Some(byte) = value.get(pos).copied() {
        if matches!(byte, b'.' | b'e' | b'E') {
            break;
        }
        if !byte.is_ascii_digit() {
            return Err(ProtocolError::TtcDecode("invalid NUMBER bind"));
        }
        let digit = byte - b'0';
        pos += 1;
        if digit == 0 && digits.is_empty() {
            continue;
        }
        digits.push(digit);
    }
    let mut decimal_point_index = i32::try_from(digits.len()).unwrap_or(i32::MAX);

    if matches!(value.get(pos), Some(&b'.')) {
        pos += 1;
        while let Some(byte) = value.get(pos).copied() {
            if matches!(byte, b'e' | b'E') {
                break;
            }
            if !byte.is_ascii_digit() {
                return Err(ProtocolError::TtcDecode("invalid NUMBER bind"));
            }
            let digit = byte - b'0';
            pos += 1;
            if digit == 0 && digits.is_empty() {
                decimal_point_index -= 1;
                continue;
            }
            digits.push(digit);
        }
    }

    if matches!(value.get(pos).copied(), Some(b'e' | b'E')) {
        pos += 1;
        let mut exponent_is_negative = false;
        if let Some(byte) = value.get(pos).copied() {
            if byte == b'-' {
                exponent_is_negative = true;
                pos += 1;
            } else if byte == b'+' {
                pos += 1;
            }
        }
        let exponent_start = pos;
        while let Some(byte) = value.get(pos).copied() {
            if !byte.is_ascii_digit() {
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

pub fn decode_number_value(bytes: &[u8]) -> Result<QueryValue> {
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
        if index > 0
            && matches!(
                i16::try_from(index)
                    .unwrap_or(i16::MAX)
                    .cmp(&decimal_point_index),
                std::cmp::Ordering::Equal
            )
        {
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
    compilation_error_warning: bool,
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
    let warning_flags = reader.read_u8()?;
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
        compilation_error_warning: warning_flags & 0x20 != 0,
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
    fn nchar_bind_text_uses_utf16be() {
        assert_eq!(encode_text_value("Aあ", CS_FORM_IMPLICIT), b"A\xE3\x81\x82");
        assert_eq!(
            encode_text_value("Aあ", CS_FORM_NCHAR),
            vec![0x00, 0x41, 0x30, 0x42]
        );
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
    fn fetch_response_skips_long_status_fields() {
        let payload =
            Vec::from_hex("07036162638101001d").expect("fixture response should be valid hex");
        let columns = vec![long_column("LONGCOL")];

        let parsed = parse_fetch_response_with_context(
            &payload,
            ClientCapabilities::default(),
            &columns,
            None,
        )
        .expect("fetch response should consume LONG status fields");

        assert_eq!(
            parsed.rows,
            vec![vec![Some(QueryValue::Text("abc".into()))]]
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

    #[test]
    fn lob_read_payload_writes_modern_token_field() {
        let locator = [0x00, 0x70, 0xaa];
        let modern =
            build_lob_read_payload_with_seq(&locator, 1, 5, 8, TNS_CCAP_FIELD_VERSION_23_1_EXT_1)
                .expect("LOB read payload should encode");
        assert_eq!(
            &modern[..7],
            &[TNS_MSG_TYPE_FUNCTION, TNS_FUNC_LOB_OP, 8, 0, 1, 1, 3]
        );

        let legacy =
            build_lob_read_payload_with_seq(&locator, 1, 5, 8, TNS_CCAP_FIELD_VERSION_23_1)
                .expect("LOB read payload should encode");
        assert_eq!(
            &legacy[..6],
            &[TNS_MSG_TYPE_FUNCTION, TNS_FUNC_LOB_OP, 8, 1, 1, 3]
        );
    }

    #[test]
    fn rowid_value_decodes_physical_rowid() {
        let mut reader = TtcReader::new(&[
            13, // non-null rowid marker
            1, 1, // rba
            1, 2, // partition id
            0, // ignored padding byte
            1, 3, // block number
            1, 4, // slot number
        ]);

        let value = parse_rowid_value(&mut reader).expect("physical rowid should decode");

        assert_eq!(value.as_deref(), Some("AAAAABAACAAAAADAAE"));
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn urowid_value_decodes_physical_rowid() {
        let mut reader = TtcReader::new(&[
            1, 13, // ignored first length buffer
            13, // second buffer length
            1,  // physical rowid marker
            0, 0, 0, 1, // rba
            0, 2, // partition id
            0, 0, 0, 3, // block number
            0, 4, // slot number
        ]);

        let value = parse_urowid_value(&mut reader).expect("physical urowid should decode");

        assert_eq!(value.as_deref(), Some("AAAAABAACAAAAADAAE"));
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn urowid_value_decodes_logical_rowid() {
        let mut reader = TtcReader::new(&[
            1, 13, // ignored first length buffer
            13, // second buffer length
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
        ]);

        let value = parse_urowid_value(&mut reader).expect("logical urowid should decode");

        assert_eq!(value.as_deref(), Some("*AQIDBAUGBwgJCgsM"));
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn binary_double_round_trips_oracle_canonical_bytes() {
        for value in [0.0, -0.0, 1.5, -2.25, f64::INFINITY, f64::NEG_INFINITY] {
            let decoded = decode_binary_double(&encode_binary_double(value))
                .expect("BINARY_DOUBLE should round trip");
            assert_eq!(decoded.to_bits(), value.to_bits());
        }

        let decoded =
            decode_binary_double(&encode_binary_double(f64::NAN)).expect("NaN should decode");
        assert!(decoded.is_nan());
    }

    #[test]
    fn bind_value_type_info_reports_protocol_metadata() {
        assert_eq!(bind_value_type_info(&BindValue::Null), None);
        assert_eq!(
            bind_value_type_info(&BindValue::Text("abc".into())),
            Some(BindTypeInfo {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_IMPLICIT,
                buffer_size: 12,
            })
        );
        assert_eq!(
            bind_value_type_info(&BindValue::BinaryDouble(1.25)),
            Some(BindTypeInfo {
                ora_type_num: ORA_TYPE_NUM_BINARY_DOUBLE,
                csfrm: 0,
                buffer_size: ORA_TYPE_SIZE_BINARY_DOUBLE,
            })
        );
    }

    #[test]
    fn define_metadata_from_bind_preserves_clob_long_define_semantics() {
        let mut source = number_column("VALUE");
        source.ora_type_num = ORA_TYPE_NUM_CLOB;
        source.csfrm = CS_FORM_NCHAR;
        let metadata = define_metadata_from_bind(
            &source,
            &BindValue::TypedNull {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_IMPLICIT,
                buffer_size: 128,
            },
        );

        assert_eq!(metadata.ora_type_num, ORA_TYPE_NUM_LONG);
        assert_eq!(metadata.csfrm, CS_FORM_NCHAR);
        assert_eq!(metadata.buffer_size, TNS_MAX_LONG_LENGTH);
        assert_eq!(metadata.max_size, 0);
    }

    #[test]
    fn output_bind_normalizes_type_metadata() {
        assert_eq!(
            output_bind(BindValue::Text("abc".into())),
            BindValue::Output {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_IMPLICIT,
                buffer_size: 12,
            }
        );
        assert_eq!(
            returning_output_bind(BindValue::Null),
            BindValue::ReturnOutput {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_IMPLICIT,
                buffer_size: 1,
            }
        );
        assert!(is_cursor_bind_template(&cursor_bind_template()));
    }

    #[test]
    fn public_dbtype_names_come_from_protocol_metadata() {
        assert_eq!(
            public_dbtype_name_from_type_name("NATIVE_FLOAT"),
            "DB_TYPE_BINARY_DOUBLE"
        );
        assert_eq!(
            public_dbtype_name_from_bind(&BindValue::BinaryDouble(1.25)),
            "DB_TYPE_BINARY_DOUBLE"
        );
        assert_eq!(
            public_dbtype_name_from_bind(&BindValue::TypedNull {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_NCHAR,
                buffer_size: 16,
            }),
            "DB_TYPE_NVARCHAR"
        );
    }

    #[test]
    fn bind_templates_are_protocol_owned() {
        assert_eq!(
            bind_template_from_type_name("DB_TYPE_NCLOB", 0),
            BindValue::TypedNull {
                ora_type_num: ORA_TYPE_NUM_LONG,
                csfrm: CS_FORM_NCHAR,
                buffer_size: TNS_MAX_LONG_LENGTH,
            }
        );
        assert_eq!(
            dbobject_element_bind_type_info("DB_TYPE_NCHAR", 12),
            BindTypeInfo {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_NCHAR,
                buffer_size: 4000,
            }
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
            object_schema: None,
            object_type_name: None,
            is_array: false,
        }
    }

    fn long_column(name: &str) -> ColumnMetadata {
        ColumnMetadata {
            name: name.into(),
            ora_type_num: ORA_TYPE_NUM_LONG,
            csfrm: CS_FORM_IMPLICIT,
            precision: 0,
            scale: 0,
            buffer_size: TNS_MAX_LONG_LENGTH,
            max_size: 0,
            nulls_allowed: true,
            is_json: false,
            is_oson: false,
            object_schema: None,
            object_type_name: None,
            is_array: false,
        }
    }
}

fn has_u8_flag(flags: u8, mask: u8) -> bool {
    flags & mask > 0
}

fn has_u32_flag(flags: u32, mask: u32) -> bool {
    flags & mask > 0
}
