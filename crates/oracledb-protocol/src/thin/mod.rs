#![forbid(unsafe_code)]

//! TTC thin-protocol wire codecs, split across cohesive submodules.
//! `mod.rs` wires the submodules and re-exports their items so every
//! `oracledb_protocol::thin::*` path that downstream crates depend on stays
//! reachable, and so submodules see each other via `use super::*`.

pub(crate) use std::collections::BTreeMap;

pub(crate) use crate::sql::statement_is_plsql;
pub(crate) use crate::wire::{TtcReader, TtcWriter};
pub(crate) use crate::{ProtocolError, Result, TNS_VERSION_DESIRED, TNS_VERSION_MIN};
pub(crate) use hex::FromHex;

mod constants;
mod types;
mod dbobject;
mod connect;
mod sessionless;
mod execute;
mod bind;
mod fetch;
mod lob;
mod auth;
mod codecs;
mod errors;

pub use constants::*;
pub use types::*;
pub use dbobject::*;
pub use connect::*;
pub use sessionless::*;
pub use execute::*;
pub use bind::*;
pub use fetch::*;
pub use lob::*;
pub use auth::*;
pub use codecs::*;
// `errors` holds only crate-internal items (ServerErrorInfo + parse/skip helpers);
// re-export at crate visibility so the glob has something to re-export.
pub(crate) use errors::*;

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
    fn lob_locator_temporary_flags_match_reference_offsets() {
        let mut locator = vec![0; 40];
        assert!(!lob_locator_is_temporary(&locator));

        locator[TNS_LOB_LOC_OFFSET_FLAG_1] = TNS_LOB_LOC_FLAGS_ABSTRACT;
        assert!(lob_locator_is_temporary(&locator));

        locator[TNS_LOB_LOC_OFFSET_FLAG_1] = 0;
        locator[TNS_LOB_LOC_OFFSET_FLAG_4] = TNS_LOB_LOC_FLAGS_TEMP;
        assert!(lob_locator_is_temporary(&locator));
    }

    #[test]
    fn lob_free_temp_payload_writes_array_free_operation() {
        let locator = vec![0xaa; 40];
        let payload = build_lob_free_temp_payload_with_seq(
            std::slice::from_ref(&locator),
            9,
            TNS_CCAP_FIELD_VERSION_23_1_EXT_1,
        )
        .expect("LOB free-temp payload should encode");

        assert_eq!(
            &payload[..19],
            &[
                TNS_MSG_TYPE_FUNCTION,
                TNS_FUNC_LOB_OP,
                9,
                0,
                1,
                1,
                40,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                4,
                0,
                8,
                1,
                0x11,
            ]
        );
        assert!(payload.ends_with(&locator));
    }

    #[test]
    fn lob_free_temp_response_skips_returned_locator_parameter() {
        let payload = Vec::from_hex(concat!(
            "0800260000020080000002ee5500000044000000030369000a000000000002",
            "5295f656000000010000040101021a390000000000000000000000000000",
            "00000000000a000000000000000000001d",
        ))
        .expect("fixture response should be valid hex");

        parse_lob_free_temp_response(&payload, ClientCapabilities::default(), 40)
            .expect("free-temp response should consume returned locator");
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
    fn adjust_refetch_metadata_follows_reference_rules() {
        let column = |ora_type_num: u8, csfrm: u8| ColumnMetadata {
            name: "VALUE".to_string(),
            ora_type_num,
            csfrm,
            precision: 0,
            scale: 0,
            buffer_size: 4000,
            max_size: 1000,
            nulls_allowed: true,
            is_json: false,
            is_oson: false,
            object_schema: None,
            object_type_name: None,
            is_array: false,
            vector_dimensions: None,
            vector_format: 0,
            vector_flags: 0,
            ..Default::default()
        };

        // VARCHAR -> CLOB fetches as LONG keeping the previous csfrm
        let mut current = column(ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT);
        assert!(adjust_refetch_metadata(
            &column(ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT),
            &mut current
        ));
        assert_eq!(current.ora_type_num, ORA_TYPE_NUM_LONG);
        assert_eq!(current.csfrm, CS_FORM_IMPLICIT);
        assert_eq!(current.buffer_size, TNS_MAX_LONG_LENGTH);
        assert_eq!(current.max_size, 0);

        // NVARCHAR -> NCLOB keeps the NCHAR character set form
        let mut current = column(ORA_TYPE_NUM_CLOB, CS_FORM_NCHAR);
        assert!(adjust_refetch_metadata(
            &column(ORA_TYPE_NUM_VARCHAR, CS_FORM_NCHAR),
            &mut current
        ));
        assert_eq!(current.ora_type_num, ORA_TYPE_NUM_LONG);
        assert_eq!(current.csfrm, CS_FORM_NCHAR);

        // RAW -> BLOB fetches as LONG RAW
        let mut current = column(ORA_TYPE_NUM_BLOB, 0);
        assert!(adjust_refetch_metadata(
            &column(ORA_TYPE_NUM_RAW, 0),
            &mut current
        ));
        assert_eq!(current.ora_type_num, ORA_TYPE_NUM_LONG_RAW);
        assert_eq!(current.csfrm, 0);

        // unrelated type changes are untouched
        let mut current = column(ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT);
        assert!(!adjust_refetch_metadata(
            &column(ORA_TYPE_NUM_NUMBER, 0),
            &mut current
        ));
        assert_eq!(current.ora_type_num, ORA_TYPE_NUM_CLOB);
        let mut current = column(ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT);
        assert!(!adjust_refetch_metadata(
            &column(ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT),
            &mut current
        ));
        assert_eq!(current.ora_type_num, ORA_TYPE_NUM_VARCHAR);
    }

    #[test]
    fn row_bind_metadata_keeps_raw_type_with_promoted_buffer_size() {
        // bytes values stay RAW regardless of size (reference
        // OracleMetadata.from_value never switches str/bytes binds to
        // LONG/LONG_RAW); only the buffer size grows to the largest row
        let rows = vec![
            vec![BindValue::Raw(vec![0; 25_000])],
            vec![BindValue::Raw(vec![0; 40_000])],
        ];
        let mut writer = TtcWriter::new();

        let (ora_type_num, csfrm, buffer_size) =
            write_bind_metadata_for_rows(&mut writer, &rows, 0).expect("metadata writes");

        assert_eq!(ora_type_num, ORA_TYPE_NUM_RAW);
        assert_eq!(csfrm, 0);
        assert_eq!(buffer_size, 40_000);
    }

    #[test]
    fn non_plsql_bind_rows_emit_long_values_last() {
        let row = vec![
            BindValue::Number("1".into()),
            BindValue::Raw(vec![0; 40_000]),
            BindValue::Number("8".into()),
            BindValue::Text("A".repeat(40_000)),
        ];
        let metadata = row.iter().map(bind_metadata).collect::<Vec<_>>();

        assert_eq!(
            bind_row_value_order(&row, &metadata, false),
            vec![0, 2, 1, 3]
        );
        assert_eq!(
            bind_row_value_order(&row, &metadata, true),
            vec![0, 1, 2, 3]
        );
    }

    #[test]
    fn lob_bind_metadata_sets_prefetch_continuation_flag() {
        let mut writer = TtcWriter::new();
        write_bind_metadata_with_type(
            &mut writer,
            &BindValue::Lob {
                ora_type_num: ORA_TYPE_NUM_CLOB,
                csfrm: CS_FORM_IMPLICIT,
                locator: vec![0; 40],
            },
            ORA_TYPE_NUM_CLOB,
            CS_FORM_IMPLICIT,
            1,
        )
        .expect("CLOB bind metadata should encode");
        let encoded = writer.into_bytes();
        let mut reader = TtcReader::new(&encoded);

        assert_eq!(reader.read_u8().expect("type"), ORA_TYPE_NUM_CLOB);
        reader.skip(3).expect("flags, precision, scale");
        assert_eq!(reader.read_ub4().expect("buffer size"), 1);
        assert_eq!(reader.read_ub4().expect("max elements"), 0);
        assert_eq!(
            reader.read_ub8().expect("cont flags"),
            TNS_LOB_PREFETCH_FLAG
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
    fn public_dbtype_names_from_column_metadata_preserve_fetch_semantics() {
        let mut metadata = number_column("VALUE");
        assert_eq!(
            public_dbtype_name_from_column_metadata(&metadata),
            "DB_TYPE_NUMBER"
        );

        metadata.ora_type_num = ORA_TYPE_NUM_CHAR;
        metadata.csfrm = CS_FORM_NCHAR;
        assert_eq!(
            public_dbtype_name_from_column_metadata(&metadata),
            "DB_TYPE_NCHAR"
        );

        metadata.ora_type_num = ORA_TYPE_NUM_VARCHAR;
        assert_eq!(
            public_dbtype_name_from_column_metadata(&metadata),
            "DB_TYPE_NVARCHAR"
        );

        metadata.ora_type_num = ORA_TYPE_NUM_CLOB;
        assert_eq!(
            public_dbtype_name_from_column_metadata(&metadata),
            "DB_TYPE_NCLOB"
        );

        metadata.csfrm = CS_FORM_IMPLICIT;
        for (ora_type_num, expected) in [
            (ORA_TYPE_NUM_LONG, "DB_TYPE_LONG"),
            (ORA_TYPE_NUM_LONG_RAW, "DB_TYPE_LONG_RAW"),
            (ORA_TYPE_NUM_ROWID, "DB_TYPE_ROWID"),
            (ORA_TYPE_NUM_UROWID, "DB_TYPE_UROWID"),
            (ORA_TYPE_NUM_TIMESTAMP, "DB_TYPE_TIMESTAMP"),
            (ORA_TYPE_NUM_TIMESTAMP_LTZ, "DB_TYPE_TIMESTAMP_LTZ"),
            (ORA_TYPE_NUM_TIMESTAMP_TZ, "DB_TYPE_TIMESTAMP_TZ"),
            (ORA_TYPE_NUM_BFILE, "DB_TYPE_BFILE"),
        ] {
            metadata.ora_type_num = ora_type_num;
            assert_eq!(public_dbtype_name_from_column_metadata(&metadata), expected);
        }

        metadata.ora_type_num = ORA_TYPE_NUM_OBJECT;
        metadata.object_schema = Some("SYS".into());
        metadata.object_type_name = Some("XMLTYPE".into());
        assert!(column_metadata_is_xmltype(&metadata));
        assert_eq!(
            public_dbtype_name_from_column_metadata(&metadata),
            "DB_TYPE_XMLTYPE"
        );
    }

    #[test]
    fn oracle_dictionary_type_metadata_is_protocol_owned() {
        assert_eq!(
            public_dbtype_name_from_oracle_type_name("timestamp with local time zone"),
            "DB_TYPE_TIMESTAMP_LTZ"
        );
        assert_eq!(
            public_dbtype_name_from_oracle_type_name("TIMESTAMP WITH TZ"),
            "DB_TYPE_TIMESTAMP_TZ"
        );
        assert_eq!(
            public_dbtype_name_from_oracle_type_name("BINARY_FLOAT"),
            "DB_TYPE_BINARY_FLOAT"
        );
        assert_eq!(
            public_dbtype_name_from_oracle_type_name("UDT_OBJECT"),
            "DB_TYPE_OBJECT"
        );
        // PL/SQL scalar attribute/element type names must NOT fall through to
        // the DB_TYPE_OBJECT ADT fallback (Wave 3 BUG 1).
        for (name, expected) in [
            ("BOOLEAN", "DB_TYPE_BOOLEAN"),
            ("PL/SQL BOOLEAN", "DB_TYPE_BOOLEAN"),
            ("PL/SQL PLS INTEGER", "DB_TYPE_BINARY_INTEGER"),
            ("PL/SQL BINARY INTEGER", "DB_TYPE_BINARY_INTEGER"),
            ("BINARY_INTEGER", "DB_TYPE_BINARY_INTEGER"),
            ("PLS_INTEGER", "DB_TYPE_BINARY_INTEGER"),
            ("INTERVAL DAY TO SECOND", "DB_TYPE_INTERVAL_DS"),
            ("INTERVAL YEAR TO MONTH", "DB_TYPE_INTERVAL_YM"),
        ] {
            assert_eq!(public_dbtype_name_from_oracle_type_name(name), expected);
        }

        assert_eq!(
            dbobject_attr_precision_scale("NUMBER", None, Some(0)),
            (38, 0)
        );
        assert_eq!(
            dbobject_attr_precision_scale("NUMBER", None, None),
            (0, -127)
        );
        assert_eq!(
            dbobject_attr_precision_scale("DOUBLE PRECISION", None, None),
            (126, -127)
        );
        assert_eq!(dbobject_attr_max_size("NVARCHAR2", Some(10)), 20);
        assert_eq!(
            dbobject_rowtype_attr_max_size("NVARCHAR2", Some(40), Some(7)),
            14
        );
        assert_eq!(
            dbobject_rowtype_attr_max_size("NVARCHAR2", Some(40), Some(0)),
            80
        );
        assert_eq!(dbobject_rowtype_attr_max_size("NUMBER", Some(22), None), 0);
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
            bind_template_from_type_name("DB_TYPE_BLOB", 0),
            BindValue::TypedNull {
                ora_type_num: ORA_TYPE_NUM_LONG_RAW,
                csfrm: 0,
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

    #[test]
    fn dbobject_packed_reader_decodes_header_lengths_and_nulls() {
        let bytes = [
            TNS_OBJ_NO_PREFIX_SEG,
            1,
            0,
            4,
            b't',
            b'e',
            b's',
            b't',
            TNS_OBJ_ATOMIC_NULL,
        ];
        let mut reader = DbObjectPackedReader::new(&bytes);
        reader.read_header().expect("header should decode");
        assert_eq!(
            reader
                .read_value_bytes()
                .expect("value bytes should decode"),
            Some(b"test".to_vec())
        );
        assert!(reader
            .read_atomic_null(false)
            .expect("atomic null should decode"));
    }

    #[test]
    fn dbobject_scalar_decoders_match_oracle_canonical_data() {
        assert_eq!(
            decode_dbobject_text(&[0, b'A'], "DB_TYPE_NCHAR").expect("nchar text"),
            "A"
        );
        assert_eq!(
            decode_dbobject_xmltype_text(&[
                TNS_OBJ_NO_PREFIX_SEG,
                1,
                0,
                0,
                0,
                0,
                0,
                TNS_XML_TYPE_STRING as u8,
                b'<',
                b'x',
                b'/',
                b'>',
            ])
            .expect("XMLTYPE text should decode"),
            Some("<x/>".to_string())
        );
        assert_eq!(
            decode_dbobject_binary_float(&[0xbf, 0x80, 0, 0]).expect("binary float"),
            1.0
        );
        assert_eq!(
            decode_dbobject_binary_double(&[0xbf, 0xf0, 0, 0, 0, 0, 0, 0]).expect("binary double"),
            1.0
        );
    }

    #[test]
    fn lob_text_encoding_uses_csfrm_and_locator_flags() {
        assert_eq!(
            decode_lob_text(b"Plain", CS_FORM_IMPLICIT, None).expect("utf8 lob"),
            "Plain"
        );
        assert_eq!(
            encode_lob_text("Text", CS_FORM_IMPLICIT, None),
            b"Text".to_vec()
        );
        assert_eq!(
            encode_lob_text("AB", CS_FORM_NCHAR, None),
            vec![0, b'A', 0, b'B']
        );
        assert_eq!(
            decode_lob_text(&[0, b'A', 0, b'B'], CS_FORM_NCHAR, None).expect("nchar lob"),
            "AB"
        );

        let mut locator = vec![0; 8];
        locator[TNS_LOB_LOC_OFFSET_FLAG_3] = TNS_LOB_LOC_FLAGS_VAR_LENGTH_CHARSET;
        locator[TNS_LOB_LOC_OFFSET_FLAG_4] = TNS_LOB_LOC_FLAGS_LITTLE_ENDIAN;
        assert_eq!(
            encode_lob_text("AB", CS_FORM_IMPLICIT, Some(&locator)),
            vec![b'A', 0, b'B', 0]
        );
        assert_eq!(
            decode_lob_text(&[b'A', 0, b'B', 0], CS_FORM_IMPLICIT, Some(&locator))
                .expect("locator utf16 lob"),
            "AB"
        );
    }

    #[test]
    fn bfile_locator_name_decodes_directory_and_file_tail() {
        let locator = Vec::from_hex(
            "0808000000010000000000000015544553545f313933365f4d495353494e475f444952\
             001a746573745f313933365f6d697373696e675f66696c652e747874",
        )
        .expect("BFILE locator fixture should be valid hex");

        assert_eq!(
            decode_bfile_locator_name(&locator),
            Some((
                "TEST_1936_MISSING_DIR".to_string(),
                "test_1936_missing_file.txt".to_string()
            ))
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
            vector_dimensions: None,
            vector_format: 0,
            vector_flags: 0,
            ..Default::default()
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
            vector_dimensions: None,
            vector_format: 0,
            vector_flags: 0,
            ..Default::default()
        }
    }
}
