//! Live integration tests for thin-mode SODA against a real Oracle container.
//!
//! These are `#[ignore]`d by default; run with the lane container up and the
//! `PYO_TEST_*` env vars set (see scripts/container.sh):
//!
//! ```text
//! cargo test -p oracledb --features soda --test live_soda -- --ignored
//! ```
//!
//! They exercise the real round-trip: create a collection, insert JSON,
//! find by key and by QBE filter, count, replace, remove, index, drop — and
//! assert the actual JSON that came back from the database.

#![cfg(feature = "soda")]

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::soda::{SodaDatabase, SodaDocument, SodaError, SodaOperation};
use oracledb::{ConnectOptions, Connection};
use oracledb_protocol::oson::OsonValue;
use oracledb_protocol::ClientIdentity;

mod common;

fn connect_options() -> ConnectOptions {
    let identity = ClientIdentity::new(
        "rust-oracledb",
        "rusthost",
        "rustuser",
        "rustterm",
        "rust-oracledb thn : 0.0.0",
    )
    .expect("identity");
    ConnectOptions::new(
        common::live_conn_string_or("localhost:1523/FREEPDB1"),
        common::live_user_or(common::FREE23_USER),
        common::live_password_or(common::FREE23_PASSWORD),
        identity,
    )
}

/// Run an async test body on a fresh current-thread runtime + connection.
fn with_conn<F>(body: F)
where
    F: for<'a> FnOnce(
        &'a mut Connection,
        &'a Cx,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>,
{
    let reactor = reactor::create_reactor().expect("reactor");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let cx = Cx::current().expect("cx");
        let mut conn = Connection::connect(&cx, connect_options())
            .await
            .expect("connect");
        body(&mut conn, &cx).await;
        conn.close(&cx).await.expect("close");
    });
}

/// Build a document from a JSON object literal string.
fn json_doc(json: &str) -> SodaDocument {
    SodaDocument::from_bytes(json.as_bytes().to_vec(), None, None)
}

/// Extract a field from a document's decoded OSON content.
fn oson_get<'a>(doc: &'a SodaDocument, key: &str) -> Option<&'a OsonValue> {
    match doc.content_as_oson()? {
        OsonValue::Object(entries) => entries.iter().find(|(k, _)| k == key).map(|(_, v)| v),
        _ => None,
    }
}

fn oson_str<'a>(doc: &'a SodaDocument, key: &str) -> Option<&'a str> {
    match oson_get(doc, key)? {
        OsonValue::String(s) => Some(s.as_str()),
        _ => None,
    }
}

async fn drop_if_exists(db: &SodaDatabase, conn: &mut Connection, cx: &Cx, name: &str) {
    let _ = db.drop_collection(conn, cx, name).await;
    let _ = conn.commit(cx).await;
}

#[test]
#[ignore = "requires live Oracle container (lane-1523)"]
fn soda_create_open_drop_collection() {
    with_conn(|conn, cx| {
        Box::pin(async move {
            let db = SodaDatabase::new();
            drop_if_exists(&db, conn, cx, "RustSodaCreate").await;

            let coll = db
                .create_collection(conn, cx, Some("RustSodaCreate"), None, false)
                .await
                .expect("create");
            assert_eq!(coll.name(), "RustSodaCreate");
            assert_eq!(coll.metadata().table_name, "RustSodaCreate");

            // open returns the same collection
            let opened = db
                .open_collection(conn, cx, "RustSodaCreate")
                .await
                .expect("open ok")
                .expect("exists");
            assert_eq!(opened.name(), "RustSodaCreate");

            // drop returns true, then false (already gone)
            assert!(db
                .drop_collection(conn, cx, "RustSodaCreate")
                .await
                .expect("drop"));
            conn.commit(cx).await.expect("commit");
            assert!(!db
                .drop_collection(conn, cx, "RustSodaCreate")
                .await
                .expect("drop2"));

            // open of a non-existent collection -> None
            assert!(db
                .open_collection(conn, cx, "RustSodaNoSuchCollection")
                .await
                .expect("open none")
                .is_none());
        })
    });
}

#[test]
#[ignore = "requires live Oracle container (lane-1523)"]
fn soda_insert_and_find_by_key() {
    with_conn(|conn, cx| {
        Box::pin(async move {
            let db = SodaDatabase::new();
            drop_if_exists(&db, conn, cx, "RustSodaInsertFind").await;
            let coll = db
                .create_collection(conn, cx, Some("RustSodaInsertFind"), None, false)
                .await
                .expect("create");

            // insertOneAndGet returns a key
            let inserted = coll
                .insert_one(
                    conn,
                    cx,
                    &json_doc(r#"{"name":"George","age":47}"#),
                    None,
                    true,
                )
                .await
                .expect("insert")
                .expect("returned doc");
            let key = inserted.key.clone().expect("key");
            assert!(!key.is_empty(), "key should be non-empty");
            conn.commit(cx).await.expect("commit");

            // count == 1
            let count = coll
                .get_count(conn, cx, &SodaOperation::default())
                .await
                .expect("count");
            assert_eq!(count, 1);

            // find by key returns the JSON we inserted
            let op = SodaOperation {
                key: Some(key.clone()),
                ..Default::default()
            };
            let found = coll
                .get_one(conn, cx, &op)
                .await
                .expect("getOne")
                .expect("present");
            assert_eq!(oson_str(&found, "name"), Some("George"));
            assert!(matches!(
                oson_get(&found, "age"),
                Some(OsonValue::Number(n)) if n == "47"
            ));

            db.drop_collection(conn, cx, "RustSodaInsertFind")
                .await
                .ok();
            conn.commit(cx).await.ok();
        })
    });
}

#[test]
#[ignore = "requires live Oracle container (lane-1523)"]
fn soda_insert_many_and_qbe_filter() {
    with_conn(|conn, cx| {
        Box::pin(async move {
            let db = SodaDatabase::new();
            drop_if_exists(&db, conn, cx, "RustSodaQbe").await;
            let coll = db
                .create_collection(conn, cx, Some("RustSodaQbe"), None, false)
                .await
                .expect("create");

            let docs = vec![
                json_doc(r#"{"name":"John","age":22}"#),
                json_doc(r#"{"name":"Johnson","age":45}"#),
                json_doc(r#"{"name":"William","age":32}"#),
            ];
            coll.insert_many(conn, cx, &docs, None, false)
                .await
                .expect("insertMany");
            conn.commit(cx).await.expect("commit");

            // count all
            assert_eq!(
                coll.get_count(conn, cx, &SodaOperation::default())
                    .await
                    .expect("count all"),
                3
            );

            // age > 18 -> 3
            let op = SodaOperation {
                filter: Some(r#"{"age": {"$gt": 18}}"#.into()),
                ..Default::default()
            };
            assert_eq!(coll.get_count(conn, cx, &op).await.expect("count gt"), 3);

            // age < 25 -> 1
            let op = SodaOperation {
                filter: Some(r#"{"age": {"$lt": 25}}"#.into()),
                ..Default::default()
            };
            assert_eq!(coll.get_count(conn, cx, &op).await.expect("count lt"), 1);

            // name like J%n -> 2 (John, Johnson)
            let op = SodaOperation {
                filter: Some(r#"{"name": {"$like": "J%n"}}"#.into()),
                ..Default::default()
            };
            assert_eq!(coll.get_count(conn, cx, &op).await.expect("count like"), 2);

            // startsWith John -> 2
            let op = SodaOperation {
                filter: Some(r#"{"name": {"$startsWith": "John"}}"#.into()),
                ..Default::default()
            };
            assert_eq!(coll.get_count(conn, cx, &op).await.expect("count sw"), 2);

            // getDocuments with filter returns content
            let op = SodaOperation {
                filter: Some(r#"{"age": {"$lt": 25}}"#.into()),
                ..Default::default()
            };
            let found = coll.get_documents(conn, cx, &op).await.expect("getDocs");
            assert_eq!(found.len(), 1);
            assert_eq!(oson_str(&found[0], "name"), Some("John"));

            db.drop_collection(conn, cx, "RustSodaQbe").await.ok();
            conn.commit(cx).await.ok();
        })
    });
}

#[test]
#[ignore = "requires live Oracle container (lane-1523)"]
fn soda_replace_and_remove() {
    with_conn(|conn, cx| {
        Box::pin(async move {
            let db = SodaDatabase::new();
            drop_if_exists(&db, conn, cx, "RustSodaReplace").await;
            let coll = db
                .create_collection(conn, cx, Some("RustSodaReplace"), None, false)
                .await
                .expect("create");

            let inserted = coll
                .insert_one(
                    conn,
                    cx,
                    &json_doc(r#"{"name":"John","city":"Sydney"}"#),
                    None,
                    true,
                )
                .await
                .expect("insert")
                .expect("doc");
            let key = inserted.key.clone().expect("key");
            conn.commit(cx).await.expect("commit");

            // replaceOne by key
            let op = SodaOperation {
                key: Some(key.clone()),
                ..Default::default()
            };
            let (replaced, _) = coll
                .replace_one(
                    conn,
                    cx,
                    &op,
                    &json_doc(r#"{"name":"John","city":"Melbourne"}"#),
                    false,
                )
                .await
                .expect("replace");
            assert!(replaced);
            conn.commit(cx).await.expect("commit");

            // verify the new content
            let found = coll
                .get_one(conn, cx, &op)
                .await
                .expect("getOne")
                .expect("present");
            assert_eq!(oson_str(&found, "city"), Some("Melbourne"));

            // replaceOne with an unknown key -> false
            let bad = SodaOperation {
                key: Some("00DEADBEEF00DEADBEEF00DEAD".into()),
                ..Default::default()
            };
            let (replaced2, _) = coll
                .replace_one(conn, cx, &bad, &json_doc(r#"{"x":1}"#), false)
                .await
                .expect("replace bad");
            assert!(!replaced2);

            // remove by key
            let removed = coll.remove(conn, cx, &op).await.expect("remove");
            assert_eq!(removed, 1);
            conn.commit(cx).await.expect("commit");
            assert_eq!(
                coll.get_count(conn, cx, &SodaOperation::default())
                    .await
                    .expect("count after remove"),
                0
            );

            db.drop_collection(conn, cx, "RustSodaReplace").await.ok();
            conn.commit(cx).await.ok();
        })
    });
}

#[test]
#[ignore = "requires live Oracle container (lane-1523)"]
fn soda_truncate_and_index_and_names() {
    with_conn(|conn, cx| {
        Box::pin(async move {
            let db = SodaDatabase::new();
            for n in ["RustSodaT", "RustSodaA", "RustSodaB"] {
                drop_if_exists(&db, conn, cx, n).await;
            }

            // getCollectionNames ordering + start/limit
            db.create_collection(conn, cx, Some("RustSodaB"), None, false)
                .await
                .expect("b");
            db.create_collection(conn, cx, Some("RustSodaA"), None, false)
                .await
                .expect("a");
            let coll = db
                .create_collection(conn, cx, Some("RustSodaT"), None, false)
                .await
                .expect("t");
            conn.commit(cx).await.expect("commit");

            let names = db
                .get_collection_names(conn, cx, Some("RustSoda"), 0)
                .await
                .expect("names");
            assert!(names.contains(&"RustSodaA".to_string()));
            assert!(names.contains(&"RustSodaB".to_string()));
            // ascending order
            let a_pos = names.iter().position(|n| n == "RustSodaA").unwrap();
            let b_pos = names.iter().position(|n| n == "RustSodaB").unwrap();
            assert!(a_pos < b_pos);

            // insert + truncate
            for v in [r#"{"k":1}"#, r#"{"k":2}"#, r#"{"k":3}"#] {
                coll.insert_one(conn, cx, &json_doc(v), None, false)
                    .await
                    .expect("ins");
            }
            conn.commit(cx).await.expect("commit");
            assert_eq!(
                coll.get_count(conn, cx, &SodaOperation::default())
                    .await
                    .expect("count"),
                3
            );
            coll.truncate(conn, cx).await.expect("truncate");
            assert_eq!(
                coll.get_count(conn, cx, &SodaOperation::default())
                    .await
                    .expect("count2"),
                0
            );

            // functional index from a fields spec (works without Oracle Text)
            coll.create_index(
                conn,
                cx,
                r#"{"name":"rust_ix_1","fields":[{"path":"k","datatype":"number","order":"asc"}]}"#,
            )
            .await
            .expect("create_index");
            assert!(coll
                .drop_index(conn, cx, "rust_ix_1", false)
                .await
                .expect("drop_index"));

            for n in ["RustSodaT", "RustSodaA", "RustSodaB"] {
                db.drop_collection(conn, cx, n).await.ok();
            }
            conn.commit(cx).await.ok();
        })
    });
}

/// Proof-of-gate for SODA on pre-21c servers (bead a4-soda-pre21c / iec3.1.21).
///
/// Thin-mode SODA serializes documents with the `JSON_SERIALIZE` SQL function,
/// which only exists on 21c+ — that function is the real capability boundary.
/// (The `USER_SODA_COLLECTIONS` name resolves to a *public synonym* present
/// even on 18c, where it is selectable, so its catalog presence is NOT a usable
/// version signal. Verified live: the synonym is in `ALL_OBJECTS` and selectable
/// on 18c, 21c and 23ai alike; `ALL_VIEWS` misses it on every lane because it is
/// a synonym, not a view. The `JSON_SERIALIZE` function is what actually differs.)
///
/// Rather than silently `#[ignore]`-skipping SODA on old servers, this test
/// makes the gate an **active, documented assertion** that runs on every lane
/// and proves the capability boundary from both sides:
///
///   * `< 21c` (the xe18 lane): assert a direct `JSON_SERIALIZE` probe fails AND
///     that `create_collection` fails with ORA-00904 (`"JSON_SERIALIZE":
///     invalid identifier`). The capability is proven missing — an honest,
///     evidence-backed XFAIL, never a quiet skip.
///   * `>= 21c` (xe21 / free23): assert the `JSON_SERIALIZE` probe succeeds AND
///     that `create_collection` succeeds, then clean up.
///
/// This mirrors the `xe18:live_soda` gate reason in
/// `scripts/version_matrix.sh` (`suite_gate_reason`) but asserts it in-process
/// so the gate cannot rot into a silent pass. Run with the lane env set:
///
/// ```text
/// cargo test -p oracledb --features soda --test live_soda \
///   soda_gated_on_pre21c_with_proof -- --ignored --nocapture
/// ```
#[test]
#[ignore = "requires live Oracle container (xe18 proves the gate; xe21/free23 prove support)"]
fn soda_gated_on_pre21c_with_proof() {
    with_conn(|conn, cx| {
        Box::pin(async move {
            let major = conn
                .server_version_tuple()
                .expect("server version negotiated at connect")
                .0;

            // Capability probe: does the JSON_SERIALIZE SQL function resolve?
            // This is the real 21c+ boundary the thin SODA write path depends
            // on (unlike USER_SODA_COLLECTIONS, a synonym present on every lane).
            let probe = conn
                .query_one(
                    cx,
                    "select json_serialize('{\"a\":1}' returning varchar2) from dual",
                    (),
                )
                .await;

            let db = SodaDatabase::new();
            let create = db
                .create_collection(conn, cx, Some("RustSodaGateProbe"), None, false)
                .await;

            if major < 21 {
                // GATE branch: SODA genuinely unavailable — prove it, don't skip.
                let probe_err = probe.expect_err("JSON_SERIALIZE must not resolve on pre-21c");
                let err = create.expect_err("SODA create must fail on pre-21c");
                let SodaError::Driver(driver_err) = &err else {
                    panic!("expected a driver error carrying an ORA code, got: {err:?}");
                };
                assert_eq!(
                    driver_err.ora_code(),
                    Some(904),
                    "pre-21c SODA create must fail with ORA-00904 \
                     (JSON_SERIALIZE invalid identifier); got: {driver_err:?}"
                );
                eprintln!(
                    "[soda-gate] version={major}c GATED: JSON_SERIALIZE absent \
                     (probe -> {:?}), create_collection -> ORA-00904 \
                     (documented XFAIL, not skipped)",
                    probe_err.ora_code()
                );
            } else {
                // SUPPORT branch: SODA present — prove it works, then clean up.
                probe.expect("JSON_SERIALIZE must resolve on 21c+");
                let coll = create.expect("SODA create must succeed on 21c+");
                assert_eq!(coll.name(), "RustSodaGateProbe");
                db.drop_collection(conn, cx, "RustSodaGateProbe").await.ok();
                conn.commit(cx).await.ok();
                eprintln!(
                    "[soda-gate] version={major}c SUPPORTED: JSON_SERIALIZE + SODA available"
                );
            }
        })
    });
}
