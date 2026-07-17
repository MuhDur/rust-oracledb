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
use oracledb::soda::{SodaCollection, SodaDatabase, SodaDocument, SodaError, SodaOperation};
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

/// Extract a numeric field as its canonical string form (OSON numbers are
/// carried as decimal strings).
fn oson_str_num<'a>(doc: &'a SodaDocument, key: &str) -> Option<&'a str> {
    match oson_get(doc, key)? {
        OsonValue::Number(s) => Some(s.as_str()),
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
            let a_pos = names
                .iter()
                .position(|n| n == "RustSodaA")
                .expect("RustSodaA must be present after create_collection");
            let b_pos = names
                .iter()
                .position(|n| n == "RustSodaB")
                .expect("RustSodaB must be present after create_collection");
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
/// Rather than silently skipping SODA on old servers, this test makes the
/// typed `pre-21c-soda-unsupported` result an **active, documented assertion**
/// that runs on every lane and proves the capability boundary from both sides:
///
///   * `< 21c` (the xe18 lane): assert a direct `JSON_SERIALIZE` probe fails AND
///     that `create_collection` fails with ORA-00904 (`"JSON_SERIALIZE":
///     invalid identifier`). The capability is proven missing before the
///     matrix records its explicit `SKIP` reason — never a quiet skip.
///   * `>= 21c` (xe21 / free23): assert the `JSON_SERIALIZE` probe succeeds AND
///     that `create_collection` succeeds, then clean up.
///
/// This mirrors the `xe18:live_soda` gate reason in
/// `scripts/version_matrix.sh` (`suite_skip_reason`) but asserts it in-process
/// so the typed skip cannot rot into a silent pass. Run with the lane env set:
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
                // Typed-SKIP branch: SODA is unavailable — prove it first.
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
                    "[soda-gate] version={major}c SKIP reason=pre-21c-soda-unsupported: \
                     (probe -> {:?}), create_collection -> ORA-00904 \
                     (active capability proof passed)",
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

/// Thin-mode SODA breadth (bead a4-h74 / iec3.1.20): the read/write surface the
/// python-oracledb SODA suites (test_3300 collections, test_3400 documents)
/// exercise beyond the create/insert/find happy path — streaming cursors with a
/// server refill, `skip`/`limit` paging, multi-`keys` filters, per-document
/// metadata (version/timestamps) on the read path, and optimistic-locking
/// replace by version.
///
/// Gated at 21c+ (thin-mode SODA needs `JSON_SERIALIZE`; the pre-21c boundary is
/// proven by `soda_gated_on_pre21c_with_proof`). On an <21c lane this test
/// self-documents and returns rather than falsely passing. Run with a 21c+ lane:
///
/// ```text
/// cargo test -p oracledb --features soda --test live_soda \
///   soda_breadth_cursor_skip_limit_keys_and_metadata -- --ignored --nocapture
/// ```
#[test]
#[ignore = "requires live Oracle 21c+ container (xe21 / free23)"]
fn soda_breadth_cursor_skip_limit_keys_and_metadata() {
    with_conn(|conn, cx| {
        Box::pin(async move {
            let major = conn
                .server_version_tuple()
                .expect("server version negotiated at connect")
                .0;
            if major < 21 {
                eprintln!(
                    "[soda-breadth] version={major}c: thin-mode SODA unavailable \
                     (< 21c), breadth N/A — see soda_gated_on_pre21c_with_proof"
                );
                return;
            }

            let db = SodaDatabase::new();
            drop_if_exists(&db, conn, cx, "RustSodaBreadth").await;
            let coll = db
                .create_collection(conn, cx, Some("RustSodaBreadth"), None, false)
                .await
                .expect("create");

            // Seed 5 ordered docs, capturing their server-assigned keys.
            let mut keys = Vec::new();
            for n in 1..=5 {
                let doc = coll
                    .insert_one(
                        conn,
                        cx,
                        &json_doc(&format!(r#"{{"n":{n},"grp":"g"}}"#)),
                        None,
                        true,
                    )
                    .await
                    .expect("insert")
                    .expect("returned doc");
                keys.push(doc.key.clone().expect("key"));
            }
            conn.commit(cx).await.expect("commit");

            // (1) Streaming cursor: a small fetch batch (< row count) forces a
            // server-side refill through `fetch_more`. Ordered by `n` so the
            // sequence is deterministic across batches; every row must carry the
            // SODA-managed metadata columns.
            let ordered = SodaOperation {
                filter: Some(r#"{"$orderby":[{"path":"n","order":"asc"}]}"#.into()),
                fetch_array_size: 2,
                ..Default::default()
            };
            let mut cursor = coll
                .open_cursor(conn, cx, &ordered)
                .await
                .expect("open_cursor");
            // The default collection carries key + version columns; timestamp
            // columns are optional in the default metadata, so we only require
            // that whichever the collection exposes are populated uniformly.
            let mut seen = Vec::new();
            let mut lastmod_seen = 0usize;
            while let Some(doc) = cursor.next_doc(conn, cx).await.expect("next_doc") {
                assert!(doc.key.is_some(), "cursor doc must carry a key");
                assert!(doc.version.is_some(), "cursor doc must carry a version");
                if doc.last_modified.is_some() {
                    lastmod_seen += 1;
                }
                match oson_get(&doc, "n") {
                    Some(OsonValue::Number(s)) => seen.push(s.parse::<i64>().expect("n int")),
                    other => panic!("expected numeric n, got {other:?}"),
                }
            }
            assert!(
                lastmod_seen == 0 || lastmod_seen == seen.len(),
                "last_modified must be populated for all rows or none (collection metadata), \
                 got {lastmod_seen}/{}",
                seen.len()
            );
            assert_eq!(
                seen,
                vec![1, 2, 3, 4, 5],
                "cursor must stream all rows in order across multiple batches"
            );
            cursor.close(conn, cx).await.expect("close cursor");
            assert!(cursor.is_closed());
            assert!(
                cursor.next_doc(conn, cx).await.is_err(),
                "next_doc on a closed cursor must error"
            );

            // (2) skip + limit paging (deterministic via the same $orderby).
            let page_op = SodaOperation {
                filter: Some(r#"{"$orderby":[{"path":"n","order":"asc"}]}"#.into()),
                skip: Some(1),
                limit: Some(2),
                ..Default::default()
            };
            let page = coll
                .get_documents(conn, cx, &page_op)
                .await
                .expect("skip/limit page");
            let page_n: Vec<Option<&str>> = page.iter().map(|d| oson_str_num(d, "n")).collect();
            assert_eq!(
                page_n,
                vec![Some("2"), Some("3")],
                "skip=1 limit=2 must return the 2nd and 3rd docs"
            );

            // (3) multi-keys filter: count + fetch a specific subset.
            let keys_op = SodaOperation {
                keys: Some(vec![keys[0].clone(), keys[4].clone()]),
                ..Default::default()
            };
            assert_eq!(
                coll.get_count(conn, cx, &keys_op)
                    .await
                    .expect("count by keys"),
                2
            );
            assert_eq!(
                coll.get_documents(conn, cx, &keys_op)
                    .await
                    .expect("get by keys")
                    .len(),
                2
            );

            // (4) optimistic locking: replaceOne matching the current version
            // succeeds and rotates the version; replaying the now-stale version
            // no longer matches.
            let current = coll
                .get_one(
                    conn,
                    cx,
                    &SodaOperation {
                        key: Some(keys[0].clone()),
                        ..Default::default()
                    },
                )
                .await
                .expect("getOne")
                .expect("present");
            let version = current.version.clone().expect("version");
            let (replaced, _) = coll
                .replace_one(
                    conn,
                    cx,
                    &SodaOperation {
                        key: Some(keys[0].clone()),
                        version: Some(version.clone()),
                        ..Default::default()
                    },
                    &json_doc(r#"{"n":1,"grp":"g","v":2}"#),
                    false,
                )
                .await
                .expect("replace matching version");
            assert!(
                replaced,
                "replaceOne with the matching version must succeed"
            );
            conn.commit(cx).await.expect("commit");

            let (stale, _) = coll
                .replace_one(
                    conn,
                    cx,
                    &SodaOperation {
                        key: Some(keys[0].clone()),
                        version: Some(version),
                        ..Default::default()
                    },
                    &json_doc(r#"{"n":1,"grp":"g","v":3}"#),
                    false,
                )
                .await
                .expect("replace stale version");
            assert!(!stale, "replaceOne with a stale version must not match");

            // (5) remove by multi-keys.
            let removed = coll
                .remove(
                    conn,
                    cx,
                    &SodaOperation {
                        keys: Some(vec![keys[1].clone(), keys[2].clone()]),
                        ..Default::default()
                    },
                )
                .await
                .expect("remove by keys");
            assert_eq!(removed, 2);
            conn.commit(cx).await.expect("commit");
            assert_eq!(
                coll.get_count(conn, cx, &SodaOperation::default())
                    .await
                    .expect("count after remove"),
                3
            );

            db.drop_collection(conn, cx, "RustSodaBreadth").await.ok();
            conn.commit(cx).await.ok();
        })
    });
}

/// Count matches for a QBE `filter` string, panicking with the filter on error.
async fn qbe_count(coll: &SodaCollection, conn: &mut Connection, cx: &Cx, filter: &str) -> u64 {
    let op = SodaOperation {
        filter: Some(filter.to_string()),
        ..Default::default()
    };
    coll.get_count(conn, cx, &op)
        .await
        .unwrap_or_else(|e| panic!("count {filter}: {e:?}"))
}

/// QBE operator breadth (bead a4-h74 / iec3.1.20): the query-by-example surface
/// python-oracledb's SODA suite (test_3300 `find()`) exercises beyond the
/// `$gt`/`$lt`/`$like`/`$startsWith` already covered — comparison, negation,
/// regex/substring, case-folding, existence, nested paths, and logical
/// combinators. Gated at 21c+.
#[test]
#[ignore = "requires live Oracle 21c+ container (xe21 / free23)"]
fn soda_qbe_operator_breadth() {
    with_conn(|conn, cx| {
        Box::pin(async move {
            let major = conn
                .server_version_tuple()
                .expect("server version negotiated at connect")
                .0;
            if major < 21 {
                eprintln!("[soda-qbe] version={major}c: SODA unavailable (< 21c), N/A");
                return;
            }

            let db = SodaDatabase::new();
            drop_if_exists(&db, conn, cx, "RustSodaQbeOps").await;
            let coll = db
                .create_collection(conn, cx, Some("RustSodaQbeOps"), None, false)
                .await
                .expect("create");

            let docs = vec![
                json_doc(r#"{"name":"John","age":22,"address":{"city":"Sydney"},"active":true}"#),
                json_doc(
                    r#"{"name":"Johnson","age":45,"address":{"city":"Sydney"},"active":false}"#,
                ),
                json_doc(r#"{"name":"William","age":32,"address":{"city":"Perth"}}"#),
                json_doc(r#"{"name":"Anne","age":29,"address":{"city":"Perth"},"active":true}"#),
            ];
            coll.insert_many(conn, cx, &docs, None, false)
                .await
                .expect("insertMany");
            conn.commit(cx).await.expect("commit");

            // Comparison + equality.
            assert_eq!(
                qbe_count(&coll, conn, cx, r#"{"name":{"$eq":"John"}}"#).await,
                1
            );
            assert_eq!(qbe_count(&coll, conn, cx, r#"{"age":{"$ne":22}}"#).await, 3);
            assert_eq!(
                qbe_count(&coll, conn, cx, r#"{"age":{"$gte":32}}"#).await,
                2
            );
            assert_eq!(
                qbe_count(&coll, conn, cx, r#"{"age":{"$lte":29}}"#).await,
                2
            );

            // Regex + substring + case-folding.
            assert_eq!(
                qbe_count(&coll, conn, cx, r#"{"name":{"$regex":"^Jo"}}"#).await,
                2
            );
            assert_eq!(
                qbe_count(&coll, conn, cx, r#"{"name":{"$hasSubstring":"ohn"}}"#).await,
                2
            );
            assert_eq!(
                qbe_count(
                    &coll,
                    conn,
                    cx,
                    r#"{"name":{"$upper":{"$startsWith":"JO"}}}"#
                )
                .await,
                2
            );

            // Existence (William has no `active` field).
            assert_eq!(
                qbe_count(&coll, conn, cx, r#"{"active":{"$exists":true}}"#).await,
                3
            );
            assert_eq!(
                qbe_count(&coll, conn, cx, r#"{"active":{"$exists":false}}"#).await,
                1
            );

            // Negation.
            assert_eq!(
                qbe_count(&coll, conn, cx, r#"{"age":{"$not":{"$eq":22}}}"#).await,
                3
            );

            // Nested path.
            assert_eq!(
                qbe_count(&coll, conn, cx, r#"{"address.city":{"$eq":"Perth"}}"#).await,
                2
            );

            // Logical combinators.
            assert_eq!(
                qbe_count(
                    &coll,
                    conn,
                    cx,
                    r#"{"$and":[{"age":{"$gte":30}},{"address.city":{"$eq":"Sydney"}}]}"#
                )
                .await,
                1
            );
            assert_eq!(
                qbe_count(
                    &coll,
                    conn,
                    cx,
                    r#"{"$or":[{"name":{"$eq":"William"}},{"age":{"$lt":25}}]}"#
                )
                .await,
                2
            );
            assert_eq!(
                qbe_count(
                    &coll,
                    conn,
                    cx,
                    r#"{"$nor":[{"name":{"$eq":"William"}},{"age":{"$lt":25}}]}"#
                )
                .await,
                2
            );

            db.drop_collection(conn, cx, "RustSodaQbeOps").await.ok();
            conn.commit(cx).await.ok();
        })
    });
}

/// insertManyAndGet (bead a4-h74 / iec3.1.20): the batch-insert-and-return path
/// python-oracledb's test_3300 exercises — every returned document carries its
/// server-assigned key and version, and the keys round-trip to the stored docs.
/// Gated at 21c+.
#[test]
#[ignore = "requires live Oracle 21c+ container (xe21 / free23)"]
fn soda_insert_many_and_get_returns_keys_and_versions() {
    with_conn(|conn, cx| {
        Box::pin(async move {
            let major = conn
                .server_version_tuple()
                .expect("server version negotiated at connect")
                .0;
            if major < 21 {
                eprintln!("[soda-iman] version={major}c: SODA unavailable (< 21c), N/A");
                return;
            }

            let db = SodaDatabase::new();
            drop_if_exists(&db, conn, cx, "RustSodaInsManyGet").await;
            let coll = db
                .create_collection(conn, cx, Some("RustSodaInsManyGet"), None, false)
                .await
                .expect("create");

            let docs = vec![
                json_doc(r#"{"seq":1,"who":"a"}"#),
                json_doc(r#"{"seq":2,"who":"b"}"#),
                json_doc(r#"{"seq":3,"who":"c"}"#),
            ];
            let returned = coll
                .insert_many(conn, cx, &docs, None, true)
                .await
                .expect("insertManyAndGet")
                .expect("return_docs=true yields docs");
            conn.commit(cx).await.expect("commit");

            assert_eq!(returned.len(), 3, "one returned doc per input");
            let mut keys = Vec::new();
            for doc in &returned {
                let key = doc.key.clone().expect("returned doc carries a key");
                assert!(!key.is_empty(), "key must be non-empty");
                assert!(
                    doc.version.as_deref().is_some_and(|v| !v.is_empty()),
                    "returned doc carries a version"
                );
                keys.push(key);
            }
            assert_eq!(
                {
                    keys.sort();
                    keys.dedup();
                    keys.len()
                },
                3,
                "keys are distinct"
            );

            // The returned keys round-trip to the stored documents.
            let by_keys = SodaOperation {
                keys: Some(keys.clone()),
                ..Default::default()
            };
            assert_eq!(
                coll.get_count(conn, cx, &by_keys)
                    .await
                    .expect("count keys"),
                3
            );

            db.drop_collection(conn, cx, "RustSodaInsManyGet")
                .await
                .ok();
            conn.commit(cx).await.ok();
        })
    });
}
