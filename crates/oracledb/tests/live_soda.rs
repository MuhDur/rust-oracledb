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
use oracledb::soda::{SodaDatabase, SodaDocument, SodaOperation};
use oracledb::{ConnectOptions, Connection};
use oracledb_protocol::oson::OsonValue;
use oracledb_protocol::ClientIdentity;

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
        std::env::var("PYO_TEST_CONNECT_STRING")
            .unwrap_or_else(|_| "localhost:1523/FREEPDB1".to_string()),
        std::env::var("PYO_TEST_MAIN_USER").unwrap_or_else(|_| "pythontest".to_string()),
        std::env::var("PYO_TEST_MAIN_PASSWORD").unwrap_or_else(|_| "pythontest".to_string()),
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
                .create_collection(conn, cx, "RustSodaCreate", None, false)
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
                .create_collection(conn, cx, "RustSodaInsertFind", None, false)
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
                .create_collection(conn, cx, "RustSodaQbe", None, false)
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
                .create_collection(conn, cx, "RustSodaReplace", None, false)
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
            db.create_collection(conn, cx, "RustSodaB", None, false)
                .await
                .expect("b");
            db.create_collection(conn, cx, "RustSodaA", None, false)
                .await
                .expect("a");
            let coll = db
                .create_collection(conn, cx, "RustSodaT", None, false)
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
