//! Cross-connection statement-shape cache + DDL-invalidation self-heal
//! (bead a4-8pp), exercised through the public crate surface.
//!
//! This is the offline "scripted" form of the bead's acceptance test: two
//! logical connections share one [`StatementShapeCache`] (the exact way real
//! connections share it via
//! [`ConnectOptions::with_shared_statement_shape_cache`]), a concurrent DDL
//! changes a prepared query's described shape between the two, and we assert the
//! cache self-heals (re-describe) and never keeps the stale shape (no stale
//! decode). The shape metadata is scripted, so no live database is required.

use std::sync::Arc;

use oracledb::protocol::thin::{ColumnMetadata, ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_VARCHAR};
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ColumnShape, ConnectOptions, Connection, StatementShapeCache};

mod common;

fn col(name: &str, ty: u8, precision: i8, scale: i8, max_size: u32) -> ColumnMetadata {
    ColumnMetadata::new(name, ty)
        .with_precision(precision)
        .with_scale(scale)
        .with_max_size(max_size)
        .with_nulls_allowed(true)
}

// Pre-DDL describe of `SELECT ID, NAME FROM T`.
fn shape_v1() -> Vec<ColumnMetadata> {
    vec![
        col("ID", ORA_TYPE_NUM_NUMBER, 9, 0, 22),
        col("NAME", ORA_TYPE_NUM_VARCHAR, 0, 0, 50),
    ]
}

// Post-DDL describe: NAME widened and an EMAIL column added.
fn shape_v2() -> Vec<ColumnMetadata> {
    vec![
        col("ID", ORA_TYPE_NUM_NUMBER, 9, 0, 22),
        col("NAME", ORA_TYPE_NUM_VARCHAR, 0, 0, 200),
        col("EMAIL", ORA_TYPE_NUM_VARCHAR, 0, 0, 100),
    ]
}

#[test]
fn options_share_one_cache_across_connections() {
    // Two ConnectOptions built with the same shared cache hand back the very
    // same Arc, so every connection they open observes into one shared map.
    let shared = Arc::new(StatementShapeCache::new());
    let id = ClientIdentity::new("app", "host", "user", "term", "rust").expect("identity");
    let a = ConnectOptions::new("h:1521/svc", "u", "p", id.clone())
        .with_shared_statement_shape_cache(shared.clone());
    let b = ConnectOptions::new("h:1521/svc", "u", "p", id)
        .with_shared_statement_shape_cache(shared.clone());
    let a_cache = a.statement_shape_cache().expect("cache on a");
    let b_cache = b.statement_shape_cache().expect("cache on b");
    assert!(
        Arc::ptr_eq(a_cache, b_cache),
        "both options must reference the one shared cache"
    );
    assert!(Arc::ptr_eq(a_cache, &shared));
}

#[test]
fn default_options_have_no_shared_cache() {
    let id = ClientIdentity::new("app", "host", "user", "term", "rust").expect("identity");
    let opts = ConnectOptions::new("h:1521/svc", "u", "p", id);
    assert!(
        opts.statement_shape_cache().is_none(),
        "default keeps each connection's cache private"
    );
}

#[test]
fn prepared_reuse_across_connections_self_heals_on_concurrent_ddl() {
    // One shared cache stands in for two connections reusing the same prepared
    // statement while a DDL on a third session changes the shape between them.
    let shared = Arc::new(StatementShapeCache::new());
    let sql = "select id, name from t";

    // Connection A executes -> records pre-DDL shape v1.
    let a = shared.observe(sql, &shape_v1());
    assert!(a.first_seen);
    assert!(!a.self_healed);
    assert_eq!(a.generation, 1);

    // ---- concurrent DDL alters T here ----

    // Connection B executes the SAME statement; the server describes v2. The
    // shared cache self-heals: invalidate v1, adopt v2, bump the generation, and
    // demand a rebind so B re-describes instead of decoding against v1.
    let b = shared.observe(sql, &shape_v2());
    assert!(b.self_healed, "shape change must self-heal");
    assert!(b.requires_rebind());
    assert_eq!(b.generation, 2);

    // No stale decode: the cache now holds v2 only, never the stale v1.
    let (gen, shape) = shared.current(sql).expect("recorded");
    assert_eq!(gen, 2);
    assert_eq!(shape, ColumnShape::from_columns(&shape_v2()));
    assert_ne!(shape, ColumnShape::from_columns(&shape_v1()));

    // A re-executes post-DDL and now sees v2 too: stable, no second heal.
    let a2 = shared.observe(sql, &shape_v2());
    assert!(!a2.self_healed);
    assert_eq!(a2.generation, 2);
}

#[test]
fn self_heal_is_downward_only_and_generation_is_monotonic() {
    let shared = Arc::new(StatementShapeCache::new());
    let sql = "select id, name from t";
    shared.observe(sql, &shape_v1());
    assert!(shared.observe(sql, &shape_v2()).self_healed);
    // Flip back: heals to EXACTLY v1, not a widened union of v1+v2.
    let back = shared.observe(sql, &shape_v1());
    assert!(back.self_healed);
    assert_eq!(back.generation, 3, "generation only ever increases");
    let (_, shape) = shared.current(sql).unwrap();
    assert_eq!(shape, ColumnShape::from_columns(&shape_v1()));
    assert_eq!(shape.len(), 2, "no phantom unioned columns");
}

fn query_one_text(connection: &mut Connection, sql: &str) -> oracledb::Result<String> {
    BlockingConnection::query(connection, sql, ())?
        .one()?
        .get(0)
}

/// Live regression/assessment for c23g.11. It intentionally orders the work
/// so that connection A retains the pre-DDL metadata, connection B observes the
/// post-DDL shape and heals the shared cache, and only then does A reuse the
/// identical SQL. A stale per-connection shape would truncate or reject the
/// widened value returned by A's final query.
#[test]
#[ignore = "requires a gvenzl Oracle lane with PYO_TEST_* configured"]
fn live_cross_connection_ddl_widening_does_not_serve_a_stale_shape() {
    let Some(creds) = common::live_creds_opt() else {
        eprintln!("skipped: PYO_TEST_* not set");
        return;
    };

    let shared = Arc::new(StatementShapeCache::new());
    let identity_a =
        ClientIdentity::new("shape-cache-a", "host", "user", "term", "rust").expect("identity A");
    let identity_b =
        ClientIdentity::new("shape-cache-b", "host", "user", "term", "rust").expect("identity B");
    let options_a = ConnectOptions::new(
        creds.connect_string.clone(),
        creds.user.clone(),
        creds.password.clone(),
        identity_a,
    )
    .with_shared_statement_shape_cache(Arc::clone(&shared));
    let options_b =
        ConnectOptions::new(creds.connect_string, creds.user, creds.password, identity_b)
            .with_shared_statement_shape_cache(Arc::clone(&shared));
    let mut a = BlockingConnection::connect(options_a).expect("connect A");
    let mut b = BlockingConnection::connect(options_b).expect("connect B");
    let server_version = a.server_version().unwrap_or("unknown").to_string();
    let server_version_tuple = a.server_version_tuple();
    let table = format!("RUST_SHAPE_CACHE_DDL_{}", std::process::id());
    let sql = format!("select payload from {table} where id = 1");
    let short_value = "before-ddl";
    let widened_value = "after-ddl payload is longer than the original varchar2(12) shape";

    let _ = BlockingConnection::execute(&mut b, &format!("drop table {table} purge"), ());
    let outcome = (|| -> oracledb::Result<()> {
        BlockingConnection::execute(
            &mut b,
            &format!("create table {table} (id number primary key, payload varchar2(12))"),
            (),
        )?;
        BlockingConnection::execute(
            &mut b,
            &format!("insert into {table} (id, payload) values (1, :1)"),
            (short_value,),
        )?;
        BlockingConnection::commit(&mut b)?;

        assert_eq!(query_one_text(&mut a, &sql)?, short_value);
        assert_eq!(shared.current(&sql).expect("initial shape").0, 1);

        BlockingConnection::execute(
            &mut b,
            &format!("alter table {table} modify (payload varchar2(4000))"),
            (),
        )?;
        BlockingConnection::execute(
            &mut b,
            &format!("update {table} set payload = :1 where id = 1"),
            (widened_value,),
        )?;
        BlockingConnection::commit(&mut b)?;

        // B describes the widened column first, causing the shared cache to
        // self-heal while A still retains the old per-connection metadata.
        assert_eq!(query_one_text(&mut b, &sql)?, widened_value);
        let (generation, shape) = shared.current(&sql).expect("widened shape");
        assert_eq!(generation, 2, "DDL must advance the shared generation");
        assert_eq!(shape.len(), 1);

        // The stale-sensitive assertion: A must receive the entire widened
        // value after B performed the cross-connection self-heal.
        assert_eq!(query_one_text(&mut a, &sql)?, widened_value);
        eprintln!(
            "shape-cache DDL assessment: server_version={server_version} version_tuple={server_version_tuple:?} scenario=varchar2(12)->varchar2(4000) generation={generation} result=fresh"
        );
        Ok(())
    })();
    let cleanup = BlockingConnection::execute(&mut b, &format!("drop table {table} purge"), ());
    let close_a = BlockingConnection::close(a);
    let close_b = BlockingConnection::close(b);

    outcome.expect("DDL scenario must not serve a stale shape");
    cleanup.expect("drop assessment table");
    close_a.expect("close A");
    close_b.expect("close B");
}
