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
use oracledb::{ColumnShape, ConnectOptions, StatementShapeCache};

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
