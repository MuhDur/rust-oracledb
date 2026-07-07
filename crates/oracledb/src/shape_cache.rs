//! Cross-connection statement-shape cache with DDL-invalidation self-heal
//! (bead a4-8pp).
//!
//! The per-connection statement cache ([`crate::Connection`]'s
//! `statement_cache`) keys an *open server cursor* by exact SQL text on a single
//! connection. It has no way to notice that a *different* connection ran a DDL
//! that changed a query's described result-column shape. Two connections that
//! prepare and reuse the same `SELECT` can therefore drift: connection A cached
//! the pre-DDL shape, connection B sees the post-DDL shape, and nothing ties the
//! two together.
//!
//! [`StatementShapeCache`] is the missing cross-connection layer. It is shared
//! (behind an [`std::sync::Arc`]) by every connection that opts in via
//! [`crate::ConnectOptions::with_shared_statement_shape_cache`], and it records
//! one [`ColumnShape`] fingerprint per normalized SQL. Each freshly-described
//! result is [`observe`](StatementShapeCache::observe)d into it:
//!
//! * first observation  -> record the shape (generation 1),
//! * same shape         -> no change,
//! * **different shape** -> **self-heal**: invalidate the stale record, adopt
//!   the fresh shape, and bump the generation.
//!
//! The self-heal is strictly *downward*: it only ever discards a stale record
//! and replaces it with the freshly-described one. It never merges, widens, or
//! unions shapes, so a caller can never end up decoding against a shape looser
//! than what the server actually described. Decoding itself always uses the
//! live describe from the current round trip; the cache's job is to *detect*
//! drift so a connection drops any retained (now-stale) per-SQL decode plan and
//! re-describes instead of serving a stale decode.
//!
//! Pure and synchronous: no I/O, no `await`, and the lock is never held across a
//! suspend point, so it cannot deadlock the async runtime.

use std::collections::HashMap;
use std::sync::Mutex;

use oracledb_protocol::thin::ColumnMetadata;

/// The decode-relevant fingerprint of one described column. Two columns with
/// equal fingerprints decode identically; any difference is a shape change.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ColumnShapeEntry {
    name: String,
    ora_type_num: u8,
    csfrm: u8,
    precision: i8,
    scale: i8,
    max_size: u32,
    nulls_allowed: bool,
    vector_format: u8,
    vector_flags: u8,
    vector_dimensions: Option<u32>,
}

impl ColumnShapeEntry {
    fn from_column(column: &ColumnMetadata) -> Self {
        Self {
            name: column.name().to_string(),
            ora_type_num: column.ora_type_num(),
            csfrm: column.csfrm(),
            precision: column.precision(),
            scale: column.scale(),
            max_size: column.max_size(),
            nulls_allowed: column.nulls_allowed(),
            vector_format: column.vector_format(),
            vector_flags: column.vector_flags(),
            vector_dimensions: column.vector_dimensions(),
        }
    }
}

/// A fingerprint of a query's described result-column shape: the decode-relevant
/// fields of every column, in select-list order. Equality means "decodes the
/// same"; inequality means the server's shape changed (typically a concurrent
/// DDL) and any cached decode plan for that SQL is stale.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ColumnShape {
    columns: Vec<ColumnShapeEntry>,
}

impl ColumnShape {
    /// Builds the fingerprint from a fresh describe's column metadata.
    pub fn from_columns(columns: &[ColumnMetadata]) -> Self {
        Self {
            columns: columns.iter().map(ColumnShapeEntry::from_column).collect(),
        }
    }

    /// Number of columns in the described shape.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether the shape has no columns (a non-query / DML execute).
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

/// Outcome of observing a freshly-described shape for a SQL statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShapeObservation {
    /// Per-key generation counter. Starts at 1 on first sight and increments by
    /// exactly one on every self-heal; it is monotonic and never decreases, so a
    /// caller can use it as a version stamp for a cached decode plan.
    pub generation: u64,
    /// True when the observed shape differed from the cached one, so the cache
    /// invalidated the stale record and adopted the fresh shape. Self-heal only
    /// ever heals *down* (invalidate + re-record); it never loosens.
    pub self_healed: bool,
    /// True only for the very first observation of this key (no prior shape).
    pub first_seen: bool,
}

impl ShapeObservation {
    /// Whether the caller must re-describe / drop any retained decode plan for
    /// this SQL before decoding (true exactly when a cross-connection shape
    /// change was just healed).
    pub fn requires_rebind(&self) -> bool {
        self.self_healed
    }
}

#[derive(Clone, Debug)]
struct CachedShape {
    shape: ColumnShape,
    generation: u64,
}

/// A cross-connection cache of statement result-column shapes, keyed by
/// normalized SQL text, with DDL-invalidation self-heal. Share one instance
/// across connections via
/// [`crate::ConnectOptions::with_shared_statement_shape_cache`].
#[derive(Debug, Default)]
pub struct StatementShapeCache {
    inner: Mutex<HashMap<String, CachedShape>>,
}

impl StatementShapeCache {
    /// Creates an empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records the freshly-described `columns` for `sql` and reports whether the
    /// shape changed since the last observation.
    ///
    /// An empty column list (a DML / non-query execute) is ignored: it never
    /// overwrites a real recorded shape, and reports `first_seen == false`,
    /// `self_healed == false`. On a real shape change the stale record is
    /// invalidated and replaced with the fresh shape (self-heal down only), and
    /// the generation is bumped.
    pub fn observe(&self, sql: &str, columns: &[ColumnMetadata]) -> ShapeObservation {
        let shape = ColumnShape::from_columns(columns);
        if shape.is_empty() {
            // Never let a non-query overwrite a recorded query shape.
            return ShapeObservation {
                generation: 0,
                self_healed: false,
                first_seen: false,
            };
        }
        let key = normalize_sql(sql);
        // Scope the lock: compute the observation, then drop the guard before
        // emitting the (feature-gated) telemetry so no tracing work runs under
        // the mutex.
        let observation = {
            let mut map = self.lock();
            match map.get_mut(&key) {
                None => {
                    map.insert(
                        key,
                        CachedShape {
                            shape,
                            generation: 1,
                        },
                    );
                    ShapeObservation {
                        generation: 1,
                        self_healed: false,
                        first_seen: true,
                    }
                }
                Some(entry) if entry.shape == shape => ShapeObservation {
                    generation: entry.generation,
                    self_healed: false,
                    first_seen: false,
                },
                Some(entry) => {
                    // Shape drift (concurrent DDL). Self-heal: discard the stale
                    // record, adopt the fresh shape, bump the generation. This
                    // only ever replaces downward; it never merges the two shapes.
                    entry.shape = shape;
                    entry.generation += 1;
                    ShapeObservation {
                        generation: entry.generation,
                        self_healed: true,
                        first_seen: false,
                    }
                }
            }
        };
        // Operator-facing cache-lookup span (feature-gated, zero-cost when off):
        // an enum-kind outcome + the version stamp. NEVER the SQL text — only the
        // shape's cross-connection lifecycle. miss/hit/self_heal are the
        // aggregatable counters an operator watches for warm-up and DDL-drift
        // behaviour. The field expressions are not evaluated in the default build.
        let _span = obs_span!(
            "oracledb.shape_cache",
            db.cache_event = if observation.first_seen {
                "miss"
            } else if observation.self_healed {
                "self_heal"
            } else {
                "hit"
            },
            db.cache_generation = observation.generation,
        );
        observation
    }

    /// Returns the currently-recorded `(generation, shape)` for `sql`, if any.
    pub fn current(&self, sql: &str) -> Option<(u64, ColumnShape)> {
        let key = normalize_sql(sql);
        let map = self.lock();
        map.get(&key)
            .map(|entry| (entry.generation, entry.shape.clone()))
    }

    /// Drops the recorded shape for `sql` (e.g. after a DDL is issued on this
    /// connection). A subsequent [`observe`](Self::observe) re-records it as a
    /// first sight. Returns whether an entry existed.
    pub fn invalidate(&self, sql: &str) -> bool {
        let key = normalize_sql(sql);
        let existed = self.lock().remove(&key).is_some();
        // Operator-facing invalidation span (feature-gated, zero-cost when off):
        // only the fact of an invalidation + whether an entry was present. NEVER
        // the SQL text.
        let _span = obs_span!(
            "oracledb.shape_cache",
            db.cache_event = "invalidate",
            db.cache_existed = existed,
        );
        existed
    }

    /// Number of distinct statements currently tracked.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the cache tracks no statements.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, CachedShape>> {
        // A poisoned lock only means a prior holder panicked mid-update; the map
        // itself stays structurally valid, so recover the guard rather than
        // propagating the panic and killing an otherwise-healthy connection.
        self.inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

/// Normalizes SQL text into the cache key: trims, and collapses every run of
/// ASCII whitespace *outside* string/quoted-identifier literals into a single
/// space. Whitespace inside `'...'` and `"..."` is preserved verbatim (it is
/// semantically significant), and the literal delimiters themselves are kept, so
/// two statements that differ only in incidental spacing/newlines share a key
/// while statements that differ in a literal do not.
///
/// This deliberately does NOT change case: Oracle string literals are
/// case-sensitive, and quoted identifiers are too, so upper-casing could
/// conflate genuinely different statements.
pub fn normalize_sql(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut pending_space = false;
    // Skip leading whitespace.
    while let Some(&c) = chars.peek() {
        if c.is_ascii_whitespace() {
            chars.next();
        } else {
            break;
        }
    }
    while let Some(c) = chars.next() {
        if c == '\'' || c == '"' {
            // Copy the literal verbatim, including its whitespace, up to and
            // including the matching close quote (doubled quotes are an escaped
            // quote inside the literal, not a terminator).
            if pending_space {
                out.push(' ');
                pending_space = false;
            }
            let quote = c;
            out.push(quote);
            while let Some(inner) = chars.next() {
                out.push(inner);
                if inner == quote {
                    if chars.peek() == Some(&quote) {
                        // Escaped quote: consume and keep both, stay in-literal.
                        out.push(quote);
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
        } else if c.is_ascii_whitespace() {
            // Collapse a run of whitespace into a single deferred space, emitted
            // only once a non-whitespace char follows (so trailing space is
            // dropped).
            pending_space = true;
        } else {
            if pending_space {
                out.push(' ');
                pending_space = false;
            }
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use oracledb_protocol::thin::{ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_VARCHAR};

    fn col(name: &str, ty: u8, precision: i8, scale: i8, max_size: u32) -> ColumnMetadata {
        ColumnMetadata::new(name, ty)
            .with_precision(precision)
            .with_scale(scale)
            .with_max_size(max_size)
            .with_nulls_allowed(true)
    }

    // The v1 shape of `SELECT ID, NAME FROM T`: (NUMBER(9,0), VARCHAR2(50)).
    fn shape_v1() -> Vec<ColumnMetadata> {
        vec![
            col("ID", ORA_TYPE_NUM_NUMBER, 9, 0, 22),
            col("NAME", ORA_TYPE_NUM_VARCHAR, 0, 0, 50),
        ]
    }

    // The post-DDL v2 shape: NAME widened, and a third column added.
    fn shape_v2() -> Vec<ColumnMetadata> {
        vec![
            col("ID", ORA_TYPE_NUM_NUMBER, 9, 0, 22),
            col("NAME", ORA_TYPE_NUM_VARCHAR, 0, 0, 200),
            col("EMAIL", ORA_TYPE_NUM_VARCHAR, 0, 0, 100),
        ]
    }

    #[test]
    fn first_observation_records_shape_without_self_heal() {
        let cache = StatementShapeCache::new();
        let obs = cache.observe("select id, name from t", &shape_v1());
        assert!(obs.first_seen);
        assert!(!obs.self_healed);
        assert_eq!(obs.generation, 1);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn identical_reobservation_is_stable() {
        let cache = StatementShapeCache::new();
        cache.observe("select id, name from t", &shape_v1());
        let obs = cache.observe("select id, name from t", &shape_v1());
        assert!(!obs.first_seen);
        assert!(!obs.self_healed);
        assert_eq!(obs.generation, 1, "same shape must not bump the generation");
    }

    #[test]
    fn cross_connection_ddl_shape_change_self_heals_and_never_serves_stale() {
        // One shared cache, two logical connections. This is the bead scenario:
        // prepared-statement reuse across connections WHILE a concurrent DDL
        // changes the described shape.
        let shared = StatementShapeCache::new();
        let sql = "select id, name from t";

        // Connection A prepares/executes: records the pre-DDL shape v1.
        let a = shared.observe(sql, &shape_v1());
        assert!(a.first_seen && !a.self_healed);

        // ... a concurrent DDL on another session alters T ...

        // Connection B executes the SAME prepared SQL and the server now
        // describes v2. The shared cache must self-heal: invalidate v1, adopt
        // v2, bump the generation, and signal a rebind.
        let b = shared.observe(sql, &shape_v2());
        assert!(b.self_healed, "shape change must self-heal");
        assert!(b.requires_rebind());
        assert_eq!(
            b.generation, 2,
            "self-heal bumps the generation exactly once"
        );

        // The cache now holds v2, NOT the stale v1: a decode plan taken from the
        // cache can never be the stale shape (no stale decode).
        let (gen, shape) = shared.current(sql).expect("recorded");
        assert_eq!(gen, 2);
        assert_eq!(shape, ColumnShape::from_columns(&shape_v2()));
        assert_ne!(
            shape,
            ColumnShape::from_columns(&shape_v1()),
            "the stale v1 shape must be gone after self-heal"
        );

        // Connection A re-executes post-DDL and also sees v2: now stable, no
        // second heal (the cache already healed down to v2).
        let a2 = shared.observe(sql, &shape_v2());
        assert!(!a2.self_healed);
        assert_eq!(a2.generation, 2);
    }

    #[test]
    fn self_heal_only_ever_heals_down_never_loosens() {
        // Flip-flopping shapes each heal to EXACTLY the just-observed shape and
        // bump the generation monotonically; the cache never accumulates a union
        // (a "looser" shape) of the two.
        let cache = StatementShapeCache::new();
        let sql = "select id, name from t";
        cache.observe(sql, &shape_v1());
        let to_v2 = cache.observe(sql, &shape_v2());
        assert!(to_v2.self_healed);
        let (_, after_v2) = cache.current(sql).unwrap();
        assert_eq!(after_v2, ColumnShape::from_columns(&shape_v2()));

        let back_to_v1 = cache.observe(sql, &shape_v1());
        assert!(back_to_v1.self_healed);
        assert_eq!(back_to_v1.generation, 3, "generation is monotonic");
        let (_, after_back) = cache.current(sql).unwrap();
        assert_eq!(
            after_back,
            ColumnShape::from_columns(&shape_v1()),
            "healed exactly to v1, not a v1+v2 union"
        );
        assert_eq!(after_back.len(), 2, "no phantom widened/unioned columns");
    }

    #[test]
    fn precision_or_scale_change_alone_triggers_self_heal() {
        // Same column names/types but a changed NUMBER precision is still a
        // decode-relevant shape change.
        let cache = StatementShapeCache::new();
        let sql = "select amount from t";
        cache.observe(sql, &[col("AMOUNT", ORA_TYPE_NUM_NUMBER, 9, 2, 22)]);
        let obs = cache.observe(sql, &[col("AMOUNT", ORA_TYPE_NUM_NUMBER, 12, 4, 22)]);
        assert!(obs.self_healed, "precision/scale change must self-heal");
    }

    #[test]
    fn empty_shape_never_overwrites_a_recorded_query_shape() {
        // A DML execute of the same text (no result columns) must not clobber a
        // recorded query shape or count as a heal.
        let cache = StatementShapeCache::new();
        let sql = "select id, name from t";
        cache.observe(sql, &shape_v1());
        let dml = cache.observe(sql, &[]);
        assert!(!dml.self_healed);
        assert!(!dml.first_seen);
        let (gen, shape) = cache.current(sql).unwrap();
        assert_eq!(gen, 1);
        assert_eq!(shape, ColumnShape::from_columns(&shape_v1()));
    }

    #[test]
    fn normalize_collapses_incidental_whitespace_outside_literals() {
        assert_eq!(
            normalize_sql("  select   id,\n\t name   from t  "),
            "select id, name from t"
        );
        // Whitespace inside a string literal is preserved; spacing outside is not.
        assert_eq!(
            normalize_sql("select 'a   b'   from   dual"),
            "select 'a   b' from dual"
        );
        // Two statements differing only in spacing share a key.
        assert_eq!(
            normalize_sql("select 1 from dual"),
            normalize_sql("select 1  from\tdual")
        );
        // A difference inside a literal is preserved (distinct keys).
        assert_ne!(
            normalize_sql("select 'x' from dual"),
            normalize_sql("select 'y' from dual")
        );
    }

    #[test]
    fn normalize_preserves_escaped_quote_inside_literal() {
        // A doubled quote is an escaped quote inside the literal, not a close.
        assert_eq!(
            normalize_sql("select 'it''s   here'  from dual"),
            "select 'it''s   here' from dual"
        );
    }

    #[test]
    fn whitespace_only_difference_shares_cache_entry() {
        let cache = StatementShapeCache::new();
        cache.observe("select id, name from t", &shape_v1());
        // Same statement, different incidental spacing -> same key, no heal.
        let obs = cache.observe("select   id,  name  from   t", &shape_v1());
        assert!(
            !obs.first_seen,
            "whitespace variant must hit the same entry"
        );
        assert!(!obs.self_healed);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn invalidate_forces_first_seen_again() {
        let cache = StatementShapeCache::new();
        let sql = "select id, name from t";
        cache.observe(sql, &shape_v1());
        assert!(cache.invalidate(sql));
        let obs = cache.observe(sql, &shape_v1());
        assert!(obs.first_seen);
        assert_eq!(obs.generation, 1);
    }
}
