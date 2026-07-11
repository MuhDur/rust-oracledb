# Thin-mode SODA (Simple Oracle Document Access)

> **Honest framing:** this is **experimental thin-mode SODA**, gated behind the
> `soda` crate feature. python-oracledb's thin mode supports **zero** SODA —
> every SODA call raises `DPY-3001` (see below). This driver implements SODA
> directly over the thin TTC wire protocol. It is **not** "production-ready" and
> **not** full parity with thick-mode SODA; it is a genuine capability
> python-oracledb thin does not have, with an explicitly documented gap list at
> the end of this file.

## What SODA is

SODA (Simple Oracle Document Access) is Oracle's document-store API: you work
with **collections** of JSON **documents** — insert, find by key or by a
query-by-example (QBE) filter, replace, remove, index — without writing SQL.
Under the covers a collection is an ordinary table (a key column, a JSON/BLOB
content column, and optional version/timestamp/media-type columns) driven
through `DBMS_SODA` PL/SQL and generated SQL. It is Oracle's answer to a
MongoDB-style document workflow while keeping everything inside the database.

## Why thin-mode SODA is the differentiator

python-oracledb ships two modes: **thin** (pure Python, no Oracle Client) and
**thick** (requires the Oracle Instant Client / OCI libraries). Its SODA
support lives **only** in thick mode. In thin mode every SODA entry point is a
stub that raises the same error — verified in the reference source
(`reference/python-oracledb/src/oracledb/impl/base/soda.pyx`), whose
`BaseSodaDbImpl` / `BaseSodaCollImpl` methods each call
`errors._raise_not_supported(...)`:

```
DPY-3001: <feature> is only supported in python-oracledb thick mode
```

(e.g. `creating a SODA collection is only supported in python-oracledb thick
mode`). So to use SODA with python-oracledb you must install and load the
Oracle Client libraries.

This crate does not. SODA is fundamentally a management layer over standard SQL
and `DBMS_SODA` PL/SQL — all of which the thin TTC protocol already runs. The
implementation (`crates/oracledb/src/soda/`, feature `soda`) generates the SQL
the thick OCI client would have generated internally and runs it through the
existing `Connection` execute/fetch surface plus the OSON/JSON codecs in
`oracledb-protocol`. No new wire-protocol work, no C client. The result is
document access from a **pure-Rust thin driver** where python-oracledb thin has
none.

## Enabling it

```toml
[dependencies]
oracledb = { version = "*", features = ["soda"] }
```

Everything below lives under `oracledb::soda`. All collection/database
operations are `async` and, like the rest of this driver, take an explicit
`&mut Connection` and the Asupersync context `&Cx`.

## Supported API surface

The public types are `SodaDatabase`, `SodaCollection`, `SodaDocument`,
`SodaOperation`, `SodaCursor`, `SodaCollectionMetadata` (+ the `KeyAssignment`
/ `ContentSqlType` / `VersionMethod` descriptor enums), and the error type
`SodaError` / alias `SodaResult`.

### Collections — create / open / list / drop

`SodaDatabase` is a zero-sized facade; every method borrows the connection.

```rust
use oracledb::soda::SodaDatabase;

let db = SodaDatabase::new();

// create (or open, if it already exists with matching metadata).
// args: name, metadata JSON (None = default native collection), map_mode.
let coll = db.create_collection(conn, cx, Some("customers"), None, false).await?;

// open an existing collection; None if it does not exist.
let maybe = db.open_collection(conn, cx, "customers").await?; // Option<SodaCollection>

// list names (alphabetical); start_name filter + limit (0 = no limit).
let names = db.get_collection_names(conn, cx, Some("cust"), 0).await?; // Vec<String>

// drop; returns whether it existed and was dropped.
let dropped: bool = db.drop_collection(conn, cx, "customers").await?;
```

### Documents

A `SodaDocument` carries content plus the SODA-managed metadata (`key`,
`version`, `created_on`, `last_modified`, `media_type`). Build one for writing
from raw bytes or from a decoded OSON value:

```rust
use oracledb::soda::SodaDocument;

// from raw JSON bytes (the server parses and VALIDATES the JSON):
let doc = SodaDocument::from_bytes(br#"{"name":"George","age":47}"#.to_vec(), None, None);

// from a decoded OSON value, optionally with a client key:
// let doc = SodaDocument::from_oson(oson_value, None);
```

On the read path, inspect the content and metadata:

```rust
doc.content_as_oson();  // Option<&OsonValue>  (native JSON collections)
doc.content_as_bytes(); // Option<&[u8]>       (BLOB/CLOB/VARCHAR2 collections)
doc.has_content();
let _ = (&doc.key, &doc.version, &doc.created_on, &doc.last_modified, &doc.media_type);
```

### Insert — one / many, with or without returned metadata

The last `bool` selects "AndGet" behaviour: when true the server-assigned
key/version/timestamps are read back (via `RETURNING`) into a metadata-only
document, matching python-oracledb's `insertOneAndGet` / `insertManyAndGet`
(no content is echoed back). The `Option<&str>` argument is an optional SQL
hint.

```rust
// insertOne (fire-and-forget):
coll.insert_one(conn, cx, &doc, None, false).await?; // Ok(None)

// insertOneAndGet -> the inserted key/version:
let meta = coll.insert_one(conn, cx, &doc, None, true).await?; // Option<SodaDocument>
let key = meta.unwrap().key.unwrap();

// insertMany / insertManyAndGet:
let docs = vec![doc1, doc2, doc3];
coll.insert_many(conn, cx, &docs, None, false).await?;             // Ok(None)
let returned = coll.insert_many(conn, cx, &docs, None, true).await?; // Option<Vec<SodaDocument>>
```

### Find — the `SodaOperation` criteria

`find()` in python-oracledb is a fluent builder; here the equivalent is the
plain `SodaOperation` struct with public fields, constructed with struct-update
syntax. It is consumed by the read/write terminals below.

```rust
use oracledb::soda::SodaOperation;

let op = SodaOperation {
    key: Some(key.clone()),                 // single-key filter
    // keys: Some(vec![k1, k2]),            // multi-key IN-list filter
    // filter: Some(r#"{"age":{"$gt":18}}"#.into()), // QBE filter (see below)
    // version: Some(v),                    // optimistic-lock predicate
    // skip: Some(1), limit: Some(2),       // OFFSET / FETCH NEXT paging
    // fetch_array_size: 100,               // streaming batch size (default 100)
    // hint: Some("MONITOR".into()),        // SQL hint
    // lock: true,                          // SELECT ... FOR UPDATE
    ..Default::default()
};
```

Terminals on `SodaCollection`:

```rust
let n: u64                   = coll.get_count(conn, cx, &op).await?;
let one: Option<SodaDocument> = coll.get_one(conn, cx, &op).await?;
let all: Vec<SodaDocument>    = coll.get_documents(conn, cx, &op).await?;
let removed: u64             = coll.remove(conn, cx, &op).await?;

// replaceOne / replaceOneAndGet — requires key() (keys() is rejected):
let (replaced, meta) = coll.replace_one(conn, cx, &op, &new_doc, /*return_doc*/ false).await?;
```

`SodaOperation` also exposes the SQL builders it uses internally
(`build_select_sql`, `build_count_sql`, `build_delete_sql`) as public methods,
so callers can inspect the generated statement + binds if they need to.

### Streaming cursor

`open_cursor` returns a `SodaCursor` that buffers a fetch batch and refills from
the server until the result set is exhausted — the connection borrow stays short
between pulls.

```rust
let mut cursor = coll.open_cursor(conn, cx, &op).await?;
while let Some(doc) = cursor.next_doc(conn, cx).await? {
    // ... use doc ...
}
cursor.close(conn, cx).await?;
assert!(cursor.is_closed());
```

### Indexes, truncate

```rust
coll.truncate(conn, cx).await?; // remove all documents

// createIndex from a SODA index spec (DBMS_SODA_ADMIN.CREATE_INDEX):
coll.create_index(conn, cx,
    r#"{"name":"cust_ix","fields":[{"path":"age","datatype":"number","order":"asc"}]}"#
).await?;

// dropIndex by name; returns whether it existed. Last arg is `force`.
let dropped: bool = coll.drop_index(conn, cx, "cust_ix", false).await?;
```

## QBE (query-by-example) support

A QBE `filter` is a JSON string set on `SodaOperation::filter`. It is translated
to an Oracle `JSON_EXISTS(<content>, '<path predicate>')` fragment (see
`crates/oracledb/src/soda/qbe.rs`). Values are inlined into the JSON path
expression (Oracle does not accept binds inside path predicates) and every
inlined string is escaped, so a value like `O'Brien` cannot break out of the SQL
literal.

Supported operators (verified in `qbe.rs` + the live QBE breadth test):

- **Comparison:** `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte` (a bare scalar is
  an implicit `$eq`).
- **String matching:** `$like`, `$regex` (→ `like_regex`), `$startsWith`,
  `$hasSubstring` / `$instr` / `$contains` (all map to a regex-escaped substring
  match — see the note on `$contains` in the gaps section).
- **Case-folding wrappers:** `$upper`, `$lower` (e.g.
  `{"name":{"$upper":{"$startsWith":"JO"}}}`).
- **Type / existence:** `$type`, `$exists`.
- **Dates:** `$date` (compares ISO-8601 date strings lexically).
- **Logical:** `$and`, `$or`, `$nor`, `$not`.
- **Paths:** dotted paths (`address.city`) and array steps (`locations[*].city`,
  `locations[0 to 1].city`).
- **Ordering:** `$orderby` (list form, `[{"path":"name","order":"desc"}]`) is
  extracted into an `ORDER BY JSON_VALUE(...)` and does not contribute to the
  `WHERE` clause.

Any operator outside this set returns `SodaError::NotSupported` — never a silent
wrong answer.

```rust
// age > 18 AND city == "Perth"
let op = SodaOperation {
    filter: Some(r#"{"$and":[{"age":{"$gt":18}},{"address.city":{"$eq":"Perth"}}]}"#.into()),
    ..Default::default()
};
let n = coll.get_count(conn, cx, &op).await?;
```

## Database / version support

The real capability boundary for thin-mode SODA is the **`JSON_SERIALIZE` SQL
function**, which exists only on **Oracle Database 21c and later**. The write
and BLOB/CLOB read paths depend on it, so thin SODA works on **21c and 23ai**
and is genuinely unavailable on **18c and earlier**.

This boundary is asserted from both sides by a live test
(`soda_gated_on_pre21c_with_proof`) rather than being silently skipped:

- `< 21c`: a direct `JSON_SERIALIZE` probe fails and `create_collection` fails
  with **ORA-00904** (`"JSON_SERIALIZE": invalid identifier`) — an evidence-backed
  XFAIL. (Note: `USER_SODA_COLLECTIONS` resolves to a public synonym present even
  on 18c, so its catalog presence is *not* a usable version signal; the
  `JSON_SERIALIZE` function is what actually differs.)
- `>= 21c`: the probe and `create_collection` succeed.

**Collection shapes** the implementation handles:
- **Native 23ai** collections: embedded-OID `RESID` (RAW) key surfaced as a hex
  string, native `JSON` (OSON) content column.
- **Legacy** collections: VARCHAR2/UUID (or client-assigned) key, BLOB/CLOB/
  VARCHAR2 content — JSON-only BLOB/CLOB content is fetched inline via
  `JSON_SERIALIZE(... RETURNING VARCHAR2)`.

Metadata is parsed from the collection descriptor stored in
`USER_SODA_COLLECTIONS` (`SodaCollectionMetadata` / `parse_metadata`), which
drives key assignment (`KeyAssignment`), content type (`ContentSqlType`), and
version method (`VersionMethod`).

## Reference-suite results (the surpass claim, measured)

Run against the Rust thin engine via the PyO3 shim on Oracle Database 23ai Free
(23.26), using Oracle's own SODA suites
(`reference/python-oracledb/tests/test_3300_soda_database.py` +
`test_3400_soda_collection.py`):

| Reference module | passed | failed | skipped |
|---|---|---|---|
| `test_3300_soda_database.py` | **12** | 0 | 0 |
| `test_3400_soda_collection.py` | **30** | 12 | 6 |
| **total** | **42** | 12 | 6 |

`harness/soda_thin_inject.py` neutralises the reference `soda_db` fixture's
thick-mode skip so the suites run against the Rust thin engine; it changes
nothing else about thick/thin detection.

## Documented gaps and limitations

Every reference failure/skip is named and explained — none is a silent failure.

**Skipped by the reference's own fixtures (6):** `save()` / `saveAndGet()`
(`test_3418`–`test_3420`, skipped on DB 23 by `skip_if_save_not_supported`; thin
SODA's `save()` is a `NotSupported` stub) and `mapMode=True` (`test_3425`,
`test_3432`, `test_3442`, skipped by `skip_if_map_mode_not_supported` — map mode
is not supported with native collections).

**Oracle Text not installed on the container (5):** the 23ai Free container has
no JSON-search indextype, so `CREATE SEARCH INDEX … FOR JSON` raises
**ORA-29833** (an environment limitation; thick mode would fail identically).
This affects `$contains` word-matching (`test_3404`) and `getDataGuide()`
(`test_3438`–`test_3441`). thin SODA maps `$contains` to a best-effort
regex-escaped **substring** match, which is documented as approximate.

**Native-collection error-code differences (2):** the 23ai native JSON column
raises different ORA numbers than the older thick-client contract the reference
encodes — `test_3400` (invalid JSON → ORA-40441 instead of ORA-40780/ORA-02290)
and `test_3406` (duplicate index → ORA-00955 instead of ORA-40733). The server
validates; only the number differs.

**Feature gaps (2):** `listIndexes()` (`test_3429`) returns `NotSupported`
(the `DBMS_SODA_ADMIN.LIST_INDEXES` signature differs on this build); and true
**mixed-media** collections storing JSON *and* arbitrary binary in one LOB
column (`test_3444`) — thin SODA serializes BLOB-JSON inline via
`JSON_SERIALIZE`, which rejects non-JSON bytes (raw LOB reads are deferred).

**Representation / accounting (3):** the embedded `_id` round-trips as `bytes`
rather than the `oracledb.JsonId` wrapper (`test_3446` — value correct, Python
type differs); a SODA `hint("MONITOR")` is injected but the test's `v$sql` probe
captures an internal metadata query instead (`test_3414` — hint reflection is
best-effort); and `getCursor()` materialises matches in one fetch, so
per-`fetchArraySize` round-trip counts differ (`test_3428` — behaviour correct,
round-trip accounting is not parity).

## Architecture (module map)

`crates/oracledb/src/soda/` (feature `soda`):
- `database` — create / open / list / drop collections (`SodaDatabase`).
- `collection` — insert / find / replace / remove / index (`SodaCollection`).
- `operation` — `SodaOperation` criteria + the SELECT/COUNT/DELETE SQL builders.
- `qbe` — QBE filter → `JSON_EXISTS` / `ORDER BY` translation.
- `cursor` — the streaming document cursor (`SodaCursor`).
- `document` — `SodaDocument` (content + SODA metadata).
- `metadata` — descriptor parsing (`SodaCollectionMetadata`, `parse_metadata`).
- `error` — `SodaError` / `SodaResult`.

The pure-Rust surface is additive: the existing parity path is untouched.

## Proof — the SODA tests

- **Unit tests** (compiled with `--features soda`) live inline in each module's
  `#[cfg(test)]` block — QBE translation (`soda/qbe.rs`), SQL generation and
  key/version handling (`soda/operation.rs`), descriptor parsing
  (`soda/metadata.rs`), and cursor/cursor-cleanup lifecycle over a loopback
  server (`soda/collection.rs`, `soda/cursor.rs`).

- **Live integration tests:** `crates/oracledb/tests/live_soda.rs` exercises the
  real round-trip against an Oracle container — create/open/drop, insert +
  find-by-key, `insertMany` + QBE filters, `replaceOne` + `remove`, truncate +
  index + names, streaming cursors with a server refill, `skip`/`limit` paging,
  multi-key filters, per-document metadata, optimistic-locking replace-by-version,
  `insertManyAndGet`, QBE operator breadth, and the pre-21c capability gate.
  Run them (they are `#[ignore]`d by default) with a live lane up:

  ```bash
  cargo test -p oracledb --features soda --test live_soda -- --ignored
  ```

To reproduce the reference-suite numbers via the PyO3 shim:

```bash
eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1523 ORACLEDB_HOST_PORT=1523 scripts/container.sh env)"
ORACLEDB_VENV_DIR=$PWD/.venv-py313 scripts/setup-python-env.sh
.venv-py313/bin/python -m maturin develop -m crates/oracledb-pyshim/Cargo.toml
cd reference/python-oracledb
PYTHONPATH="$OLDPWD/harness" ../../.venv-py313/bin/python -m pytest \
  tests/test_3300_soda_database.py tests/test_3400_soda_collection.py \
  -p shim_inject -p soda_thin_inject -q
```
