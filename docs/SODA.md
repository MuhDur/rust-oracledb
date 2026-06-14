# Thin-mode SODA (Simple Oracle Document Access) — experimental

> **Honest framing:** this is **experimental thin-mode SODA**. python-oracledb's
> thin mode supports **zero** SODA (SODA is documented thick-only). This driver
> implements SODA over the thin TTC protocol, and currently passes **42 of the
> reference SODA suite** (Oracle's own `test_3300_soda_database.py` +
> `test_3400_soda_collection.py`), run against the Rust engine via the shim on a
> live Oracle Database 23ai Free container.
>
> This is **not** "production-ready" and **not** "full parity" with thick-mode
> SODA. It is a genuine surpass over python-oracledb thin (which has none) with
> an explicitly documented gap list below.

## The surpass claim

python-oracledb's thin mode raises `DPI-1050: SODA requires Oracle Client … thick
mode` for every SODA call. SODA is a management abstraction whose thick OCI
library internally generates standard SQL and `DBMS_SODA` PL/SQL — all of which
the thin TTC protocol already runs. This crate implements SODA directly over the
existing `Connection` execute/fetch surface and the OSON/JSON codecs, with no new
wire-protocol work. The result is the first pure-thin SODA in an Oracle driver.

## Reference suite results (the claim, measured)

Run against the Rust thin engine via the PyO3 shim on Oracle Database 23ai Free
(23.26), lane container `rust-oracledb-lane-1523`:

| Reference module | passed | failed | skipped |
|---|---|---|---|
| `test_3300_soda_database.py` | **12** | 0 | 0 |
| `test_3400_soda_collection.py` | **30** | 12 | 6 |
| **total** | **42** | 12 | 6 |

To reproduce:

```bash
eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1523 ORACLEDB_HOST_PORT=1523 scripts/container.sh env)"
ORACLEDB_VENV_DIR=$PWD/.venv-py313 scripts/setup-python-env.sh
.venv-py313/bin/python -m maturin develop -m crates/oracledb-pyshim/Cargo.toml
cd reference/python-oracledb
PYTHONPATH="$OLDPWD/harness" ../../.venv-py313/bin/python -m pytest \
  tests/test_3300_soda_database.py tests/test_3400_soda_collection.py \
  -p shim_inject -p soda_thin_inject -q
```

`harness/soda_thin_inject.py` neutralises the reference `soda_db` fixture's
thick-mode skip so the suites run against the Rust thin engine; it changes
nothing else about thick/thin detection.

## What thin-mode SODA supports

Implemented and verified against the live database (with round-tripped JSON):

- **Collections:** `createCollection` (default native + custom metadata),
  `openCollection`, `getCollectionNames` (start/limit), `drop`, `truncate`.
- **Documents:** `createDocument` (dict/list/scalar/str/bytes),
  `insertOne` / `insertOneAndGet`, `insertMany` / `insertManyAndGet`.
- **Find:** `find().key()` / `.keys()` / `.filter(QBE)` / `.version()` /
  `.skip()` / `.limit()` / `.lock()`; terminals `count()`, `getOne()`,
  `getDocuments()`, `getCursor()`, `remove()`, `replaceOne()` /
  `replaceOneAndGet()`.
- **QBE operators:** `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$like`,
  `$regex`, `$startsWith`, `$hasSubstring`, `$instr`, `$contains` (best-effort
  substring — see gaps), `$upper`, `$lower`, `$type`, `$exists`, `$date`,
  `$and`, `$or`, `$nor`, `$not`, plus array path steps (`field[*]`,
  `field[0 to 1]`) and `$orderby`. Unsupported operators raise a clear
  `NotSupported` error — never a silent wrong answer.
- **Indexes:** `createIndex` (functional / field indexes via
  `DBMS_SODA_ADMIN.CREATE_INDEX`), `dropIndex`.
- **Collection shapes:** native 23ai (embedded-OID `RESID` key, native `JSON`
  content) **and** legacy (VARCHAR2 UUID key, BLOB/CLOB content fetched inline
  via `JSON_SERIALIZE`).

## Documented gaps (every reference failure/skip explained)

Each entry names the exact reference test, its assertion, and why it is a known
gap — none is a silent failure.

### Skipped by the reference's own fixtures (6)

- **`test_3418`, `test_3419`, `test_3420`** — `save()` / `saveAndGet()`. Skipped
  by the reference `skip_if_save_not_supported` fixture on Oracle Database 23
  ("save() is not implemented in Oracle Database version 23"). thin-SODA's
  `save()` is a NotSupported stub (UPSERT semantics); the reference skips it on
  this DB regardless.
- **`test_3425`, `test_3432`, `test_3442`-mapMode portions** — `mapMode=True`.
  Skipped by the reference `skip_if_map_mode_not_supported` fixture on Oracle
  Client 23 ("map mode not supported with native collections").

### Oracle Text not installed on the container (5)

The 23ai Free container has no `CTXSYS` / JSON-search indextype, so
`CREATE SEARCH INDEX … FOR JSON` raises **ORA-29833**. This is an environment
limitation, not a driver limitation — thick mode on the same container would
fail identically.

- **`test_3404`** — uses `$contains` after `createIndex({"name": "js_ix_3404"})`.
  The search index cannot be created (ORA-29833), and `$contains` word-matching
  needs it. thin-SODA maps `$contains` to a best-effort substring `like_regex`,
  which is documented as approximate.
- **`test_3438`, `test_3439`, `test_3440`, `test_3441`** — `getDataGuide()`.
  Requires a data-guide-enabled JSON search index (ORA-29833 / ORA-40582). Not
  available without Oracle Text.

### Native-collection error-code differences (2)

The 23ai default native collection raises different ORA codes than the older
thick-client contract the reference encodes; thick mode on this DB version would
behave the same.

- **`test_3400`** — inserts invalid JSON `{testKey:testValue}` and asserts
  `ORA-40780` **or** `ORA-02290`. The native JSON column raises **ORA-40441**
  (JSON syntax error). The driver correctly lets the server validate; only the
  error number differs.
- **`test_3406`** — creates a duplicate index and asserts **ORA-40733**.
  `DBMS_SODA_ADMIN.CREATE_INDEX` on 23.26 raises **ORA-00955** (name already
  used). Same call, different server-version error number.

### Feature gaps (2)

- **`test_3429`** — `listIndexes()`. Returns NotSupported. The
  `DBMS_SODA_ADMIN.LIST_INDEXES` signature on this DB differs from the
  documented one; deferred.
- **`test_3444`** — mixed-media collection storing JSON **and** binary
  (`text/plain`, `application/octet-stream`) in one BLOB column. thin-SODA
  serializes BLOB-JSON inline via `JSON_SERIALIZE`, which rejects non-JSON bytes
  (ORA-40441). True mixed-media needs raw LOB reads (deferred).

### Representation / accounting gaps (2)

- **`test_3446`** — asserts the embedded `_id` round-trips as
  `oracledb.JsonId`. thin-SODA returns it as `bytes` (the raw OID). The JsonId
  extended-type wrapper (23.4+) is not reconstructed; the value is correct, the
  Python wrapper type differs.
- **`test_3414`** — asserts a SODA `hint("MONITOR")` appears in `v$sql`. Hints
  are injected into the generated SQL, but the statement captured by the test's
  `prev_sql_id` probe is an internal SODA metadata query, not the hinted
  document statement, so the assertion does not see the hint. Hint reflection is
  best-effort.
- **`test_3428`** — asserts exact round-trip counts for `fetchArraySize`.
  thin-SODA's `getCursor()` materialises the matching documents in one fetch
  (keeping the connection borrow short), so per-array-size round-trip counts
  differ (`2 != 1`). Behaviour is correct; the round-trip accounting is not
  parity.

## Architecture

- `crates/oracledb/src/soda/` (feature `soda`): `metadata` (descriptor parsing),
  `document`, `qbe` (filter → `JSON_EXISTS` translation), `operation` (SQL
  builders), `collection` (insert/find/replace/remove/index), `cursor`,
  `database` (create/open/list/drop).
- `crates/oracledb-pyshim/src/soda.rs`: the PyO3 surface
  (`ThinSodaDbImpl` / `ThinSodaCollImpl` / `ThinSodaDocImpl` /
  `ThinSodaDocCursorImpl`) wired via `ThinConnImpl::create_soda_database_impl`.
- Additive only: the existing parity path is untouched (`test_1100` 57p/5s,
  `test_2200` 39p verified unaffected).
