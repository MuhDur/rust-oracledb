# Connect-String Parsing

`rust-oracledb` ships a real, full-fidelity connect-string parser in
`oracledb_protocol::net::connectstring`, matching python-oracledb thin-mode
semantics (`impl/base/parsers.pyx` + `connect_params.pyx`) and going beyond the
reference with precise, offset-pointed diagnostics.

The public entry points:

- `connectstring::parse(&str) -> Result<Option<Descriptor>>` — parses a TNS
  descriptor or EZConnect/EZConnect-Plus string. Returns `Ok(None)` when the
  string is neither (i.e. it is a tnsnames.ora alias to be resolved).
- `connectstring::tnsnames::TnsnamesReader::read(config_dir)` — reads
  `tnsnames.ora` (with `IFILE` includes) into an alias → descriptor map.
- `net::EasyConnect::parse(&str)` — resolves any of the above to the single
  primary endpoint (host/port/service/protocol) used by the connection path.
- `net::EasyConnect::parse_descriptor(&str)` — the full resolved topology.

## 1. TNS connect descriptors

Full nested descriptors are parsed by a real recursive-descent tokenizer
(nested parens, quoted values, case-insensitive keywords, whitespace/newline
tolerance):

```
(DESCRIPTION=
  (LOAD_BALANCE=ON)(FAILOVER=ON)(SOURCE_ROUTE=OFF)
  (RETRY_COUNT=3)(RETRY_DELAY=5)(EXPIRE_TIME=10)
  (TRANSPORT_CONNECT_TIMEOUT=15)(SDU=16384)
  (ADDRESS_LIST=(LOAD_BALANCE=ON)
    (ADDRESS=(PROTOCOL=tcp)(HOST=primary)(PORT=1521))
    (ADDRESS=(PROTOCOL=tcp)(HOST=standby)(PORT=1521)))
  (CONNECT_DATA=(SERVICE_NAME=svc)(SERVER=dedicated)(INSTANCE_NAME=inst))
  (SECURITY=(SSL_SERVER_DN_MATCH=ON)(SSL_SERVER_CERT_DN=CN=db)
            (MY_WALLET_DIRECTORY=/etc/wallet)))
```

Supported: `DESCRIPTION_LIST` / `DESCRIPTION` / `ADDRESS_LIST` / `ADDRESS` /
`CONNECT_DATA` / `SECURITY`; `LOAD_BALANCE` / `FAILOVER` / `SOURCE_ROUTE`;
`RETRY_COUNT` / `RETRY_DELAY`; `EXPIRE_TIME` (keepalive); `CONNECT_TIMEOUT` /
`TRANSPORT_CONNECT_TIMEOUT` (with `ms` / `sec` / `min` units); `SDU` (clamped to
512..=2 097 152); wallet / `SSL_SERVER_CERT_DN`; DRCP `POOL_*` / `SERVER=pooled`;
`HTTPS_PROXY` / `HTTPS_PROXY_PORT`. Unrecognised keys in `DESCRIPTION`,
`CONNECT_DATA`, and `SECURITY` are preserved and passed through to the listener
verbatim (e.g. `COLOCATION_TAG`, `FAILOVER_MODE`, `COMPRESSION`).

## 2. EZConnect and EZConnect-Plus

```
[protocol://]host[,host2;host3][:port][/service][:server][/instance][?k=v&...]
```

- Multiple hosts in one address list (`host1,host2`) and multiple address lists
  (`host1;host2`); ports back-fill onto earlier hostless entries.
- IPv6 literals in brackets: `[::1]:1521/svc`.
- Server type after the service (`/svc:pooled`) and instance name (`/svc/inst`).
- Extended `?key=value&...` parameters: `retry_count`, `retry_delay`,
  `expire_time`, `sdu`, `transport_connect_timeout`, `failover`,
  `load_balance`, `source_route`, `use_sni`, `ssl_server_dn_match`,
  `ssl_server_cert_dn`, `wallet_location`, `https_proxy[_port]`,
  `pool_connection_class`, `pool_purity`, and `pyo.*` driver-specific keys.
  Double-quoted values are unwrapped; single quotes are preserved verbatim.

## 3. tnsnames.ora

`TnsnamesReader::read(config_dir)` (resolve from `TNS_ADMIN` or an explicit
path) parses `tnsnames.ora` with:

- `#` comments (including comments embedded inside a descriptor value),
- multi-line, paren-balanced descriptor values,
- comma-separated alias lists (`a, b, c = ...`), possibly across lines,
- `IFILE = path` includes (relative to the including file, optionally quoted),
  with **cycle detection**,
- last-definition-wins for duplicate aliases; case-insensitive lookup.

## 4. Diagnostics (the differentiator)

Malformed input yields an error that points at the offending token with a caret
context window, rather than python-oracledb's terse `DPY-4017`:

```
invalid connect descriptor "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1)":
  unbalanced parenthesis: expected ')' at offset 52
  tcp)(HOST=h)(PORT=1)
                      ^
```

Other examples: `invalid protocol "ipc"`, `invalid server_type: bogus`,
`RETRY_COUNT value "wrong" is not a non-negative integer`,
`no addresses are defined in connect descriptor: ...`,
`unexpected simple value for a container keyword at offset N`.

`Descriptor::describe()` prints the resolved address list and connect data for
troubleshooting:

```
Descriptor {
  description[0]:
    address_list[0]: load_balance=false, failover=true, source_route=false
      tcp://localhost:1521
    connect_data: service_name=FREEPDB1
}
```

## Tests

Offline corpus-differential tables and a live test live alongside the parser:

- `crates/oracledb-protocol/src/net/connectstring.rs` — 40+ offline unit tests:
  descriptor, EZConnect/Plus, IPv6, multi-host, diagnostics, plus a valid-input
  table (21 rows) and a malformed-input table (10 rows) asserting the exact
  diagnostic, all matching python-oracledb `test_4500` / `test_7200` semantics.
- `crates/oracledb/tests/live_connect_string.rs` — parses a full `DESCRIPTION`
  and an EZConnect string and connects with each, running `select 7 + 5` → 12.
