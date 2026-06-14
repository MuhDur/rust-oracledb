# Deployment — single static binary + `FROM scratch` image

`rust-oracledb` is a **pure-Rust, thin-mode** Oracle driver. It speaks the
Oracle TNS/TTC wire protocol directly over TCP and links no native Oracle
libraries. That property has a concrete operational payoff: an application using
this driver can be compiled to **one fully-static executable** and shipped in a
**`FROM scratch`** container — no operating system, no glibc, no shared
libraries, no Python interpreter, and no Oracle Instant Client.

This is the deployment story `python-oracledb` cannot match:

- **`python-oracledb` thin mode** still needs a Python interpreter plus its
  standard library and the compiled extension's shared objects.
- **`python-oracledb` thick mode** additionally needs the Oracle **Instant
  Client** (hundreds of MB of native libraries) present at runtime.

Everything below is reproducible with `scripts/smoke-static.sh`. The numbers in
this document are **real, measured** on this machine (x86_64), not estimates.

---

## 1. The proof at a glance

| Artifact | What's inside | Size (measured) |
| --- | --- | --- |
| `rust-oracledb` static binary (`smoke`, stripped, musl) | the whole app + driver + TLS stack | **4.26 MB** |
| `rust-oracledb` `FROM scratch` image | *only* that binary | **4.26 MB** |
| `python:3.13-slim` base image (before any deps) | CPython + slim Debian userland | **118 MB** |
| `python-oracledb` thin deploy (`python:3.13-slim` + `pip install oracledb`) | interpreter + stdlib + driver wheel | **151 MB** |
| `python-oracledb` thin wheel alone (4.0.1) | the driver, no interpreter | 2.5 MB |

The `rust-oracledb` scratch image is **~35× smaller** than the equivalent
`python-oracledb` thin deploy image — and `python-oracledb` thick mode would add
the Instant Client (typically another ~250–500 MB) on top of that 151 MB.

The scratch image is byte-for-byte the binary: image size == binary size,
because the image has exactly one layer containing exactly one file.

### Static-linkage proof

```text
$ file smoke
smoke: ELF 64-bit LSB pie executable, x86-64, version 1 (SYSV), static-pie linked, stripped

$ ldd smoke
        statically linked
```

### Smoke run (scratch image against a live listener)

```text
$ docker run --rm --network=host \
    -e PYO_TEST_CONNECT_STRING=localhost:1525/FREEPDB1 \
    -e PYO_TEST_MAIN_USER=pythontest \
    -e PYO_TEST_MAIN_PASSWORD=pythontest \
    rust-oracledb-smoke:scratch
[smoke] connecting to localhost:1525/FREEPDB1 as pythontest ...
[smoke] connected: session_id=228 serial=9508
12
[smoke] typed query returned label="rust-oracledb"
[smoke] OK — connected, ran 2 queries, closed cleanly
```

The lone `12` on its own line is the result of `select 7+5 from dual`, fetched
from a real Oracle database by a binary running in a container that contains
nothing but that binary.

---

## 2. Build commands

The example binary lives at
[`crates/oracledb/examples/smoke.rs`](../crates/oracledb/examples/smoke.rs). It
uses the synchronous [`BlockingConnection`] facade, so it is an ordinary
`main()` with no visible async runtime, and pulls in only the crate's existing
dependencies (the pure-Rust `rustls` + `ring` stack — **no OpenSSL**).

### One-shot: `scripts/smoke-static.sh`

```bash
# point at a listener (or source the lane container env)
eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1525 \
        ORACLEDB_HOST_PORT=1525 scripts/container.sh env)"

scripts/smoke-static.sh
```

That script does all of the following, end to end: fetch the musl cross
toolchain, build the static binary, prove it's static (`file` + `ldd`), build
the `FROM scratch` image, and run it against the listener — failing loudly if
the binary isn't static or the query doesn't return 12.

### Manual steps

```bash
# 1. musl target
rustup target add x86_64-unknown-linux-musl

# 2. musl C cross-toolchain for ring (rustls' crypto backend compiles a little C
#    for the musl target). Prebuilt, relocatable, no root required:
curl -sSL -o /tmp/musl-cross.tgz https://musl.cc/x86_64-linux-musl-cross.tgz
tar xzf /tmp/musl-cross.tgz -C "$HOME/.cache"
export PATH="$HOME/.cache/x86_64-linux-musl-cross/bin:$PATH"
export CC_x86_64_unknown_linux_musl=x86_64-linux-musl-gcc
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=x86_64-linux-musl-gcc

# 3. fully-static release build
cargo build --release --example smoke -p oracledb \
  --target x86_64-unknown-linux-musl

# 4. (optional) strip for the smallest artifact
x86_64-linux-musl-strip \
  target/x86_64-unknown-linux-musl/release/examples/smoke

# 5. verify static
file target/x86_64-unknown-linux-musl/release/examples/smoke   # -> static-pie linked
ldd  target/x86_64-unknown-linux-musl/release/examples/smoke   # -> statically linked

# 6. FROM scratch image (stage the binary as ./smoke in a build context)
docker build -f docker/Dockerfile.scratch -t rust-oracledb-smoke:scratch <context-dir>
```

The Dockerfile is
[`docker/Dockerfile.scratch`](../docker/Dockerfile.scratch): a two-real-line
`FROM scratch` / `COPY smoke /smoke` / `ENTRYPOINT ["/smoke"]`.

---

## 3. Why the musl C toolchain is needed (the `ring` note)

The driver's TLS support uses [`rustls`](https://github.com/rustls/rustls) with
the **`ring`** crypto backend (already pinned in
[`Cargo.toml`](../Cargo.toml)). `ring` is pure-safe-Rust at the API boundary,
but its build compiles a small amount of C and architecture assembly. Under
`x86_64-unknown-linux-musl`, `cc-rs` therefore looks for
`x86_64-linux-musl-gcc`. The host's `gcc`/`clang` target glibc and there is no
musl sysroot on the box, so the standard, no-root fix is a prebuilt musl
cross-toolchain (we fetch `x86_64-linux-musl-cross` from musl.cc into the user
cache). With that on `PATH` and `CC_x86_64_unknown_linux_musl` set, the static
build links cleanly and `ring` works under musl.

We chose `ring` over `aws-lc-rs` because `ring` cross-compiles to musl with the
least ceremony (`aws-lc-rs` pulls a larger C/CMake build surface). The TLS stack
stays pure-Rust at the source level either way — there is no OpenSSL anywhere in
the dependency graph.

If you build the **driver core without the TLS path** at all (plain TCP only),
you don't even need the C toolchain — but this crate's `rustls` dependency is
not feature-gated off, so the TLS stack is always compiled in. The musl
toolchain is therefore part of the standard static build here.

---

## 4. Caveats (the honest part)

- **TLS/TCPS needs CA certs in the image.** The smoke binary above connects over
  **plain TCP**, which needs no certificates, so the scratch image is just the
  binary. If you connect over **TCPS** (Autonomous Database, hardened
  listeners), the binary must be able to validate the server chain, which means
  a CA bundle must be present at runtime — either baked into the image
  (`COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt`, the commented
  block in `docker/Dockerfile.scratch`) or mounted as a volume. A wallet-based
  mTLS connection additionally needs the wallet files mounted. See
  [`TLS_SETUP.md`](TLS_SETUP.md). A scratch image has **no** default trust
  store, so this is not optional for TLS — it's a hard requirement.
- **The binary is `x86_64`-`musl`.** It runs on any Linux x86_64 host/kernel
  (that's the point of static musl), but it is not a universal binary. ARM64
  deployments need an `aarch64-unknown-linux-musl` build with the matching cross
  toolchain; Windows/macOS need their own native (non-musl) targets and are not
  `FROM scratch`.
- **musl allocator.** Static musl binaries use musl's `malloc`, which can be
  slower than glibc's under highly concurrent allocation-heavy workloads. For
  the driver's network-bound profile this is rarely the bottleneck, but it's a
  known musl trade-off; benchmark if allocation throughput matters to you.
- **Host networking in the demo.** The smoke run uses `--network=host` so the
  in-container binary can reach a listener published on the host's `localhost`.
  Real deployments point `PYO_TEST_CONNECT_STRING` (or your own connect string)
  at a reachable listener and don't need host networking.
- **No shell, no debugger in the image.** `FROM scratch` means there is no
  `/bin/sh` to `docker exec` into. That's a feature (tiny attack surface) but it
  changes how you debug — rely on the binary's own stderr logging, or run the
  same binary outside the container.

---

## 5. What the image actually contains

```text
$ docker run --rm --entrypoint /smoke rust-oracledb-smoke:scratch --help 2>/dev/null; \
  docker history rust-oracledb-smoke:scratch
# one COPY layer == the binary; everything else is zero-byte metadata
```

There is no package manager, no libc, no CA store, no `/etc`, no `/tmp`, no
users database, just `/smoke`. The deployable surface area is exactly the code
you wrote plus the Rust driver, statically linked, and nothing else.

[`BlockingConnection`]: https://docs.rs/oracledb/latest/oracledb/struct.BlockingConnection.html
