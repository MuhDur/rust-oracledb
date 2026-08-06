# oraclemcp-driver-cx

> ## ⚠️ Discontinued — the `oracledb` name now belongs to Oracle
>
> I gave the [`oracledb`](https://crates.io/crates/oracledb) crate name to Oracle. An
> Oracle Database driver is essentially Oracle's product, and I respect that — the
> canonical name belongs with the vendor. Oracle now ships its own official pure-Rust
> thin driver there ([oracle-samples/rust-oracledb](https://github.com/oracle-samples/rust-oracledb));
> **use that for production Rust-on-Oracle.**
>
> This crate — `oraclemcp-driver-cx` — is the original clean-room Rust thin driver that
> preceded it. **I am not continuing Rust-driver development here.** It was a genuinely
> great and fun thing to build, but Oracle's own team supports Rust now, so the mantle is
> rightly theirs.
>
> **What I *do* continue:** **[oraclemcp](https://github.com/MuhDur/oraclemcp)** — a
> governed, least-privilege Oracle Database MCP server (fail-closed SQL guard,
> confirmation-gated writes) that this driver grew out of.
>
> ### → [durakovic.ai](https://durakovic.ai)
> I build the hard parts of databases, systems, and AI in production — vendor-neutral,
> cloud or self-hosted. Building something hard on Oracle, Rust, or AI?
> **[hello@durakovic.ai](mailto:hello@durakovic.ai)**

---

**What it was.** A pure-Rust, **async** thin-mode Oracle Database driver on the
[`asupersync`](https://crates.io/crates/asupersync) structured-concurrency runtime (`Cx`) —
a clean-room port of python-oracledb thin mode that **passed python-oracledb's own
conformance suite** (2,462 reference thin-mode tests against Oracle 23ai), ran **in
production**, and is `#![forbid(unsafe_code)]` throughout.

**Status: discontinued.** The name and the "canonical Rust Oracle driver" role were handed
to Oracle, whose official driver now lives at [`oracledb`](https://crates.io/crates/oracledb).
This crate stays published for provenance and pinned users but won't get further
development — for new work, use Oracle's official driver.

## License

Dual-licensed under [Apache-2.0](LICENSE-APACHE) OR [MIT](LICENSE-MIT) at your option.
