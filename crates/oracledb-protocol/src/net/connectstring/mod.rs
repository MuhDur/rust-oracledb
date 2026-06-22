#![forbid(unsafe_code)]
//! Real, full-fidelity Oracle connect-string parsing.
//!
//! This module parses the three connect-string forms understood by
//! python-oracledb thin mode, matching the reference parser
//! (`impl/base/parsers.pyx` / `connect_params.pyx`) semantics:
//!
//!   1. **TNS connect descriptors** —
//!      `(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=..)(PORT=..)))
//!      (CONNECT_DATA=(SERVICE_NAME=..)))`, including `DESCRIPTION_LIST`,
//!      multiple `ADDRESS_LIST`/`ADDRESS`, `LOAD_BALANCE`/`FAILOVER`/
//!      `SOURCE_ROUTE`, `RETRY_COUNT`/`RETRY_DELAY`, `EXPIRE_TIME`,
//!      `TRANSPORT_CONNECT_TIMEOUT`, `SDU`, `SECURITY` (wallet / cert DN), and
//!      arbitrary pass-through keys. Case-insensitive keywords, nested parens,
//!      quoted values, and whitespace tolerance.
//!
//!   2. **EZConnect / EZConnect-Plus** —
//!      `[proto://]host[,host2][:port][/service][:server][/instance][?k=v&..]`,
//!      including multiple hosts, multiple address lists (`;`), IPv6 `[::1]`,
//!      and the extended `?key=value` parameters.
//!
//!   3. **tnsnames.ora** — alias -> descriptor maps with comments (`#`),
//!      multi-line entries, comma-separated alias lists, and `IFILE` includes
//!      (with cycle detection), resolved relative to `TNS_ADMIN` / a config dir.
//!
//! Beyond parity, the parser produces **rich diagnostics**: every error points
//! at the offending byte offset with surrounding context, and [`Descriptor`]
//! offers a [`Descriptor::describe`] troubleshooting dump of the resolved
//! address list and connect data.

use crate::{ProtocolError, Result};

/// Default listener port when none is given (reference `DEFAULT_PORT`).
pub const DEFAULT_PORT: u16 = 1521;
/// Default TCPS listener port.
pub const DEFAULT_TCPS_PORT: u16 = 2484;
/// Default SDU in bytes (reference `DEFAULT_SDU`).
pub const DEFAULT_SDU: u32 = 8192;
/// Minimum SDU after sanitisation.
pub const MIN_SDU: u32 = 512;
/// Maximum SDU after sanitisation.
pub const MAX_SDU: u32 = 2_097_152;
/// Default retry delay (reference `DEFAULT_RETRY_DELAY`).
pub const DEFAULT_RETRY_DELAY: u32 = 1;
/// Default transport connect timeout in seconds.
pub const DEFAULT_TCP_CONNECT_TIMEOUT: f64 = 20.0;

/// Transport protocol parsed from an `ADDRESS` `PROTOCOL=` or an EZConnect
/// `proto://` prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum Protocol {
    /// Plain TCP (default); default port 1521.
    #[default]
    Tcp,
    /// TLS-encrypted TCP; default port 2484.
    Tcps,
}

impl Protocol {
    /// Default listener port for this protocol.
    #[must_use]
    pub fn default_port(self) -> u16 {
        match self {
            Self::Tcp => DEFAULT_PORT,
            Self::Tcps => DEFAULT_TCPS_PORT,
        }
    }

    /// Returns whether this protocol requires a TLS handshake.
    #[must_use]
    pub fn is_tls(self) -> bool {
        matches!(self, Self::Tcps)
    }

    /// Lower-case keyword as it appears in a connect string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Tcps => "tcps",
        }
    }

    fn from_keyword(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "tcp" => Ok(Self::Tcp),
            "tcps" => Ok(Self::Tcps),
            other => Err(ProtocolError::InvalidConnectDescriptor(format!(
                "invalid protocol \"{other}\""
            ))),
        }
    }
}

/// Database server connection mode (`(SERVER=..)` / `:server` in EZConnect).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerType {
    /// A dedicated server process.
    Dedicated,
    /// A shared (multi-threaded) server.
    Shared,
    /// A DRCP pooled server.
    Pooled,
}

impl ServerType {
    /// Lower-case keyword as it appears in a connect string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dedicated => "dedicated",
            Self::Shared => "shared",
            Self::Pooled => "pooled",
        }
    }

    fn from_keyword(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "dedicated" => Ok(Self::Dedicated),
            "shared" => Ok(Self::Shared),
            "pooled" => Ok(Self::Pooled),
            other => Err(ProtocolError::InvalidConnectDescriptor(format!(
                "invalid server_type: {other}"
            ))),
        }
    }
}

/// DRCP connection-pool purity (`(POOL_PURITY=..)`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Purity {
    /// Reuse a session from the pool as-is.
    Self_,
    /// Force a brand-new session.
    New,
}

impl Purity {
    fn from_keyword(value: &str) -> Result<Self> {
        match value.to_ascii_uppercase().as_str() {
            "SELF" => Ok(Self::Self_),
            "NEW" => Ok(Self::New),
            other => Err(ProtocolError::InvalidConnectDescriptor(format!(
                "invalid value for enum Purity: {other}"
            ))),
        }
    }
}

/// A single resolved network endpoint (one `ADDRESS` node).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Address {
    /// Host name or IP literal.
    pub host: Option<String>,
    /// Listener port.
    pub port: u16,
    /// Transport protocol.
    pub protocol: Protocol,
    /// Optional forward proxy host.
    pub https_proxy: Option<String>,
    /// Optional forward proxy port (0 = unset).
    pub https_proxy_port: u16,
}

impl Default for Address {
    fn default() -> Self {
        Self {
            host: None,
            port: DEFAULT_PORT,
            protocol: Protocol::Tcp,
            https_proxy: None,
            https_proxy_port: 0,
        }
    }
}

/// A group of [`Address`]es (one `ADDRESS_LIST` node) plus its navigation flags.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AddressList {
    /// Member addresses.
    pub addresses: Vec<Address>,
    /// `LOAD_BALANCE=ON` randomises address order.
    pub load_balance: bool,
    /// `FAILOVER=OFF` disables trying alternate addresses.
    pub failover: bool,
    /// `SOURCE_ROUTE=ON` chains through the addresses in order.
    pub source_route: bool,
}

/// Resolved `CONNECT_DATA` settings.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnectData {
    /// `SERVICE_NAME=`.
    pub service_name: Option<String>,
    /// `SID=`.
    pub sid: Option<String>,
    /// `INSTANCE_NAME=`.
    pub instance_name: Option<String>,
    /// `SERVER=` (dedicated / shared / pooled).
    pub server_type: Option<ServerType>,
    /// `POOL_CONNECTION_CLASS=`.
    pub cclass: Option<String>,
    /// `POOL_PURITY=`.
    pub purity: Option<Purity>,
    /// `POOL_BOUNDARY=`.
    pub pool_boundary: Option<String>,
    /// `POOL_NAME=`.
    pub pool_name: Option<String>,
    /// `CONNECTION_ID_PREFIX=`.
    pub connection_id_prefix: Option<String>,
    /// `USE_TCP_FAST_OPEN=ON`.
    pub use_tcp_fast_open: bool,
    /// Unrecognised CONNECT_DATA keys, passed through to the listener verbatim
    /// (uppercased key -> reconstructed value).
    pub extra: Vec<(String, String)>,
}

/// Resolved `SECURITY` settings (only meaningful for TCPS addresses).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Security {
    /// `SSL_SERVER_DN_MATCH` (defaults to true).
    pub ssl_server_dn_match: bool,
    /// `SSL_SERVER_CERT_DN=`.
    pub ssl_server_cert_dn: Option<String>,
    /// `MY_WALLET_DIRECTORY=` / `WALLET_LOCATION=`.
    pub wallet_location: Option<String>,
    /// Unrecognised SECURITY keys, passed through verbatim.
    pub extra: Vec<(String, String)>,
}

impl Default for Security {
    fn default() -> Self {
        Self {
            ssl_server_dn_match: true,
            ssl_server_cert_dn: None,
            wallet_location: None,
            extra: Vec::new(),
        }
    }
}

/// A single resolved `DESCRIPTION` node.
#[derive(Clone, Debug, PartialEq)]
pub struct Description {
    /// Address lists belonging to this description.
    pub address_lists: Vec<AddressList>,
    /// `CONNECT_DATA` settings.
    pub connect_data: ConnectData,
    /// `SECURITY` settings.
    pub security: Security,
    /// `RETRY_COUNT=`.
    pub retry_count: u32,
    /// `RETRY_DELAY=` (seconds).
    pub retry_delay: u32,
    /// `EXPIRE_TIME=` (minutes; TCP keepalive).
    pub expire_time: u32,
    /// `TRANSPORT_CONNECT_TIMEOUT` / `CONNECT_TIMEOUT` (seconds).
    pub tcp_connect_timeout: f64,
    /// `SDU=` (sanitised into [`MIN_SDU`]..=[`MAX_SDU`]).
    pub sdu: u32,
    /// `LOAD_BALANCE=ON`.
    pub load_balance: bool,
    /// `FAILOVER=OFF`.
    pub failover: bool,
    /// `SOURCE_ROUTE=ON`.
    pub source_route: bool,
    /// `USE_SNI=ON`.
    pub use_sni: bool,
    /// Unrecognised DESCRIPTION keys, passed through verbatim.
    pub extra: Vec<(String, String)>,
}

impl Default for Description {
    fn default() -> Self {
        Self {
            address_lists: Vec::new(),
            connect_data: ConnectData::default(),
            security: Security::default(),
            retry_count: 0,
            retry_delay: DEFAULT_RETRY_DELAY,
            expire_time: 0,
            tcp_connect_timeout: DEFAULT_TCP_CONNECT_TIMEOUT,
            sdu: DEFAULT_SDU,
            load_balance: false,
            failover: true,
            source_route: false,
            use_sni: false,
            extra: Vec::new(),
        }
    }
}

impl Description {
    /// Iterator over every [`Address`] across all address lists, in order.
    pub fn addresses(&self) -> impl Iterator<Item = &Address> {
        self.address_lists
            .iter()
            .flat_map(|list| list.addresses.iter())
    }
}

/// A fully parsed connect string: one or more [`Description`]s.
#[derive(Clone, Debug, PartialEq)]
pub struct Descriptor {
    /// Member descriptions (one for a plain `DESCRIPTION`, several for a
    /// `DESCRIPTION_LIST` or multi-address-list EZConnect).
    pub descriptions: Vec<Description>,
    /// `DESCRIPTION_LIST` `LOAD_BALANCE=ON`.
    pub load_balance: bool,
    /// `DESCRIPTION_LIST` `FAILOVER=OFF`.
    pub failover: bool,
    /// `DESCRIPTION_LIST` `SOURCE_ROUTE=ON`.
    pub source_route: bool,
}

impl Descriptor {
    /// The first description (always present for a successfully parsed string).
    #[must_use]
    pub fn first_description(&self) -> &Description {
        &self.descriptions[0]
    }

    /// Iterator over every [`Address`] across all descriptions, in order.
    pub fn addresses(&self) -> impl Iterator<Item = &Address> {
        self.descriptions.iter().flat_map(Description::addresses)
    }

    /// The first address that has a host, if any.
    #[must_use]
    pub fn first_address(&self) -> Option<&Address> {
        self.addresses().find(|addr| addr.host.is_some())
    }

    /// Human-readable troubleshooting dump of the resolved address list and
    /// connect data — the differentiator over python-oracledb's terse errors.
    #[must_use]
    pub fn describe(&self) -> String {
        let mut out = String::new();
        out.push_str("Descriptor {\n");
        if self.descriptions.len() > 1 || self.load_balance || self.source_route || !self.failover {
            out.push_str(&format!(
                "  description_list: load_balance={}, failover={}, source_route={}\n",
                self.load_balance, self.failover, self.source_route
            ));
        }
        for (di, desc) in self.descriptions.iter().enumerate() {
            out.push_str(&format!("  description[{di}]:\n"));
            for (li, list) in desc.address_lists.iter().enumerate() {
                out.push_str(&format!(
                    "    address_list[{li}]: load_balance={}, failover={}, source_route={}\n",
                    list.load_balance, list.failover, list.source_route
                ));
                for addr in &list.addresses {
                    out.push_str(&format!(
                        "      {}://{}:{}\n",
                        addr.protocol.as_str(),
                        addr.host.as_deref().unwrap_or("<none>"),
                        addr.port
                    ));
                }
            }
            let cd = &desc.connect_data;
            out.push_str("    connect_data:");
            if let Some(s) = &cd.service_name {
                out.push_str(&format!(" service_name={s}"));
            }
            if let Some(s) = &cd.sid {
                out.push_str(&format!(" sid={s}"));
            }
            if let Some(s) = &cd.instance_name {
                out.push_str(&format!(" instance_name={s}"));
            }
            if let Some(s) = cd.server_type {
                out.push_str(&format!(" server={}", s.as_str()));
            }
            out.push('\n');
            if desc.retry_count != 0 {
                out.push_str(&format!(
                    "    retry_count={}, retry_delay={}\n",
                    desc.retry_count, desc.retry_delay
                ));
            }
        }
        out.push('}');
        out
    }
}

/// Parses a connect string into a [`Descriptor`].
///
/// Accepts a TNS connect descriptor (when the first non-space character is `(`)
/// or an EZConnect / EZConnect-Plus string otherwise. Returns
/// [`ProtocolError::InvalidConnectDescriptor`] with offset/context diagnostics
/// on malformed input, or `Ok(None)` when the string is neither (i.e. it is a
/// tnsnames.ora alias to be resolved separately).
pub fn parse(connect_string: &str) -> Result<Option<Descriptor>> {
    let trimmed = connect_string.trim();
    if trimmed.is_empty() {
        return Err(err_descriptor(
            connect_string,
            0,
            "connect string must not be empty",
        ));
    }
    let chars: Vec<char> = trimmed.chars().collect();
    if chars[0] == '(' {
        let mut parser = DescriptorParser::new(&chars, connect_string);
        parser.pos = 1;
        parser.temp_pos = 1;
        let args = parser.parse_descriptor()?;
        let descriptor = build_descriptor(connect_string, &args)?;
        // The whole input must be consumed; mirror the reference's trailing
        // check (it raises ERR_CANNOT_PARSE_CONNECT_STRING).
        if parser.pos != chars.len() {
            return Err(err_cannot_parse(connect_string));
        }
        Ok(Some(descriptor))
    } else {
        easy_connect::parse(&chars, connect_string)
    }
}

/// EZConnect / EZConnect-Plus parsing.
///
/// Mirrors the reference `_parse_easy_connect*` methods: it parses an optional
/// `proto://` prefix, one or more comma/semicolon-separated hosts (with IPv6
/// brackets), an optional `:port`, an optional `/service[:server]`, an optional
/// `/instance`, and an optional `?key=value&...` extended-parameter section.
mod easy_connect;

// ---------------------------------------------------------------------------
// Diagnostics helpers
// ---------------------------------------------------------------------------

/// The raw connect string is included so the message is self-describing; a
/// caret-context snippet is appended pointing at `offset` (a char index into
/// the trimmed string) so the operator can see exactly where parsing failed.
fn err_descriptor(connect_string: &str, char_offset: usize, reason: &str) -> ProtocolError {
    let trimmed = connect_string.trim();
    let snippet = context_snippet(trimmed, char_offset);
    ProtocolError::InvalidConnectDescriptor(format!(
        "invalid connect descriptor \"{connect_string}\": {reason} at offset {char_offset}\n{snippet}"
    ))
}

fn err_cannot_parse(connect_string: &str) -> ProtocolError {
    ProtocolError::InvalidConnectDescriptor(format!(
        "cannot parse connect string \"{connect_string}\""
    ))
}

/// Builds a two-line snippet: a window of the input around `char_offset` and a
/// caret `^` underneath the offending character.
fn context_snippet(trimmed: &str, char_offset: usize) -> String {
    let chars: Vec<char> = trimmed.chars().collect();
    let start = char_offset.saturating_sub(20);
    let end = (char_offset + 20).min(chars.len());
    let window: String = chars[start..end].iter().collect();
    let caret_pos = char_offset - start;
    let mut caret = String::new();
    for _ in 0..caret_pos {
        caret.push(' ');
    }
    caret.push('^');
    format!("  {window}\n  {caret}")
}

// ---------------------------------------------------------------------------
// Descriptor argument tree
// ---------------------------------------------------------------------------

/// A parsed value in the descriptor argument tree: either a simple string or a
/// nested key/value map (a parenthesised sub-node).
#[derive(Clone, Debug)]
enum ArgValue {
    Simple(String),
    Node(ArgMap),
}

/// A descriptor node: maps lower-cased keys to one or more values. The reference
/// stores repeated keys as a Python list; we model that as a `Vec` per key.
#[derive(Clone, Debug, Default)]
struct ArgMap {
    entries: Vec<(String, Vec<ArgValue>)>,
}

impl ArgMap {
    fn get(&self, key: &str) -> Option<&Vec<ArgValue>> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    fn take(&mut self, key: &str) -> Option<Vec<ArgValue>> {
        if let Some(idx) = self.entries.iter().position(|(k, _)| k == key) {
            Some(self.entries.remove(idx).1)
        } else {
            None
        }
    }

    fn push(&mut self, key: String, value: ArgValue) {
        if let Some((_, values)) = self.entries.iter_mut().find(|(k, _)| *k == key) {
            values.push(value);
        } else {
            self.entries.push((key, vec![value]));
        }
    }
}

/// Alternative parameter names accepted inside descriptors (reference
/// `ALTERNATIVE_PARAM_NAMES`): the listener keyword maps to the canonical key.
fn canonical_param_name(name: &str) -> &str {
    match name {
        "pool_connection_class" => "cclass",
        "pool_purity" => "purity",
        "server" => "server_type",
        "transport_connect_timeout" => "tcp_connect_timeout",
        "my_wallet_directory" => "wallet_location",
        other => other,
    }
}

/// Container keywords that may not take a simple (non-parenthesised) value
/// (reference `CONTAINER_PARAM_NAMES`).
fn is_container_param(name: &str) -> bool {
    matches!(
        name,
        "address"
            | "address_list"
            | "connect_data"
            | "description"
            | "description_list"
            | "security"
    )
}

// ---------------------------------------------------------------------------
// Descriptor tokenizer / recursive-descent parser
// ---------------------------------------------------------------------------

/// Recursive-descent parser for TNS connect descriptors. Mirrors the reference
/// `ConnectStringParser` (`_parse_descriptor_key_value_pair`): it tokenises
/// keywords, simple values, and quoted strings while tracking nested parens.
/// Maximum nesting depth for a TNS connect descriptor. Real topologies
/// (DESCRIPTION_LIST > DESCRIPTION > ADDRESS_LIST > ADDRESS / CONNECT_DATA >
/// SECURITY ...) are well under 10 deep; 128 is far beyond any legitimate
/// descriptor. The cap converts an attacker/garbage deeply-nested input into a
/// clean `Result::Err` instead of unbounded recursion that overflows the stack
/// and ABORTS the process (an uncatchable crash, not a recoverable panic) —
/// bead rust-oracledb-uf8.
const MAX_DESCRIPTOR_DEPTH: usize = 128;

struct DescriptorParser<'a> {
    chars: &'a [char],
    raw: &'a str,
    /// Confirmed cursor (chars consumed).
    pos: usize,
    /// Lookahead cursor.
    temp_pos: usize,
    /// Current parenthesis nesting depth (guards against stack overflow).
    depth: usize,
}

impl<'a> DescriptorParser<'a> {
    fn new(chars: &'a [char], raw: &'a str) -> Self {
        Self {
            chars,
            raw,
            pos: 0,
            temp_pos: 0,
            depth: 0,
        }
    }

    fn current(&self) -> char {
        self.chars[self.temp_pos]
    }

    fn skip_spaces(&mut self) {
        while self.temp_pos < self.chars.len() && self.chars[self.temp_pos].is_whitespace() {
            self.temp_pos += 1;
        }
    }

    /// Parses a keyword: alphanumeric plus `_` and `.` (reference
    /// `parse_keyword`).
    fn parse_keyword(&mut self) {
        while self.temp_pos < self.chars.len() {
            let ch = self.current();
            if !ch.is_alphanumeric() && ch != '_' && ch != '.' {
                break;
            }
            self.temp_pos += 1;
        }
    }

    /// Parses a quoted string body, consuming the closing quote (reference
    /// `parse_quoted_string`). On entry `temp_pos` is just past the opening
    /// quote.
    fn parse_quoted_string(&mut self, quote: char) -> Result<()> {
        while self.temp_pos < self.chars.len() {
            let ch = self.current();
            self.temp_pos += 1;
            if ch == quote {
                self.pos = self.temp_pos;
                return Ok(());
            }
        }
        let reason = if quote == '\'' {
            "missing ending quote (')"
        } else {
            "missing ending quote (\")"
        };
        Err(err_descriptor(self.raw, self.temp_pos, reason))
    }

    /// Parses a top-level descriptor node. On entry the opening `(` has already
    /// been consumed (reference `_parse_descriptor` calls
    /// `_parse_descriptor_key_value_pair` once on the implicit root).
    fn parse_descriptor(&mut self) -> Result<ArgMap> {
        let mut args = ArgMap::default();
        self.parse_key_value_pair(&mut args)?;
        Ok(args)
    }

    /// Parses one `(KEY=VALUE)` pair into `args`. Assumes the opening `(` for
    /// this pair was already consumed. Directly mirrors the reference
    /// `_parse_descriptor_key_value_pair`.
    fn parse_key_value_pair(&mut self, args: &mut ArgMap) -> Result<()> {
        let mut is_simple_value = false;
        let mut simple_start = 0usize;
        let mut value: Option<ArgValue> = None;

        // parse keyword
        self.skip_spaces();
        let start_pos = self.temp_pos;
        self.parse_keyword();
        if self.temp_pos == start_pos {
            return Err(err_descriptor(
                self.raw,
                self.temp_pos,
                "expected a keyword",
            ));
        }
        let raw_name: String = self.chars[start_pos..self.temp_pos]
            .iter()
            .collect::<String>()
            .to_ascii_lowercase();
        let name = canonical_param_name(&raw_name).to_string();

        // look for equals sign
        self.skip_spaces();
        let mut ch = '\0';
        if self.temp_pos < self.chars.len() {
            ch = self.current();
        }
        if ch != '=' {
            return Err(err_descriptor(
                self.raw,
                self.temp_pos,
                "expected '=' after keyword",
            ));
        }
        self.temp_pos += 1;
        self.skip_spaces();

        // parse value
        while self.temp_pos < self.chars.len() {
            ch = self.current();
            if ch == '"' {
                if is_simple_value {
                    return Err(err_descriptor(
                        self.raw,
                        self.temp_pos,
                        "unexpected quote inside a simple value",
                    ));
                }
                self.temp_pos += 1;
                let q_start = self.temp_pos;
                self.parse_quoted_string('"')?;
                if self.temp_pos > q_start + 1 {
                    let v: String = self.chars[q_start..self.temp_pos - 1].iter().collect();
                    value = Some(ArgValue::Simple(v));
                }
                break;
            } else if ch == '(' {
                if is_simple_value {
                    return Err(err_descriptor(
                        self.raw,
                        self.temp_pos,
                        "unexpected '(' inside a simple value",
                    ));
                }
                self.temp_pos += 1;
                let mut node = match value.take() {
                    Some(ArgValue::Node(n)) => n,
                    _ => ArgMap::default(),
                };
                self.depth += 1;
                if self.depth > MAX_DESCRIPTOR_DEPTH {
                    return Err(err_descriptor(
                        self.raw,
                        self.temp_pos,
                        "connect descriptor nesting too deep",
                    ));
                }
                let result = self.parse_key_value_pair(&mut node);
                self.depth -= 1;
                result?;
                value = Some(ArgValue::Node(node));
                continue;
            } else if ch == ')' {
                break;
            } else if !is_simple_value && !ch.is_whitespace() {
                if value.is_some() || is_container_param(&name) {
                    return Err(err_descriptor(
                        self.raw,
                        self.temp_pos,
                        "unexpected simple value for a container keyword",
                    ));
                }
                simple_start = self.temp_pos;
                is_simple_value = true;
            }
            self.temp_pos += 1;
        }
        if is_simple_value {
            let v: String = self.chars[simple_start..self.temp_pos]
                .iter()
                .collect::<String>()
                .trim()
                .to_string();
            value = Some(ArgValue::Simple(v));
        }
        self.skip_spaces();
        if self.temp_pos < self.chars.len() {
            ch = self.current();
            if ch != ')' {
                return Err(err_descriptor(
                    self.raw,
                    self.temp_pos,
                    "expected ')' to close the keyword",
                ));
            }
            self.temp_pos += 1;
        } else {
            return Err(err_descriptor(
                self.raw,
                self.temp_pos,
                "unbalanced parenthesis: expected ')'",
            ));
        }
        self.skip_spaces();
        self.pos = self.temp_pos;

        if let Some(value) = value {
            self.set_descriptor_arg(args, name, value);
        }
        Ok(())
    }

    /// Stores a value in `args`, mirroring the reference `_set_descriptor_arg`
    /// special handling for `address` vs `address_list` interleaving.
    fn set_descriptor_arg(&self, args: &mut ArgMap, name: String, value: ArgValue) {
        if args.get(&name).is_none() {
            if name == "address" && args.get("address_list").is_some() {
                let mut wrapper = ArgMap::default();
                wrapper.push("address".to_string(), value);
                self.set_descriptor_arg(args, "address_list".to_string(), ArgValue::Node(wrapper));
                return;
            } else if name == "address_list" && args.get("address").is_some() {
                let addresses = args.take("address").unwrap_or_default();
                // existing addresses become their own address_list nodes,
                // preserving order before the new list.
                for addr in addresses {
                    let mut wrapper = ArgMap::default();
                    wrapper.push("address".to_string(), addr);
                    args.push("address_list".to_string(), ArgValue::Node(wrapper));
                }
                args.push(name, value);
                return;
            }
            args.push(name, value);
        } else {
            args.push(name, value);
        }
    }
}

// ---------------------------------------------------------------------------
// tnsnames.ora parsing
// ---------------------------------------------------------------------------

/// Parses `tnsnames.ora` files into an alias -> connect-descriptor map.
///
/// Mirrors the reference `TnsnamesFileParser` / `TnsnamesFileReader`:
/// comment (`#`) handling, multi-line paren-balanced values, comma-separated
/// alias lists, and `IFILE` includes (resolved relative to the including file's
/// directory) with cycle detection. Aliases are upper-cased; the last
/// definition of a duplicate alias wins.
pub mod tnsnames;

mod builders;
use builders::build_descriptor;
#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(input: &str) -> Descriptor {
        parse(input)
            .unwrap_or_else(|e| panic!("parse({input:?}) should succeed but failed: {e}"))
            .unwrap_or_else(|| panic!("parse({input:?}) should be a descriptor, not a tns alias"))
    }

    /// Flattened host list across all descriptions/lists (host order),
    /// mirroring python-oracledb's `params.host` for the multi-address case.
    fn hosts(d: &Descriptor) -> Vec<String> {
        d.addresses().filter_map(|a| a.host.clone()).collect()
    }

    fn ports(d: &Descriptor) -> Vec<u16> {
        d.addresses().map(|a| a.port).collect()
    }

    fn protocols(d: &Descriptor) -> Vec<Protocol> {
        d.addresses().map(|a| a.protocol).collect()
    }

    #[test]
    fn parses_simple_name_value_descriptor() {
        // reference test_4503
        let d = parse_ok(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=my_host4)(PORT=1589))\
             (CONNECT_DATA=(SERVICE_NAME=my_service_name4)))",
        );
        let addr = d.first_address().expect("descriptor has an address");
        assert_eq!(addr.host.as_deref(), Some("my_host4"));
        assert_eq!(addr.port, 1589);
        assert_eq!(addr.protocol, Protocol::Tcp);
        assert_eq!(
            d.first_description().connect_data.service_name.as_deref(),
            Some("my_service_name4")
        );
    }

    // --- EZConnect / EZConnect-Plus -------------------------------------

    #[test]
    fn parses_easy_connect_with_port() {
        // reference test_4500
        let d = parse_ok("my_host:1578/my_service_name");
        let a = d.first_address().unwrap();
        assert_eq!(a.host.as_deref(), Some("my_host"));
        assert_eq!(a.port, 1578);
        assert_eq!(
            d.first_description().connect_data.service_name.as_deref(),
            Some("my_service_name")
        );
    }

    #[test]
    fn parses_easy_connect_default_port() {
        // reference test_4501
        let d = parse_ok("my_host2/my_service_name2");
        let a = d.first_address().unwrap();
        assert_eq!(a.host.as_deref(), Some("my_host2"));
        assert_eq!(a.port, 1521);
    }

    #[test]
    fn parses_easy_connect_drcp_server_type() {
        // reference test_4502
        let d = parse_ok("my_host3.org/my_service_name3:pooled");
        assert_eq!(
            d.first_description().connect_data.server_type,
            Some(ServerType::Pooled)
        );
        let d = parse_ok("my_host3/my_service_name3:ShArEd");
        assert_eq!(
            d.first_description().connect_data.server_type,
            Some(ServerType::Shared)
        );
    }

    #[test]
    fn parses_easy_connect_tcps_protocol() {
        // reference test_4504
        let d = parse_ok("tcps://my_host6/my_service_name6");
        assert_eq!(d.first_address().unwrap().protocol, Protocol::Tcps);
    }

    #[test]
    fn parses_easy_connect_no_service() {
        // reference test_4512
        let d = parse_ok("my_host15:1578/");
        let a = d.first_address().unwrap();
        assert_eq!(a.host.as_deref(), Some("my_host15"));
        assert_eq!(a.port, 1578);
        assert!(d.first_description().connect_data.service_name.is_none());
    }

    #[test]
    fn parses_easy_connect_missing_port_value() {
        // reference test_4513
        let d = parse_ok("my_host17:/my_service_name17");
        let a = d.first_address().unwrap();
        assert_eq!(a.host.as_deref(), Some("my_host17"));
        assert_eq!(a.port, 1521);
        assert_eq!(
            d.first_description().connect_data.service_name.as_deref(),
            Some("my_service_name17")
        );
    }

    #[test]
    fn parses_easy_connect_ipv6() {
        // reference test_4547
        let d = parse_ok("[::1]:4547/service_name_4547");
        let a = d.first_address().unwrap();
        assert_eq!(a.host.as_deref(), Some("::1"));
        assert_eq!(a.port, 4547);
        assert_eq!(
            d.first_description().connect_data.service_name.as_deref(),
            Some("service_name_4547")
        );
    }

    #[test]
    fn parses_easy_connect_multiple_hosts_different_ports() {
        // reference test_4548
        let d = parse_ok("host4548a,host4548b:4548,host4548c,host4548d:4549/service_name_4548");
        assert_eq!(
            hosts(&d),
            vec!["host4548a", "host4548b", "host4548c", "host4548d"]
        );
        assert_eq!(ports(&d), vec![4548, 4548, 4549, 4549]);
    }

    #[test]
    fn parses_easy_connect_multiple_address_lists() {
        // reference test_4549
        let d = parse_ok("host4549a;host4549b,host4549c:4549;host4549d/service_name_4549");
        assert_eq!(
            hosts(&d),
            vec!["host4549a", "host4549b", "host4549c", "host4549d"]
        );
        assert_eq!(ports(&d), vec![1521, 4549, 4549, 1521]);
    }

    #[test]
    fn parses_easy_connect_degenerate_protocol() {
        // reference test_4552
        let d = parse_ok("//host_4552:4552/service_name_4552");
        let a = d.first_address().unwrap();
        assert_eq!(a.host.as_deref(), Some("host_4552"));
        assert_eq!(a.port, 4552);
    }

    #[test]
    fn parses_easy_connect_instance_name() {
        // reference test_4571
        let d = parse_ok("host_4571:4571/service_4571/instance_4571");
        assert_eq!(
            d.first_description().connect_data.instance_name.as_deref(),
            Some("instance_4571")
        );
        assert_eq!(
            d.first_description().connect_data.service_name.as_deref(),
            Some("service_4571")
        );
    }

    #[test]
    fn parses_easy_connect_extended_params() {
        // reference test_4517
        let d = parse_ok(
            "my_host21/my_server_name21?expire_time=5&retry_delay=10&retry_count=12&transport_connect_timeout=2.5",
        );
        let desc = d.first_description();
        assert_eq!(desc.expire_time, 5);
        assert_eq!(desc.retry_delay, 10);
        assert_eq!(desc.retry_count, 12);
        assert!((desc.tcp_connect_timeout - 2.5).abs() < 1e-9);
    }

    #[test]
    fn parses_easy_connect_security_params() {
        // reference test_4582
        let d = parse_ok(
            "tcps://host_4580:4580/service_4580?ssl_server_dn_match=true&ssl_server_cert_dn='cn=sales'&wallet_location='/tmp/oracle'",
        );
        // Single quotes are preserved verbatim in EZConnect-Plus params
        // (only double quotes are stripped) — matches reference test_4582,
        // whose get_connect_string() keeps the single quotes.
        let sec = &d.first_description().security;
        assert!(sec.ssl_server_dn_match);
        assert_eq!(sec.ssl_server_cert_dn.as_deref(), Some("'cn=sales'"));
        assert_eq!(sec.wallet_location.as_deref(), Some("'/tmp/oracle'"));
    }

    #[test]
    fn rejects_invalid_protocol_in_easy_connect() {
        // reference test_4505
        let err = parse("invalid_proto://my_host7/my_service_name7").unwrap_err();
        assert!(format!("{err}").contains("invalid protocol"));
    }

    // --- diagnostics ----------------------------------------------------

    #[test]
    fn diagnostic_points_at_unbalanced_paren() {
        let err = parse("(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1521))").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("offset"), "expected offset in: {msg}");
        assert!(msg.contains('^'), "expected caret context in: {msg}");
    }

    #[test]
    fn diagnostic_for_missing_addresses() {
        // reference test_4546 (wrong container names -> no addresses)
        let err = parse(
            "(DESRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVICE_NAME=s)))",
        )
        .unwrap_err();
        assert!(format!("{err}").contains("no addresses are defined"));
    }

    #[test]
    fn protocol_default_port_resolves_for_unported_address() {
        let d = parse_ok("tcps://h/svc");
        assert_eq!(d.first_address().unwrap().port, 2484);
    }

    #[test]
    fn describe_dumps_addresses() {
        let d = parse_ok(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=h1)(PORT=1521))\
             (CONNECT_DATA=(SERVICE_NAME=svc)))",
        );
        let text = d.describe();
        assert!(text.contains("tcp://h1:1521"));
        assert!(text.contains("service_name=svc"));
    }

    #[test]
    fn keeps_protocols_for_multi_list_descriptor() {
        // reference test_4522
        let d = parse_ok(
            "(DESCRIPTION=(LOAD_BALANCE=ON)(RETRY_COUNT=5)(RETRY_DELAY=2)\
             (ADDRESS_LIST=(LOAD_BALANCE=ON)\
             (ADDRESS=(PROTOCOL=tcp)(PORT=1521)(HOST=my_host26))\
             (ADDRESS=(PROTOCOL=tcp)(PORT=222)(HOST=my_host27)))\
             (ADDRESS_LIST=(LOAD_BALANCE=ON)\
             (ADDRESS=(PROTOCOL=tcps)(PORT=5555)(HOST=my_host28))\
             (ADDRESS=(PROTOCOL=tcps)(PORT=444)(HOST=my_host29)))\
             (CONNECT_DATA=(SERVICE_NAME=my_service_name26)))",
        );
        assert_eq!(
            hosts(&d),
            vec!["my_host26", "my_host27", "my_host28", "my_host29"]
        );
        assert_eq!(ports(&d), vec![1521, 222, 5555, 444]);
        assert_eq!(
            protocols(&d),
            vec![Protocol::Tcp, Protocol::Tcp, Protocol::Tcps, Protocol::Tcps]
        );
    }

    #[test]
    fn parses_multiple_descriptions() {
        // reference test_4523 (host ordering across descriptions)
        let d = parse_ok(
            "(DESCRIPTION_LIST=(FAIL_OVER=ON)(LOAD_BALANCE=OFF)\
             (DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(PORT=5001)(HOST=my_host30))\
             (ADDRESS=(PROTOCOL=tcp)(PORT=1521)(HOST=my_host31)))\
             (CONNECT_DATA=(SERVICE_NAME=svc27)))\
             (DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(PORT=5002)(HOST=my_host34)))\
             (CONNECT_DATA=(SERVICE_NAME=svc28))))",
        );
        assert_eq!(hosts(&d), vec!["my_host30", "my_host31", "my_host34"]);
        assert_eq!(d.descriptions.len(), 2);
    }

    #[test]
    fn interleaves_address_and_address_list_small_first() {
        // reference test_4529
        let d = parse_ok(
            "(DESCRIPTION=\
             (ADDRESS=(PROTOCOL=tcp)(HOST=host1)(PORT=1521))\
             (ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=host2a)(PORT=1522))\
             (ADDRESS=(PROTOCOL=tcp)(HOST=host2b)(PORT=1523)))\
             (ADDRESS=(PROTOCOL=tcp)(HOST=host3)(PORT=1524))\
             (CONNECT_DATA=(SERVICE_NAME=svc)))",
        );
        assert_eq!(hosts(&d), vec!["host1", "host2a", "host2b", "host3"]);
    }

    // --- corpus-differential table (valid inputs) -----------------------

    /// Each row: (connect_string, first_host, first_port, service_name option,
    /// first_protocol). Drives a broad differential sweep matching the
    /// reference's parse results across EZConnect and descriptor forms.
    #[test]
    fn corpus_valid_inputs() {
        let cases: &[(&str, &str, u16, Option<&str>, Protocol)] = &[
            // EZConnect family
            ("h/s", "h", 1521, Some("s"), Protocol::Tcp),
            ("h:1600/s", "h", 1600, Some("s"), Protocol::Tcp),
            ("tcp://h/s", "h", 1521, Some("s"), Protocol::Tcp),
            ("tcps://h/s", "h", 2484, Some("s"), Protocol::Tcps),
            ("tcps://h:9999/s", "h", 9999, Some("s"), Protocol::Tcps),
            ("h.example.org/s.dom", "h.example.org", 1521, Some("s.dom"), Protocol::Tcp),
            ("h:1521/", "h", 1521, None, Protocol::Tcp),
            ("h:/s", "h", 1521, Some("s"), Protocol::Tcp),
            ("[2001:db8::1]:1521/s", "2001:db8::1", 1521, Some("s"), Protocol::Tcp),
            ("[::1]/s", "::1", 1521, Some("s"), Protocol::Tcp),
            ("//h:1521/s", "h", 1521, Some("s"), Protocol::Tcp),
            ("h1,h2:1700/s", "h1", 1700, Some("s"), Protocol::Tcp),
            ("h/s:dedicated", "h", 1521, Some("s"), Protocol::Tcp),
            ("h/s/inst", "h", 1521, Some("s"), Protocol::Tcp),
            ("h/s?sdu=16384", "h", 1521, Some("s"), Protocol::Tcp),
            ("h/s?pyo.stmtcachesize=40", "h", 1521, Some("s"), Protocol::Tcp),
            // descriptor family
            (
                "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=dh)(PORT=1599))(CONNECT_DATA=(SERVICE_NAME=ds)))",
                "dh",
                1599,
                Some("ds"),
                Protocol::Tcp,
            ),
            (
                "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=sh)(PORT=2484))(CONNECT_DATA=(SID=mysid)))",
                "sh",
                2484,
                None,
                Protocol::Tcps,
            ),
            (
                "(DESCRIPTION =(ADDRESS=(PROTOCOL=tcp) (HOST = wh) (PORT = 1521))(CONNECT_DATA=(SERVICE_NAME=ws)))",
                "wh",
                1521,
                Some("ws"),
                Protocol::Tcp,
            ),
            (
                "(DESCRIPTION=(ADDRESS=(HTTPS_PROXY=px)(HTTPS_PROXY_PORT=8080)(PROTOCOL=tcps)(HOST=ph)(PORT=443))(CONNECT_DATA=(SERVICE_NAME=ps)))",
                "ph",
                443,
                Some("ps"),
                Protocol::Tcps,
            ),
        ];
        for (cs, host, port, service, protocol) in cases {
            let d = parse_ok(cs);
            let a = d
                .first_address()
                .unwrap_or_else(|| panic!("no address for {cs:?}"));
            assert_eq!(a.host.as_deref(), Some(*host), "host mismatch for {cs:?}");
            assert_eq!(a.port, *port, "port mismatch for {cs:?}");
            assert_eq!(a.protocol, *protocol, "protocol mismatch for {cs:?}");
            assert_eq!(
                d.first_description().connect_data.service_name.as_deref(),
                *service,
                "service mismatch for {cs:?}"
            );
        }
    }

    /// Each row: (connect_string, expected substring in the diagnostic).
    #[test]
    fn corpus_malformed_inputs() {
        let cases: &[(&str, &str)] = &[
            // unbalanced / structural
            (
                "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1)",
                "offset",
            ),
            ("(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp", "offset"),
            // missing addresses (reference DPY-2049)
            (
                "(DESRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVICE_NAME=s)))",
                "no addresses are defined",
            ),
            // invalid protocol (reference DPY-4021)
            ("badproto://h/s", "invalid protocol"),
            (
                "(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=k))(CONNECT_DATA=(SERVICE_NAME=s)))",
                "invalid protocol",
            ),
            // invalid server type (reference DPY-4028)
            (
                "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVER=BOGUS)(SERVICE_NAME=s)))",
                "invalid server_type",
            ),
            // non-numeric RETRY_COUNT (reference DPY-4018)
            (
                "(DESCRIPTION=(RETRY_COUNT=wrong)(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVICE_NAME=s)))",
                "not a non-negative integer",
            ),
            // simple value for a container keyword (reference DPY-4017)
            ("(address=5)", "container"),
            // mixed complex/simple data (reference DPY-4017)
            (
                "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVER=DEDICATED) SERVICE_NAME=s))",
                "offset",
            ),
            // empty
            ("", "must not be empty"),
        ];
        for (cs, needle) in cases {
            let err = parse(cs)
                .err()
                .unwrap_or_else(|| panic!("expected error for {cs:?}"));
            let msg = format!("{err}");
            assert!(
                msg.contains(needle),
                "diagnostic for {cs:?} = {msg:?} should contain {needle:?}"
            );
        }
    }

    #[test]
    fn tns_alias_returns_none() {
        // A bare alphanumeric name is neither a descriptor nor an EZConnect
        // string; it must resolve via tnsnames.ora (parse returns None).
        assert!(parse("my_tns_alias")
            .expect("alias is not an error")
            .is_none());
    }

    #[test]
    fn sdu_is_clamped() {
        // reference: SDU sanitised into 512..=2097152
        let d = parse_ok("(DESCRIPTION=(SDU=1)(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVICE_NAME=s)))");
        assert_eq!(d.first_description().sdu, 512);
        let d = parse_ok("(DESCRIPTION=(SDU=99999999)(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVICE_NAME=s)))");
        assert_eq!(d.first_description().sdu, 2_097_152);
    }

    #[test]
    fn duration_units_parse() {
        // reference test_4511
        let base = "(DESCRIPTION=(TRANSPORT_CONNECT_TIMEOUT=UNIT)(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVICE_NAME=s)))";
        let cases = [
            ("500 ms", 0.5_f64),
            ("15 SEC", 15.0),
            ("5 min", 300.0),
            ("34", 34.0),
        ];
        for (unit, expected) in cases {
            let d = parse_ok(&base.replace("UNIT", unit));
            assert!(
                (d.first_description().tcp_connect_timeout - expected).abs() < 1e-9,
                "duration {unit:?} -> {}",
                d.first_description().tcp_connect_timeout
            );
        }
    }

    #[test]
    fn passthrough_extras_preserved_in_connect_data() {
        // reference test_4579 — unknown CONNECT_DATA keys are passed through.
        let d = parse_ok(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVICE_NAME=s)(COLOCATION_TAG=Tag1)))",
        );
        let extra = &d.first_description().connect_data.extra;
        assert!(extra
            .iter()
            .any(|(k, v)| k == "COLOCATION_TAG" && v == "Tag1"));
    }

    #[test]
    fn wallet_and_cert_dn_in_security() {
        // reference test_4515
        let d = parse_ok(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVICE_NAME=s))\
             (SECURITY=(SSL_SERVER_CERT_DN=\"CN=unknown\")(SSL_SERVER_DN_MATCH=Off)(MY_WALLET_DIRECTORY=\"/tmp/w\")))",
        );
        let sec = &d.first_description().security;
        assert_eq!(sec.ssl_server_cert_dn.as_deref(), Some("CN=unknown"));
        assert_eq!(sec.wallet_location.as_deref(), Some("/tmp/w"));
        assert!(!sec.ssl_server_dn_match);
    }
}
