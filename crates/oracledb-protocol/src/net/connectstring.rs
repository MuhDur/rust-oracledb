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
mod easy_connect {
    use super::*;

    /// Private sentinel keys used to stash `https_proxy` host/port until the
    /// address lists are assembled (these never reach the public `extra` list).
    const PROXY_HOST_KEY: &str = "\0https_proxy_host";
    const PROXY_PORT_KEY: &str = "\0https_proxy_port";

    /// Common EZConnect-Plus parameters recognised by all drivers (reference
    /// `COMMON_PARAM_NAMES`); the value is the canonical name.
    fn is_common_param(name: &str) -> bool {
        matches!(
            name,
            "expire_time"
                | "failover"
                | "https_proxy"
                | "https_proxy_port"
                | "load_balance"
                | "pool_boundary"
                | "pool_name"
                | "pool_connection_class"
                | "pool_purity"
                | "retry_count"
                | "retry_delay"
                | "sdu"
                | "source_route"
                | "ssl_server_cert_dn"
                | "ssl_server_dn_match"
                | "transport_connect_timeout"
                | "use_sni"
                | "wallet_location"
        )
    }

    /// Extra DESCRIPTION params passed through when seen in an easy connect
    /// string (reference `EXTRA_DESCRIPTION_PARAM_NAMES`).
    fn is_extra_description_param(name: &str) -> bool {
        matches!(name, "enable" | "recv_buf_size" | "send_buf_size")
    }

    fn is_host_or_service_char(ch: char) -> bool {
        ch.is_alphanumeric() || matches!(ch, '-' | '_' | '.')
    }

    /// Parser state for an EZConnect string.
    struct Ez<'a> {
        chars: &'a [char],
        pos: usize,
        temp_pos: usize,
    }

    impl<'a> Ez<'a> {
        fn current(&self) -> char {
            self.chars[self.temp_pos]
        }

        fn skip_spaces(&mut self) {
            while self.temp_pos < self.chars.len() && self.chars[self.temp_pos].is_whitespace() {
                self.temp_pos += 1;
            }
        }

        fn parse_keyword(&mut self) {
            while self.temp_pos < self.chars.len() {
                let ch = self.current();
                if !ch.is_alphanumeric() && ch != '_' && ch != '.' {
                    break;
                }
                self.temp_pos += 1;
            }
        }

        /// Parses an optional `proto://` prefix. Returns the protocol keyword
        /// (lower-cased) if one was found, advancing `pos` past the `//`.
        /// Mirrors `_parse_easy_connect_protocol`.
        fn parse_protocol(&mut self) -> Option<String> {
            let mut start_sep_pos = self.pos;
            let mut num_sep_chars = 0i32;
            let mut protocol: Option<String> = None;
            self.temp_pos = self.pos;
            while self.temp_pos < self.chars.len() {
                let ch = self.current();
                if ch == ':' {
                    protocol = Some(
                        self.chars[self.pos..self.temp_pos]
                            .iter()
                            .collect::<String>()
                            .to_ascii_lowercase(),
                    );
                    start_sep_pos = self.temp_pos + 1;
                } else if ch == '/' && (self.temp_pos - start_sep_pos) as i32 == num_sep_chars {
                    num_sep_chars += 1;
                    if num_sep_chars == 2 {
                        self.temp_pos += 1;
                        self.pos = self.temp_pos;
                        break;
                    }
                } else if !ch.is_alphabetic() && ch != '-' && ch != '_' {
                    break;
                }
                self.temp_pos += 1;
            }
            if protocol.is_some() && num_sep_chars == 2 {
                protocol
            } else {
                None
            }
        }

        /// Parses one host (optionally bracketed IPv6). Mirrors
        /// `_parse_easy_connect_host`.
        fn parse_host(&mut self, address: &mut Address) {
            let mut found_bracket = false;
            let mut found_host = false;
            let mut start_pos = self.temp_pos;
            while self.temp_pos < self.chars.len() {
                let ch = self.current();
                if !found_bracket && !found_host && ch == '[' {
                    found_bracket = true;
                    start_pos = self.temp_pos + 1;
                } else if found_bracket && ch == ']' {
                    address.host = Some(self.chars[start_pos..self.temp_pos].iter().collect());
                    self.temp_pos += 1;
                    self.pos = self.temp_pos;
                    break;
                } else if found_bracket || is_host_or_service_char(ch) {
                    self.temp_pos += 1;
                    found_host = true;
                } else {
                    if found_host {
                        address.host = Some(self.chars[start_pos..self.temp_pos].iter().collect());
                        self.pos = self.temp_pos;
                    }
                    break;
                }
            }
            // Handle a host that runs to end-of-string.
            if found_host && self.temp_pos == self.chars.len() && address.host.is_none() {
                address.host = Some(self.chars[start_pos..self.temp_pos].iter().collect());
                self.pos = self.temp_pos;
            }
        }

        /// Parses a port number. Mirrors `_parse_easy_connect_port`.
        fn parse_port(&mut self, address: &mut Address) {
            let start = self.temp_pos;
            let mut found = false;
            while self.temp_pos < self.chars.len() && self.current().is_ascii_digit() {
                found = true;
                self.temp_pos += 1;
            }
            if found {
                let digits: String = self.chars[start..self.temp_pos].iter().collect();
                if let Ok(port) = digits.parse::<u16>() {
                    address.port = port;
                }
            }
        }
    }

    /// Builds the host/address-list portion of an EZConnect string into a list
    /// of address lists, plus the description that owns them. Mirrors
    /// `_parse_easy_connect_hosts`.
    #[allow(clippy::too_many_lines)]
    pub(super) fn parse(chars: &[char], connect_string: &str) -> Result<Option<Descriptor>> {
        let mut ez = Ez {
            chars,
            pos: 0,
            temp_pos: 0,
        };

        // protocol prefix
        let template_protocol = match ez.parse_protocol() {
            Some(protocol) => Protocol::from_keyword(&protocol)?,
            None => Protocol::Tcp,
        };

        // Hosts: a series of host names separated by commas (same list) or
        // semicolons (new list).
        let mut address_lists: Vec<Vec<Address>> = Vec::new();
        let mut current_list: Vec<Address> = Vec::new();
        ez.temp_pos = ez.pos;
        let mut port_index = 0usize;
        loop {
            let mut address = Address {
                protocol: template_protocol,
                port: template_protocol.default_port(),
                ..Address::default()
            };
            ez.parse_host(&mut address);
            // No host consumed and not at end: stop (no more hosts).
            if ez.temp_pos != ez.pos || ez.pos >= chars.len() {
                // If a host was parsed and we're at end, it was committed by
                // parse_host setting pos == temp_pos == len.
                if ez.pos >= chars.len() && address.host.is_some() {
                    current_list.push(address);
                }
                break;
            }
            ez.pos = ez.temp_pos;
            current_list.push(address);
            if ez.temp_pos >= chars.len() {
                break;
            }
            let mut ch = ez.current();
            if ch == ':' {
                ez.temp_pos += 1;
                if let Some(last) = current_list.last_mut() {
                    ez.parse_port(last);
                    let port = last.port;
                    ez.pos = ez.temp_pos;
                    if ez.pos >= chars.len() {
                        break;
                    }
                    // Back-fill the port onto earlier hosts in this list that
                    // had no explicit port (reference port_index loop).
                    let upper = current_list.len() - 1;
                    for addr in current_list.iter_mut().take(upper).skip(port_index) {
                        addr.port = port;
                    }
                    port_index = current_list.len();
                }
                ch = ez.current();
            }
            if ch == ';' {
                address_lists.push(std::mem::take(&mut current_list));
                port_index = 0;
            } else if ch != ',' {
                break;
            }
            ez.temp_pos += 1;
        }
        address_lists.push(current_list);

        // service name / server type, then instance name, then parameters.
        let mut description = Description::default();
        let mut found_service_section = false;
        parse_service_name(&mut ez, chars, &mut description, &mut found_service_section);
        if found_service_section {
            parse_instance_name(&mut ez, chars, &mut description);
        }

        // If no `/` was ever seen, this is not a valid EZConnect string — it is
        // a tnsnames.ora alias to resolve separately (reference returns None).
        if !found_service_section {
            return Ok(None);
        }

        parse_parameters(&mut ez, chars, connect_string, &mut description)?;

        // Trailing data after a successful parse is an error.
        if ez.pos != chars.len() {
            if ez.pos > 0 {
                return Err(err_cannot_parse(connect_string));
            }
            return Ok(None);
        }

        // Assemble the descriptor: each non-empty host group becomes an address
        // list; a lone single list collapses into the description directly.
        let mut lists: Vec<AddressList> = Vec::new();
        for hosts in address_lists {
            if hosts.is_empty() {
                continue;
            }
            lists.push(AddressList {
                addresses: hosts,
                failover: true,
                ..AddressList::default()
            });
        }
        if lists.is_empty() {
            return Ok(None);
        }
        description.address_lists = lists;

        // Apply any stashed https_proxy host/port onto every address, then drop
        // the sentinel entries from `extra`.
        let proxy_host = description
            .extra
            .iter()
            .find(|(k, _)| k == PROXY_HOST_KEY)
            .map(|(_, v)| v.clone());
        let proxy_port = description
            .extra
            .iter()
            .find(|(k, _)| k == PROXY_PORT_KEY)
            .and_then(|(_, v)| v.parse::<u16>().ok());
        description
            .extra
            .retain(|(k, _)| k != PROXY_HOST_KEY && k != PROXY_PORT_KEY);
        if proxy_host.is_some() || proxy_port.is_some() {
            for list in &mut description.address_lists {
                for addr in &mut list.addresses {
                    if let Some(host) = &proxy_host {
                        addr.https_proxy = Some(host.clone());
                    }
                    if let Some(port) = proxy_port {
                        addr.https_proxy_port = port;
                    }
                }
            }
        }

        Ok(Some(Descriptor {
            descriptions: vec![description],
            load_balance: false,
            failover: true,
            source_route: false,
        }))
    }

    /// Mirrors `_parse_easy_connect_service_name`.
    fn parse_service_name(
        ez: &mut Ez,
        chars: &[char],
        description: &mut Description,
        found_slash_out: &mut bool,
    ) {
        let mut found_service_name = false;
        let mut found_server_type = false;
        let mut found_slash = false;
        let mut found_colon = false;
        let mut service_name_end_pos = 0usize;
        ez.temp_pos = ez.pos;
        while ez.temp_pos < chars.len() {
            let ch = ez.current();
            if !found_slash && ch == '/' {
                found_slash = true;
            } else if found_service_name && !found_colon && ch == ':' {
                found_colon = true;
            } else if found_slash && !found_colon && is_host_or_service_char(ch) {
                found_service_name = true;
                service_name_end_pos = ez.temp_pos + 1;
            } else if found_colon && ch.is_alphabetic() {
                found_server_type = true;
            } else {
                break;
            }
            ez.temp_pos += 1;
        }
        if found_service_name {
            description.connect_data.service_name =
                Some(chars[ez.pos + 1..service_name_end_pos].iter().collect());
        }
        if found_slash {
            ez.pos = ez.temp_pos;
            *found_slash_out = true;
        }
        if found_server_type {
            let value: String = chars[service_name_end_pos + 1..ez.temp_pos]
                .iter()
                .collect();
            if let Ok(server_type) = ServerType::from_keyword(&value) {
                description.connect_data.server_type = Some(server_type);
            }
        }
    }

    /// Mirrors `_parse_easy_connect_instance_name`.
    fn parse_instance_name(ez: &mut Ez, chars: &[char], description: &mut Description) {
        let mut found_instance_name = false;
        let mut found_slash = false;
        let mut instance_name_end_pos = 0usize;
        ez.temp_pos = ez.pos;
        while ez.temp_pos < chars.len() {
            let ch = ez.current();
            if !found_slash && ch == '/' {
                found_slash = true;
            } else if found_slash && is_host_or_service_char(ch) {
                found_instance_name = true;
                instance_name_end_pos = ez.temp_pos + 1;
            } else {
                break;
            }
            ez.temp_pos += 1;
        }
        if found_instance_name {
            description.connect_data.instance_name =
                Some(chars[ez.pos + 1..instance_name_end_pos].iter().collect());
            ez.pos = ez.temp_pos;
        }
    }

    /// Mirrors `_parse_easy_connect_parameters` + `_parse_easy_connect_parameter`.
    fn parse_parameters(
        ez: &mut Ez,
        chars: &[char],
        connect_string: &str,
        description: &mut Description,
    ) -> Result<()> {
        let mut expected_sep = '?';
        ez.temp_pos = ez.pos;
        while ez.temp_pos < chars.len() {
            let ch = ez.current();
            if ch != expected_sep {
                break;
            }
            expected_sep = '&';
            ez.temp_pos += 1;
            parse_one_parameter(ez, chars, connect_string, description)?;
        }
        Ok(())
    }

    fn parse_one_parameter(
        ez: &mut Ez,
        chars: &[char],
        connect_string: &str,
        description: &mut Description,
    ) -> Result<()> {
        // parameter name
        ez.skip_spaces();
        let start = ez.temp_pos;
        ez.parse_keyword();
        if ez.temp_pos == start || ez.temp_pos >= chars.len() {
            return Ok(());
        }
        let raw_name: String = chars[start..ez.temp_pos]
            .iter()
            .collect::<String>()
            .to_ascii_lowercase();
        let (name, keep) = if let Some(stripped) = raw_name.strip_prefix("pyo.") {
            (stripped.to_string(), true)
        } else {
            let keep = is_common_param(&raw_name) || is_extra_description_param(&raw_name);
            (canonical_param_name(&raw_name).to_string(), keep)
        };

        // equals sign
        ez.skip_spaces();
        if ez.temp_pos >= chars.len() {
            return Ok(());
        }
        if ez.current() != '=' {
            return Ok(());
        }
        ez.temp_pos += 1;

        // value
        ez.skip_spaces();
        let mut start_pos = ez.temp_pos;
        let mut end_pos = ez.temp_pos;
        while ez.temp_pos < chars.len() {
            let ch = ez.current();
            if ch == '"' {
                if ez.temp_pos > start_pos {
                    return Ok(());
                }
                ez.temp_pos += 1;
                start_pos = ez.temp_pos;
                // parse quoted string
                let mut closed = false;
                while ez.temp_pos < chars.len() {
                    let qc = ez.current();
                    ez.temp_pos += 1;
                    if qc == '"' {
                        closed = true;
                        break;
                    }
                }
                if !closed {
                    return Err(err_descriptor(
                        connect_string,
                        ez.temp_pos,
                        "missing ending quote (\")",
                    ));
                }
                end_pos = ez.temp_pos - 1;
                break;
            } else if ch == '&' {
                end_pos = ez.temp_pos;
                break;
            }
            ez.temp_pos += 1;
            end_pos = ez.temp_pos;
        }
        if end_pos > start_pos && keep {
            let value: String = chars[start_pos..end_pos].iter().collect();
            apply_easy_param(connect_string, description, &name, &value)?;
        }
        ez.skip_spaces();
        ez.pos = ez.temp_pos;
        Ok(())
    }

    /// Applies a recognised EZConnect-Plus parameter onto the description.
    fn apply_easy_param(
        connect_string: &str,
        description: &mut Description,
        name: &str,
        value: &str,
    ) -> Result<()> {
        match name {
            "expire_time" => {
                description.expire_time = parse_uint(connect_string, "EXPIRE_TIME", value)?
            }
            "retry_count" => {
                description.retry_count = parse_uint(connect_string, "RETRY_COUNT", value)?
            }
            "retry_delay" => {
                description.retry_delay = parse_uint(connect_string, "RETRY_DELAY", value)?
            }
            "sdu" => {
                description.sdu = parse_uint(connect_string, "SDU", value)?.clamp(MIN_SDU, MAX_SDU);
            }
            "tcp_connect_timeout" => {
                description.tcp_connect_timeout =
                    parse_duration(connect_string, "TRANSPORT_CONNECT_TIMEOUT", value)?;
            }
            "failover" => description.failover = parse_bool(value),
            "load_balance" => description.load_balance = parse_bool(value),
            "source_route" => description.source_route = parse_bool(value),
            "use_sni" => description.use_sni = parse_bool(value),
            "ssl_server_dn_match" => description.security.ssl_server_dn_match = parse_bool(value),
            "ssl_server_cert_dn" => {
                description.security.ssl_server_cert_dn = Some(value.to_string());
            }
            "wallet_location" => description.security.wallet_location = Some(value.to_string()),
            // https_proxy / https_proxy_port are applied to every address after
            // the address lists are assembled; they are stashed in `extra` under
            // a private sentinel key and consumed in `parse`.
            "https_proxy" => description
                .extra
                .push((PROXY_HOST_KEY.to_string(), value.to_string())),
            "https_proxy_port" => description
                .extra
                .push((PROXY_PORT_KEY.to_string(), value.to_string())),
            "pool_boundary" => description.connect_data.pool_boundary = Some(value.to_string()),
            "pool_name" => description.connect_data.pool_name = Some(value.to_string()),
            "cclass" => {
                if !value.is_empty() {
                    description.connect_data.cclass = Some(value.to_string());
                }
            }
            "purity" => {
                description.connect_data.purity = Some(Purity::from_keyword(value)?);
            }
            "enable" | "recv_buf_size" | "send_buf_size" => {
                description
                    .extra
                    .push((name.to_ascii_uppercase(), value.to_string()));
            }
            // Extended (`pyo.`) params not affecting the descriptor topology are
            // accepted but not modelled here (e.g. stmtcachesize, edition).
            _ => {}
        }
        Ok(())
    }
}

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
pub mod tnsnames {
    use crate::{ProtocolError, Result};
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};

    /// A fully resolved set of tnsnames.ora entries.
    #[derive(Debug, Default)]
    pub struct TnsnamesReader {
        /// Alias (upper-cased) -> connect descriptor/easy-connect string, in
        /// first-seen order.
        entries: Vec<(String, String)>,
        /// The path of the primary tnsnames.ora file (for diagnostics).
        file_name: PathBuf,
    }

    impl TnsnamesReader {
        /// Reads `tnsnames.ora` from `config_dir`, following `IFILE` includes.
        pub fn read(config_dir: &Path) -> Result<Self> {
            let primary = config_dir.join("tnsnames.ora");
            let mut reader = TnsnamesReader {
                entries: Vec::new(),
                file_name: primary.clone(),
            };
            let mut in_progress: Vec<PathBuf> = Vec::new();
            let mut seen: HashSet<PathBuf> = HashSet::new();
            reader.read_file(&primary, &mut in_progress, &mut seen)?;
            Ok(reader)
        }

        /// Looks up an alias (case-insensitive). Returns the connect string.
        #[must_use]
        pub fn get(&self, alias: &str) -> Option<&str> {
            let upper = alias.to_ascii_uppercase();
            self.entries
                .iter()
                .find(|(name, _)| *name == upper)
                .map(|(_, value)| value.as_str())
        }

        /// All known network service names (upper-cased), in first-seen order.
        #[must_use]
        pub fn service_names(&self) -> Vec<String> {
            self.entries.iter().map(|(name, _)| name.clone()).collect()
        }

        /// The path of the primary tnsnames.ora file.
        #[must_use]
        pub fn file_name(&self) -> &Path {
            &self.file_name
        }

        fn set_entry(&mut self, name: String, value: String) {
            // Last definition wins, but keep first-seen ordering: if the alias
            // already exists, overwrite its value in place.
            if let Some(slot) = self.entries.iter_mut().find(|(n, _)| *n == name) {
                slot.1 = value;
            } else {
                self.entries.push((name, value));
            }
        }

        fn read_file(
            &mut self,
            path: &Path,
            in_progress: &mut Vec<PathBuf>,
            seen: &mut HashSet<PathBuf>,
        ) -> Result<()> {
            let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
            if in_progress.contains(&canonical) {
                let including = in_progress
                    .last()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default();
                return Err(ProtocolError::InvalidConnectDescriptor(format!(
                    "file '{including}' includes file '{}', which forms a cycle",
                    path.display()
                )));
            }
            let contents = std::fs::read_to_string(path).map_err(|_| {
                ProtocolError::InvalidConnectDescriptor(format!(
                    "file '{}' is missing or unreadable",
                    path.display()
                ))
            })?;
            in_progress.push(canonical.clone());
            seen.insert(canonical);

            let dir = path.parent().unwrap_or_else(|| Path::new("."));
            // Collect entries first to avoid borrow conflicts during IFILE
            // recursion.
            let parsed = parse_file(&contents);
            for (key, value) in parsed {
                if key.eq_ignore_ascii_case("ifile") {
                    let mut inc = value.trim().to_string();
                    if inc.starts_with('"') && inc.ends_with('"') && inc.len() >= 2 {
                        inc = inc[1..inc.len() - 1].to_string();
                    }
                    let inc_path = if Path::new(&inc).is_absolute() {
                        PathBuf::from(&inc)
                    } else {
                        dir.join(&inc)
                    };
                    self.read_file(&inc_path, in_progress, seen)?;
                } else {
                    // The key may be a comma-separated alias list spanning
                    // multiple lines; split, take the last line of each, upper.
                    for raw_alias in key.split(',') {
                        let alias = raw_alias.trim().lines().last().unwrap_or("").trim();
                        if alias.is_empty() {
                            continue;
                        }
                        self.set_entry(alias.to_ascii_uppercase(), value.clone());
                    }
                }
            }
            in_progress.pop();
            Ok(())
        }
    }

    /// Parses a tnsnames.ora file into a list of `(key, value)` pairs, where the
    /// key may be a (possibly multi-line) comma-separated alias list or `IFILE`,
    /// and the value is the descriptor / easy-connect / include path. Mirrors
    /// the reference `TnsnamesFileParser.parse`.
    fn parse_file(contents: &str) -> Vec<(String, String)> {
        let chars: Vec<char> = contents.chars().collect();
        let mut parser = FileParser {
            chars: &chars,
            temp_pos: 0,
            pos: 0,
        };
        let mut out = Vec::new();
        while parser.temp_pos < parser.chars.len() {
            let key = parser.parse_key();
            let value = parser.parse_value();
            if let (Some(key), Some(value)) = (key, value) {
                if !key.is_empty() && !value.is_empty() {
                    out.push((key, value.trim().to_string()));
                }
            }
        }
        out
    }

    struct FileParser<'a> {
        chars: &'a [char],
        temp_pos: usize,
        pos: usize,
    }

    impl FileParser<'_> {
        fn current(&self) -> char {
            self.chars[self.temp_pos]
        }

        fn skip_spaces(&mut self) {
            while self.temp_pos < self.chars.len() && self.chars[self.temp_pos].is_whitespace() {
                self.temp_pos += 1;
            }
        }

        fn skip_to_end_of_line(&mut self) {
            while self.temp_pos < self.chars.len() {
                let ch = self.current();
                self.temp_pos += 1;
                if ch == '\n' || ch == '\r' {
                    break;
                }
            }
            self.pos = self.temp_pos;
            self.skip_spaces();
        }

        /// Mirrors `_parse_key`: reads non-whitespace chars until `=`. Lines with
        /// stray parens / comments before `=` are discarded.
        fn parse_key(&mut self) -> Option<String> {
            let mut found_key = false;
            let mut start_pos = 0usize;
            self.skip_spaces();
            while self.temp_pos < self.chars.len() {
                let ch = self.current();
                if ch == '(' || ch == ')' || ch == '#' {
                    self.skip_to_end_of_line();
                    found_key = false;
                    continue;
                } else if ch == '=' {
                    if !found_key {
                        self.skip_to_end_of_line();
                        continue;
                    }
                    self.temp_pos += 1;
                    self.pos = self.temp_pos;
                    let key: String = self.chars[start_pos..self.temp_pos - 1].iter().collect();
                    return Some(key.trim().to_string());
                } else if !found_key {
                    found_key = true;
                    start_pos = self.temp_pos;
                }
                self.temp_pos += 1;
            }
            None
        }

        /// Mirrors `_parse_value`: accumulates value parts until parens balance.
        fn parse_value(&mut self) -> Option<String> {
            let mut num_parens: isize = 0;
            let mut parts: Vec<String> = Vec::new();
            while self.temp_pos < self.chars.len() {
                if let Some(part) = self.parse_value_part(&mut num_parens) {
                    parts.push(part);
                }
                if num_parens == 0 {
                    break;
                }
            }
            if parts.is_empty() {
                None
            } else {
                Some(parts.join("\n"))
            }
        }

        /// Mirrors `_parse_value_part`.
        fn parse_value_part(&mut self, num_parens: &mut isize) -> Option<String> {
            let mut start_pos = 0usize;
            let mut end_pos = 0usize;
            let mut found_part = false;
            self.skip_spaces();
            while self.temp_pos < self.chars.len() {
                let ch = self.current();
                if ch == '#' {
                    end_pos = self.temp_pos;
                    self.skip_to_end_of_line();
                    if found_part {
                        break;
                    }
                    continue;
                }
                if found_part && *num_parens == 0 {
                    if ch == '\n' || ch == '\r' {
                        end_pos = self.temp_pos;
                        break;
                    }
                } else if ch == '(' {
                    *num_parens += 1;
                } else if ch == ')' && *num_parens > 0 {
                    *num_parens -= 1;
                }
                if !found_part {
                    found_part = true;
                    start_pos = self.temp_pos;
                }
                self.temp_pos += 1;
                end_pos = self.temp_pos;
            }
            if found_part {
                let part: String = self.chars[start_pos..end_pos].iter().collect();
                Some(part.trim().to_string())
            } else {
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Argument-tree -> Descriptor builder
// ---------------------------------------------------------------------------

/// Returns the first simple value for `key`, if present and simple.
fn simple(map: &ArgMap, key: &str) -> Option<String> {
    match map.get(key)?.first()? {
        ArgValue::Simple(s) => Some(s.clone()),
        ArgValue::Node(_) => None,
    }
}

/// Parses a connect-string boolean (reference `_set_bool_param`): the strings
/// `on` / `yes` / `true` (case-insensitive) are true; everything else is false.
fn parse_bool(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "on" | "yes" | "true"
    )
}

/// Parses a connect-string unsigned int (reference `_set_uint_param`). The
/// reference uses Python `int()`, which rejects non-numeric strings; we mirror
/// that by surfacing a diagnostic.
fn parse_uint(connect_string: &str, key: &str, value: &str) -> Result<u32> {
    value.trim().parse::<u32>().map_err(|_| {
        ProtocolError::InvalidConnectDescriptor(format!(
            "invalid connect descriptor \"{connect_string}\": {key} value \"{value}\" is not a \
             non-negative integer"
        ))
    })
}

/// Parses a duration (reference `_set_duration_param`): a float with an
/// optional `ms` / `sec` / `min` unit suffix, normalised to seconds.
fn parse_duration(connect_string: &str, key: &str, value: &str) -> Result<f64> {
    let v = value.trim().to_ascii_lowercase();
    let (num, scale) = if let Some(stripped) = v.strip_suffix("sec") {
        (stripped.trim(), 1.0)
    } else if let Some(stripped) = v.strip_suffix("ms") {
        (stripped.trim(), 0.001)
    } else if let Some(stripped) = v.strip_suffix("min") {
        (stripped.trim(), 60.0)
    } else {
        (v.as_str(), 1.0)
    };
    num.parse::<f64>().map(|n| n * scale).map_err(|_| {
        ProtocolError::InvalidConnectDescriptor(format!(
            "invalid connect descriptor \"{connect_string}\": {key} value \"{value}\" is not a \
             valid duration"
        ))
    })
}

/// Reconstructs the listener-form string for a pass-through (extra) value,
/// mirroring the reference `_value_repr`: simple values are kept verbatim;
/// nested nodes become `(KEY=value)` chains with upper-cased keys.
fn value_repr(value: &ArgValue) -> String {
    match value {
        ArgValue::Simple(s) => s.clone(),
        ArgValue::Node(node) => {
            let mut out = String::new();
            for (key, values) in &node.entries {
                for v in values {
                    out.push('(');
                    out.push_str(&key.to_ascii_uppercase());
                    out.push('=');
                    out.push_str(&value_repr(v));
                    out.push(')');
                }
            }
            out
        }
    }
}

/// Iterates `(key, value)` pairs not in `allowed`, collecting them as
/// reconstructed pass-through strings (reference `_process_args_with_extras`).
fn collect_extras(map: &ArgMap, allowed: &[&str]) -> Vec<(String, String)> {
    let mut extras = Vec::new();
    for (key, values) in &map.entries {
        if allowed.contains(&key.as_str()) {
            continue;
        }
        for v in values {
            extras.push((key.to_ascii_uppercase(), value_repr(v)));
        }
    }
    extras
}

/// Builds a [`Descriptor`] from the parsed argument tree, mirroring the
/// reference `_parse_descriptor`.
fn build_descriptor(connect_string: &str, args: &ArgMap) -> Result<Descriptor> {
    let mut descriptor = Descriptor {
        descriptions: Vec::new(),
        load_balance: false,
        failover: true,
        source_route: false,
    };

    // DESCRIPTION_LIST flags, if present.
    let list_node = args.get("description_list").and_then(|v| match v.first() {
        Some(ArgValue::Node(n)) => Some(n),
        _ => None,
    });
    let description_container = if let Some(list_node) = list_node {
        descriptor.load_balance = list_node.get("load_balance").is_some()
            && simple(list_node, "load_balance").is_some_and(|v| parse_bool(&v));
        if let Some(v) = simple(list_node, "failover") {
            descriptor.failover = parse_bool(&v);
        }
        descriptor.source_route = simple(list_node, "source_route").is_some_and(|v| parse_bool(&v));
        list_node
    } else {
        args
    };

    // Descriptions: the reference takes list_args.get("description", list_args)
    // — i.e. if there's no explicit "description" key, the container itself is
    // treated as a single description.
    let descriptions: Vec<&ArgMap> = match description_container.get("description") {
        Some(values) => {
            let mut out = Vec::new();
            for v in values {
                if let ArgValue::Node(n) = v {
                    out.push(n);
                }
            }
            out
        }
        None => vec![description_container],
    };

    for desc_args in descriptions {
        let description = build_description(connect_string, desc_args)?;
        descriptor.descriptions.push(description);
    }

    if descriptor.addresses().next().is_none() {
        return Err(ProtocolError::InvalidConnectDescriptor(format!(
            "no addresses are defined in connect descriptor: {connect_string}"
        )));
    }
    Ok(descriptor)
}

const DESCRIPTION_PARAM_NAMES: &[&str] = &[
    "address",
    "address_list",
    "connect_data",
    "expire_time",
    "failover",
    "load_balance",
    "source_route",
    "retry_count",
    "retry_delay",
    "sdu",
    "tcp_connect_timeout",
    "use_sni",
    "security",
];

const CONNECT_DATA_PARAM_NAMES: &[&str] = &[
    "cclass",
    "connection_id_prefix",
    "instance_name",
    "pool_boundary",
    "pool_name",
    "purity",
    "server_type",
    "service_name",
    "sid",
    "use_tcp_fast_open",
];

const SECURITY_PARAM_NAMES: &[&str] = &[
    "ssl_server_cert_dn",
    "ssl_server_dn_match",
    "ssl_version",
    "wallet_location",
];

fn build_description(connect_string: &str, desc_args: &ArgMap) -> Result<Description> {
    let mut description = Description::default();

    // DESCRIPTION-level args.
    if let Some(v) = simple(desc_args, "expire_time") {
        description.expire_time = parse_uint(connect_string, "EXPIRE_TIME", &v)?;
    }
    if let Some(v) = simple(desc_args, "failover") {
        description.failover = parse_bool(&v);
    }
    if let Some(v) = simple(desc_args, "load_balance") {
        description.load_balance = parse_bool(&v);
    }
    if let Some(v) = simple(desc_args, "source_route") {
        description.source_route = parse_bool(&v);
    }
    if let Some(v) = simple(desc_args, "retry_count") {
        description.retry_count = parse_uint(connect_string, "RETRY_COUNT", &v)?;
    }
    if let Some(v) = simple(desc_args, "retry_delay") {
        description.retry_delay = parse_uint(connect_string, "RETRY_DELAY", &v)?;
    }
    if let Some(v) = simple(desc_args, "use_sni") {
        description.use_sni = parse_bool(&v);
    }
    if let Some(v) = simple(desc_args, "sdu") {
        description.sdu = parse_uint(connect_string, "SDU", &v)?.clamp(MIN_SDU, MAX_SDU);
    }
    if let Some(v) = simple(desc_args, "tcp_connect_timeout") {
        description.tcp_connect_timeout =
            parse_duration(connect_string, "TRANSPORT_CONNECT_TIMEOUT", &v)?;
    }
    description.extra = collect_extras(desc_args, DESCRIPTION_PARAM_NAMES);

    // CONNECT_DATA.
    if let Some(ArgValue::Node(cd)) = desc_args.get("connect_data").and_then(|v| v.first()) {
        description.connect_data = build_connect_data(connect_string, cd)?;
    }

    // SECURITY.
    if let Some(ArgValue::Node(sec)) = desc_args.get("security").and_then(|v| v.first()) {
        description.security = build_security(sec);
    }

    // Address lists. The reference takes desc_args.get("address_list", desc_args)
    // and if that is not a list, sets source_route=False and wraps it.
    let address_list_nodes: Vec<&ArgMap> = match desc_args.get("address_list") {
        Some(values) => values
            .iter()
            .filter_map(|v| match v {
                ArgValue::Node(n) => Some(n),
                ArgValue::Simple(_) => None,
            })
            .collect(),
        None => {
            description.source_route = false;
            vec![desc_args]
        }
    };

    for list_args in address_list_nodes {
        let mut address_list = AddressList {
            failover: true,
            ..AddressList::default()
        };
        if let Some(v) = simple(list_args, "failover") {
            address_list.failover = parse_bool(&v);
        }
        if let Some(v) = simple(list_args, "load_balance") {
            address_list.load_balance = parse_bool(&v);
        }
        if let Some(v) = simple(list_args, "source_route") {
            address_list.source_route = parse_bool(&v);
        }
        if let Some(addresses) = list_args.get("address") {
            for addr in addresses {
                if let ArgValue::Node(addr_node) = addr {
                    address_list.addresses.push(build_address(addr_node)?);
                }
            }
        }
        description.address_lists.push(address_list);
    }

    Ok(description)
}

fn build_address(addr: &ArgMap) -> Result<Address> {
    let mut address = Address::default();
    if let Some(host) = simple(addr, "host") {
        address.host = Some(host);
    }
    if let Some(port) = simple(addr, "port") {
        address.port = port.trim().parse::<u16>().map_err(|_| {
            ProtocolError::InvalidConnectDescriptor(format!("invalid port: {port}"))
        })?;
    }
    if let Some(protocol) = simple(addr, "protocol") {
        address.protocol = Protocol::from_keyword(&protocol)?;
    }
    if let Some(proxy) = simple(addr, "https_proxy") {
        address.https_proxy = Some(proxy);
    }
    if let Some(proxy_port) = simple(addr, "https_proxy_port") {
        address.https_proxy_port = proxy_port.trim().parse::<u16>().unwrap_or(0);
    }
    Ok(address)
}

fn build_connect_data(connect_string: &str, cd: &ArgMap) -> Result<ConnectData> {
    let mut data = ConnectData {
        service_name: simple(cd, "service_name"),
        instance_name: simple(cd, "instance_name"),
        sid: simple(cd, "sid"),
        ..ConnectData::default()
    };
    if let Some(server) = simple(cd, "server_type") {
        data.server_type = Some(ServerType::from_keyword(&server)?);
    }
    if let Some(cclass) = simple(cd, "cclass") {
        if !cclass.is_empty() {
            data.cclass = Some(cclass);
        }
    }
    if let Some(purity) = simple(cd, "purity") {
        data.purity = Some(Purity::from_keyword(&purity).map_err(|_| {
            ProtocolError::InvalidConnectDescriptor(format!(
                "invalid connect descriptor \"{connect_string}\": invalid POOL_PURITY \"{purity}\""
            ))
        })?);
    }
    data.pool_boundary = simple(cd, "pool_boundary");
    data.pool_name = simple(cd, "pool_name");
    data.connection_id_prefix = simple(cd, "connection_id_prefix");
    if let Some(v) = simple(cd, "use_tcp_fast_open") {
        data.use_tcp_fast_open = parse_bool(&v);
    }
    data.extra = collect_extras(cd, CONNECT_DATA_PARAM_NAMES);
    Ok(data)
}

fn build_security(sec: &ArgMap) -> Security {
    let mut security = Security::default();
    if let Some(v) = simple(sec, "ssl_server_dn_match") {
        security.ssl_server_dn_match = parse_bool(&v);
    }
    security.ssl_server_cert_dn = simple(sec, "ssl_server_cert_dn");
    security.wallet_location = simple(sec, "wallet_location");
    security.extra = collect_extras(sec, SECURITY_PARAM_NAMES);
    security
}

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

#[cfg(test)]
mod tnsnames_tests {
    use super::tnsnames::TnsnamesReader;
    use super::*;
    use std::io::Write;

    /// Writes `contents` to `<dir>/<name>` and returns nothing.
    fn write_file(dir: &std::path::Path, name: &str, contents: &str) {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).expect("create tns file");
        f.write_all(contents.as_bytes()).expect("write tns file");
    }

    fn temp_dir() -> std::path::PathBuf {
        let base = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_string());
        let unique = format!(
            "hk6_tns_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::path::Path::new(&base).join(unique);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn resolves_simple_alias() {
        // reference test_7200
        let dir = temp_dir();
        write_file(
            &dir,
            "tnsnames.ora",
            "nsn_7200 = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=host_7200)(PORT=7200))\
             (CONNECT_DATA=(SERVICE_NAME=service_7200)))",
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        let cs = reader.get("nsn_7200").expect("alias present");
        let d = parse(cs).unwrap().unwrap();
        let a = d.first_address().unwrap();
        assert_eq!(a.host.as_deref(), Some("host_7200"));
        assert_eq!(a.port, 7200);
    }

    #[test]
    fn missing_entry_is_none() {
        // reference test_7201
        let dir = temp_dir();
        write_file(&dir, "tnsnames.ora", "# no entries");
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        assert!(reader.get("nsn_7201").is_none());
        assert!(reader.service_names().is_empty());
    }

    #[test]
    fn missing_file_errors() {
        // reference test_7202
        let dir = temp_dir();
        let err = TnsnamesReader::read(&dir).unwrap_err();
        assert!(format!("{err}").contains("missing or unreadable"));
    }

    #[test]
    fn ignores_garbage_lines() {
        // reference test_7203
        let dir = temp_dir();
        write_file(
            &dir,
            "tnsnames.ora",
            "some garbage data which is not a valid entry\n\
             nsn_7203 = host_7203:7203/service_7203\n",
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        assert!(reader.get("nsn_7203").is_some());
    }

    #[test]
    fn multiple_aliases_one_line() {
        // reference test_7204
        let dir = temp_dir();
        write_file(
            &dir,
            "tnsnames.ora",
            "nsn_7204a,nsn_7204b = host_7204:7204/service_7204\n",
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        assert!(reader.get("nsn_7204a").is_some());
        assert!(reader.get("nsn_7204b").is_some());
        assert_eq!(reader.service_names(), vec!["NSN_7204A", "NSN_7204B"]);
    }

    #[test]
    fn case_insensitive_alias_lookup() {
        let dir = temp_dir();
        write_file(&dir, "tnsnames.ora", "Nsn_X = host:1521/svc\n");
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        assert!(reader.get("nsn_x").is_some());
        assert!(reader.get("NSN_X").is_some());
    }

    #[test]
    fn ifile_same_directory() {
        // reference test_7207
        let dir = temp_dir();
        write_file(&dir, "inc_7207.ora", "nsn_7207b = host_b:72072/service_b");
        write_file(
            &dir,
            "tnsnames.ora",
            "nsn_7207a = host_a:72071/service_a\nifile = inc_7207.ora",
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        assert!(reader.get("nsn_7207a").is_some());
        assert!(reader.get("nsn_7207b").is_some());
    }

    #[test]
    fn ifile_cycle_detected() {
        // reference test_7209
        let dir = temp_dir();
        write_file(
            &dir,
            "tnsnames.ora",
            "nsn_7209 = some_host/some_service\nIFILE = tnsnames.ora",
        );
        let err = TnsnamesReader::read(&dir).unwrap_err();
        assert!(format!("{err}").contains("cycle"));
    }

    #[test]
    fn ifile_quoted_path() {
        // reference test_7223 style (double-quoted IFILE path)
        let dir = temp_dir();
        let inc = dir.join("inc_q.ora");
        write_file(&dir, "inc_q.ora", "nsn_q = host_q:1521/svc_q");
        write_file(
            &dir,
            "tnsnames.ora",
            &format!(
                "nsn_main = host_m:1521/svc_m\nifile = \"{}\"",
                inc.display()
            ),
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        assert!(reader.get("nsn_q").is_some());
    }

    #[test]
    fn duplicate_entry_last_wins() {
        // reference test_7213
        let dir = temp_dir();
        write_file(
            &dir,
            "tnsnames.ora",
            "nsn = host_a:7213/svc_a\nother = h/s\nnsn = host_b:7213/svc_b\n",
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        let d = parse(reader.get("nsn").unwrap()).unwrap().unwrap();
        assert_eq!(d.first_address().unwrap().host.as_deref(), Some("host_b"));
    }

    #[test]
    fn multiline_aliases() {
        // reference test_7219
        let dir = temp_dir();
        write_file(
            &dir,
            "tnsnames.ora",
            "nsn_a,\nnsn_b,\nnsn_c = host:1521/svc",
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        assert!(reader.get("nsn_a").is_some());
        assert!(reader.get("nsn_b").is_some());
        assert!(reader.get("nsn_c").is_some());
    }

    #[test]
    fn embedded_comment_in_descriptor() {
        // reference test_7220
        let dir = temp_dir();
        write_file(
            &dir,
            "tnsnames.ora",
            "nsn_7220 = (DESCRIPTION=\n(ADDRESS=(PROTOCOL=TCP)(HOST=host_7220)(PORT=7220))\n\
             (CONNECT_DATA=\n(SERVICE_NAME=service_7220)\n# embedded comment\n)\n)\n",
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        let d = parse(reader.get("nsn_7220").unwrap()).unwrap().unwrap();
        assert_eq!(
            d.first_address().unwrap().host.as_deref(),
            Some("host_7220")
        );
    }

    #[test]
    fn missing_ifile_errors() {
        // reference test_7216
        let dir = temp_dir();
        write_file(&dir, "tnsnames.ora", "IFILE = missing.ora\n");
        let err = TnsnamesReader::read(&dir).unwrap_err();
        assert!(format!("{err}").contains("missing or unreadable"));
    }

    // bead rust-oracledb-uf8: a deeply-nested descriptor must return a clean
    // Err, never recurse until the stack overflows and ABORTS the process.
    #[test]
    fn deeply_nested_descriptor_errors_not_crashes() {
        // 5000 levels of "(A=" + "1" + 5000 ")" — far past MAX_DESCRIPTOR_DEPTH
        // but small enough that the depth guard fires long before any real
        // stack pressure. Without the guard this overflows the stack.
        let depth = 5000;
        let mut s = String::with_capacity(depth * 4);
        for _ in 0..depth {
            s.push_str("(A=");
        }
        s.push('1');
        for _ in 0..depth {
            s.push(')');
        }
        let err = parse(&s).unwrap_err();
        assert!(
            format!("{err}").contains("nesting too deep"),
            "expected a nesting-depth error, got: {err}"
        );
    }

    #[test]
    fn legitimately_deep_descriptor_still_parses() {
        // A realistic DESCRIPTION_LIST topology (~5 deep) must NOT be rejected.
        let ok = "(DESCRIPTION_LIST=(DESCRIPTION=(ADDRESS_LIST=\
                  (ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1521)))\
                  (CONNECT_DATA=(SERVICE_NAME=svc))))";
        assert!(parse(ok).is_ok(), "a real ~5-deep descriptor must parse");
    }
}
