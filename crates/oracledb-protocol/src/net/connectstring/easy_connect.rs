use super::builders::{parse_bool, parse_duration, parse_uint};
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
            | "connect_timeout"
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
