use super::*;

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
pub(super) fn parse_bool(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "on" | "yes" | "true"
    )
}

/// Parses a connect-string unsigned int (reference `_set_uint_param`). The
/// reference uses Python `int()`, which rejects non-numeric strings; we mirror
/// that by surfacing a diagnostic.
pub(super) fn parse_uint(connect_string: &str, key: &str, value: &str) -> Result<u32> {
    value.trim().parse::<u32>().map_err(|_| {
        ProtocolError::InvalidConnectDescriptor(format!(
            "invalid connect descriptor \"{connect_string}\": {key} value \"{value}\" is not a \
             non-negative integer"
        ))
    })
}

/// Parses a duration (reference `_set_duration_param`): a float with an
/// optional `ms` / `sec` / `min` unit suffix, normalised to seconds.
pub(super) fn parse_duration(connect_string: &str, key: &str, value: &str) -> Result<f64> {
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
pub(super) fn build_descriptor(connect_string: &str, args: &ArgMap) -> Result<Descriptor> {
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
