//! Oracle server-certificate DN / name matching.
//!
//! python-oracledb thin disables rustls's standard hostname verification and
//! instead runs its own check after the TLS handshake completes
//! (`impl/thin/crypto.pyx::check_server_dn`). This module is a faithful,
//! sans-I/O port of that algorithm operating on already-extracted certificate
//! fields (subject DN string, SAN DNS names, common names).
//!
//! Two modes, mirroring the reference exactly:
//!
//! * **Explicit DN** (`ssl_server_cert_dn` is set): parse the expected DN and
//!   the server's subject DN into `{ATTR: value}` maps and require the maps to
//!   be equal. Order-independent; exact (no wildcards).
//! * **Name match** (no `ssl_server_cert_dn`): match the expected host against
//!   the certificate's SAN DNS names first, then its common names, with
//!   wildcard support (`_name_matches`).

/// Outcome of a DN / name check, kept distinct so the driver can surface the
/// reference's two distinct errors (ERR_INVALID_SERVER_CERT_DN vs
/// ERR_INVALID_SERVER_NAME).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnMatchError {
    /// `ssl_server_cert_dn` was supplied but did not equal the server's
    /// subject DN (reference ERR_INVALID_SERVER_CERT_DN).
    CertDnMismatch { expected_dn: String },
    /// No `ssl_server_cert_dn`; the host matched neither a SAN DNS name nor a
    /// common name (reference ERR_INVALID_SERVER_NAME).
    NameMismatch { expected_name: String },
}

impl core::fmt::Display for DnMatchError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::CertDnMismatch { expected_dn } => write!(
                f,
                "the distinguished name (DN) on the server certificate does not match \
                 the expected value \"{expected_dn}\""
            ),
            Self::NameMismatch { expected_name } => write!(
                f,
                "the server name \"{expected_name}\" does not match the names in the \
                 server certificate"
            ),
        }
    }
}

impl std::error::Error for DnMatchError {}

/// Parse a distinguished-name string into a map of `ATTR -> value`, mirroring
/// python-oracledb's `DN_REGEX` semantics:
///
/// `(?:^|,\s?)(?:(?P<name>[A-Z]+)=(?P<val>"(?:[^"]|"")+"|[^,]+))+`
///
/// i.e. comma-separated `ATTR=value` pairs where the attribute name is one or
/// more uppercase ASCII letters and the value is either a double-quoted string
/// (in which `""` is a literal quote) or a run of non-comma characters. The
/// separator may be a comma optionally followed by a single space.
///
/// Returned as a sorted `Vec` of `(attr, value)` so two DNs can be compared
/// order-independently (the reference compares Python dicts).
#[must_use]
pub fn parse_dn(dn: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let bytes: Vec<char> = dn.chars().collect();
    let mut i = 0usize;
    let n = bytes.len();
    while i < n {
        // Skip a leading separator: optional comma + optional single space.
        if bytes[i] == ',' {
            i += 1;
            if i < n && bytes[i] == ' ' {
                i += 1;
            }
        }
        // Skip any other incidental whitespace at the start of a pair.
        while i < n && bytes[i] == ' ' {
            i += 1;
        }
        if i >= n {
            break;
        }
        // Attribute name: one or more uppercase ASCII letters.
        let name_start = i;
        while i < n && bytes[i].is_ascii_uppercase() {
            i += 1;
        }
        if i == name_start || i >= n || bytes[i] != '=' {
            // Not a well-formed pair; skip to the next comma to stay in sync.
            while i < n && bytes[i] != ',' {
                i += 1;
            }
            continue;
        }
        let name: String = bytes[name_start..i].iter().collect();
        i += 1; // consume '='

        // Value: quoted ("" => literal quote) or a run of non-comma chars.
        let value = if i < n && bytes[i] == '"' {
            i += 1; // opening quote
            let mut val = String::new();
            while i < n {
                if bytes[i] == '"' {
                    if i + 1 < n && bytes[i + 1] == '"' {
                        // Escaped quote.
                        val.push('"');
                        i += 2;
                    } else {
                        i += 1; // closing quote
                        break;
                    }
                } else {
                    val.push(bytes[i]);
                    i += 1;
                }
            }
            val
        } else {
            let val_start = i;
            while i < n && bytes[i] != ',' {
                i += 1;
            }
            let mut val: String = bytes[val_start..i].iter().collect();
            // The non-quoted branch in the reference regex ([^,]+) does not
            // trim, but a trailing space before the next ", " separator is part
            // of the value only if no space-separator follows. To match the
            // reference's "comma + optional single space" separator we leave
            // the value as-is; callers compare verbatim. We do trim a single
            // trailing space that would otherwise belong to the separator.
            if val.ends_with(' ') {
                // Only strip if the next char is a comma-less end / separator.
                val = val.trim_end_matches(' ').to_string();
            }
            val
        };
        out.push((name, value));
    }
    out.sort();
    out
}

/// Compare an expected DN against the server's subject DN for equality, the
/// `expected_dn is not None` branch of `check_server_dn`.
///
/// # Errors
/// Returns [`DnMatchError::CertDnMismatch`] when the parsed attribute maps
/// differ.
pub fn check_cert_dn(expected_dn: &str, server_subject_dn: &str) -> Result<(), DnMatchError> {
    let expected = parse_dn(expected_dn);
    let server = parse_dn(server_subject_dn);
    if expected == server {
        Ok(())
    } else {
        Err(DnMatchError::CertDnMismatch {
            expected_dn: expected_dn.to_string(),
        })
    }
}

/// Returns whether `name_to_check` matches `cert_name`, where `cert_name` may
/// contain a wildcard (`*`). Faithful port of python-oracledb's
/// `crypto.pyx::_name_matches` (case-insensitive).
#[must_use]
pub fn name_matches(name_to_check: &str, cert_name: &str) -> bool {
    let cert_name = cert_name.to_lowercase();
    let name_to_check = name_to_check.to_lowercase();

    // Full match.
    if name_to_check == cert_name {
        return true;
    }

    // Both must have more than one label.
    let check_pos = name_to_check.find('.');
    let cert_pos = cert_name.find('.');
    let (Some(check_pos), Some(cert_pos)) = (check_pos, cert_pos) else {
        return false;
    };
    if check_pos == 0 || cert_pos == 0 {
        return false;
    }

    // Right-hand labels (from the first dot onward) must match.
    if name_to_check[check_pos..] != cert_name[cert_pos..] {
        return false;
    }

    // Wildcard matching on the left-most label.
    let cert_label = &cert_name[..cert_pos];
    let check_label = &name_to_check[..check_pos];
    if cert_label == "*" {
        return true;
    } else if let Some(suffix) = cert_label.strip_prefix('*') {
        return check_label.ends_with(suffix);
    } else if let Some(prefix) = cert_label.strip_suffix('*') {
        return check_label.starts_with(prefix);
    }
    // Wildcard somewhere in the middle.
    match cert_name.find('*') {
        None => false,
        Some(_) => {
            // The reference uses the wildcard position within the *full*
            // cert_name to slice cert_name (not cert_label). Replicate that.
            let wildcard_pos = cert_name.find('*').unwrap_or(0);
            let pre = &cert_name[..wildcard_pos];
            let post_start = wildcard_pos + 1;
            // cert_name[wildcard_pos + 1:] in the reference is sliced from the
            // full cert_name, but `_name_matches` only reaches here for the
            // left label, so post is the remainder of cert_label.
            let post = if post_start <= cert_label.len() {
                &cert_label[post_start..]
            } else {
                ""
            };
            check_label.starts_with(pre) && check_label.ends_with(post)
        }
    }
}

/// Match the expected host name against the certificate's SAN DNS names and
/// then its common names — the `expected_dn is None` branch of
/// `check_server_dn`.
///
/// # Errors
/// Returns [`DnMatchError::NameMismatch`] when no SAN DNS name and no common
/// name matches `expected_name`.
pub fn check_server_name(
    expected_name: &str,
    san_dns_names: &[String],
    common_names: &[String],
) -> Result<(), DnMatchError> {
    for name in san_dns_names {
        if name_matches(expected_name, name) {
            return Ok(());
        }
    }
    for name in common_names {
        if name_matches(expected_name, name) {
            return Ok(());
        }
    }
    Err(DnMatchError::NameMismatch {
        expected_name: expected_name.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dn_simple() {
        let parsed = parse_dn("CN=db.example.com,O=Example,C=US");
        assert_eq!(
            parsed,
            vec![
                ("C".to_string(), "US".to_string()),
                ("CN".to_string(), "db.example.com".to_string()),
                ("O".to_string(), "Example".to_string()),
            ]
        );
    }

    #[test]
    fn parse_dn_order_independent_equality() {
        let a = parse_dn("CN=x,O=y");
        let b = parse_dn("O=y,CN=x");
        assert_eq!(a, b);
    }

    #[test]
    fn parse_dn_comma_space_separator() {
        let a = parse_dn("CN=x, O=y, C=Z");
        assert_eq!(
            a,
            vec![
                ("C".to_string(), "Z".to_string()),
                ("CN".to_string(), "x".to_string()),
                ("O".to_string(), "y".to_string()),
            ]
        );
    }

    #[test]
    fn parse_dn_quoted_value() {
        let a = parse_dn(r#"CN="Acme, Inc.",C=US"#);
        // The quoted value contains a comma that must NOT split the pair.
        assert!(a.contains(&("CN".to_string(), "Acme, Inc.".to_string())));
        assert!(a.contains(&("C".to_string(), "US".to_string())));
    }

    #[test]
    fn check_cert_dn_accept_exact() {
        assert!(check_cert_dn("CN=x,O=y", "O=y,CN=x").is_ok());
    }

    #[test]
    fn check_cert_dn_reject_diff() {
        let err = check_cert_dn("CN=x,O=y", "CN=z,O=y").unwrap_err();
        assert!(matches!(err, DnMatchError::CertDnMismatch { .. }));
    }

    #[test]
    fn check_cert_dn_reject_extra_attr() {
        let err = check_cert_dn("CN=x", "CN=x,O=y").unwrap_err();
        assert!(matches!(err, DnMatchError::CertDnMismatch { .. }));
    }

    #[test]
    fn name_matches_full_case_insensitive() {
        assert!(name_matches("DB.example.com", "db.example.COM"));
    }

    #[test]
    fn name_matches_leading_wildcard() {
        assert!(name_matches("host.example.com", "*.example.com"));
        assert!(!name_matches("host.sub.example.com", "*.example.com"));
    }

    #[test]
    fn name_matches_prefix_wildcard_label() {
        // cert "web*.example.com" matches "webserver.example.com"
        assert!(name_matches("webserver.example.com", "web*.example.com"));
        assert!(!name_matches("appserver.example.com", "web*.example.com"));
    }

    #[test]
    fn name_matches_suffix_wildcard_label() {
        assert!(name_matches("serverweb.example.com", "*web.example.com"));
    }

    #[test]
    fn name_matches_rejects_single_label() {
        assert!(!name_matches("localhost", "*"));
    }

    #[test]
    fn check_server_name_san_first() {
        assert!(check_server_name(
            "db.example.com",
            &["db.example.com".to_string()],
            &[]
        )
        .is_ok());
    }

    #[test]
    fn check_server_name_falls_back_to_cn() {
        assert!(check_server_name(
            "db.example.com",
            &[],
            &["db.example.com".to_string()]
        )
        .is_ok());
    }

    #[test]
    fn check_server_name_rejects_unknown() {
        let err = check_server_name("evil.example.com", &["db.example.com".to_string()], &[])
            .unwrap_err();
        assert!(matches!(err, DnMatchError::NameMismatch { .. }));
    }
}
