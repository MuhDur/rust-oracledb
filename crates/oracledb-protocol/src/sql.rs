#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SqlError {
    #[error("missing ending single quote")]
    MissingEndingSingleQuote,
    #[error("missing ending double quote")]
    MissingEndingDoubleQuote,
}

pub type Result<T> = std::result::Result<T, SqlError>;

pub fn plsql_function_return_bind_name(statement: &str) -> Option<String> {
    let rest = statement.trim_start();
    if !rest.get(.."begin".len())?.eq_ignore_ascii_case("begin") {
        return None;
    }
    let rest = rest.get("begin".len()..)?.trim_start();
    let rest = rest.strip_prefix(':')?;
    let mut name_end = 0;
    for (offset, ch) in rest.char_indices() {
        if is_bind_name_char(ch) {
            name_end = offset + ch.len_utf8();
        } else {
            break;
        }
    }
    if name_end == 0 {
        return None;
    }
    let (name, rest) = rest.split_at(name_end);
    rest.trim_start()
        .starts_with(":=")
        .then(|| name.to_string())
}

pub fn unique_bind_names(statement: &str) -> Result<Vec<String>> {
    let mut names: Vec<String> = Vec::new();
    for name in scan_bind_names(statement)? {
        if !names
            .iter()
            .any(|existing| bind_names_equal(existing, &name))
        {
            names.push(name);
        }
    }
    Ok(names)
}

pub fn public_bind_name(name: &str) -> String {
    if is_quoted_bind_name(name) {
        name[1..name.len() - 1].to_string()
    } else {
        name.to_uppercase()
    }
}

pub fn returning_bind_names(statement: &str) -> Result<Vec<String>> {
    if statement_is_plsql(statement) {
        return Ok(Vec::new());
    }
    let lower = statement.to_ascii_lowercase();
    let Some(returning_pos) = lower.find("returning") else {
        return Ok(Vec::new());
    };
    let Some(into_relative_pos) = lower[returning_pos..].find("into") else {
        return Ok(Vec::new());
    };
    let into_pos = returning_pos + into_relative_pos + "into".len();
    scan_bind_names(&statement[into_pos..])
}

pub fn dml_returning_single_bind_name(statement: &str) -> Result<Option<String>> {
    let Some(parts) = dml_returning_projection_parts(statement)? else {
        return Ok(None);
    };
    if parts.bind_names.len() == 1 {
        Ok(parts.bind_names.into_iter().next())
    } else {
        Ok(None)
    }
}

pub fn rewrite_dml_returning_projection(
    statement: &str,
    attr_name: &str,
) -> Result<Option<String>> {
    let Some(parts) = dml_returning_projection_parts(statement)? else {
        return Ok(None);
    };
    if parts.bind_names.len() != 1 {
        return Ok(None);
    }
    Ok(Some(format!(
        "{}returning ({}).{} into{}",
        &statement[..parts.returning_pos],
        parts.return_expr,
        attr_name,
        &statement[parts.binds_start..]
    )))
}

pub fn plsql_assignment_bind_names(statement: &str) -> Result<Vec<String>> {
    if !statement_is_plsql(statement) {
        return Ok(Vec::new());
    }
    let bytes = statement.as_bytes();
    let mut names: Vec<String> = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' => {
                index += 1;
                while index < bytes.len() {
                    if is_single_quote_byte(bytes.get(index)) {
                        if is_single_quote_byte(bytes.get(index + 1)) {
                            index += 2;
                        } else {
                            index += 1;
                            break;
                        }
                    } else {
                        index += 1;
                    }
                }
                if index >= bytes.len() && !is_single_quote_byte(bytes.last()) {
                    return Err(SqlError::MissingEndingSingleQuote);
                }
            }
            b':' => {
                let start = index + 1;
                let Some(&next) = bytes.get(start) else {
                    index += 1;
                    continue;
                };
                let (name, end) = if is_double_quote_byte(Some(&next)) {
                    let mut end = start + 1;
                    while end < bytes.len() && !is_double_quote_byte(bytes.get(end)) {
                        end += 1;
                    }
                    if end >= bytes.len() {
                        index = start;
                        continue;
                    }
                    (statement[start..=end].to_string(), end + 1)
                } else {
                    let mut end = start;
                    for (offset, ch) in statement[start..].char_indices() {
                        if is_bind_name_char(ch) {
                            end = start + offset + ch.len_utf8();
                        } else {
                            break;
                        }
                    }
                    if end <= start {
                        index += 1;
                        continue;
                    }
                    (statement[start..end].to_string(), end)
                };
                let mut after_name = end;
                while bytes
                    .get(after_name)
                    .is_some_and(|byte| byte.is_ascii_whitespace())
                {
                    after_name += 1;
                }
                if matches!(bytes.get(after_name), Some(b':'))
                    && matches!(bytes.get(after_name + 1), Some(b'='))
                    && !names
                        .iter()
                        .any(|existing| bind_names_equal(existing, &name))
                {
                    names.push(name);
                }
                index = end;
            }
            _ => index += 1,
        }
    }
    Ok(names)
}

pub fn statement_is_plsql(statement: &str) -> bool {
    statement
        .trim_start()
        .split(|ch: char| !ch.is_ascii_alphabetic())
        .next()
        .is_some_and(|keyword| {
            keyword.eq_ignore_ascii_case("begin")
                || keyword.eq_ignore_ascii_case("declare")
                || keyword.eq_ignore_ascii_case("call")
        })
}

/// Mirrors the reference statement-type classification for DML
/// (impl/thin/statement.pyx `_determine_statement_type`).
pub fn statement_is_dml(statement: &str) -> bool {
    statement
        .trim_start()
        .split(|ch: char| !ch.is_ascii_alphabetic())
        .next()
        .is_some_and(|keyword| {
            keyword.eq_ignore_ascii_case("insert")
                || keyword.eq_ignore_ascii_case("update")
                || keyword.eq_ignore_ascii_case("delete")
                || keyword.eq_ignore_ascii_case("merge")
        })
}

pub fn is_bind_name_char(ch: char) -> bool {
    ch.is_alphanumeric() || matches!(ch, '_' | '$' | '#')
}

pub fn scan_bind_names(statement: &str) -> Result<Vec<String>> {
    let mut names = Vec::new();
    let mut index = 0;
    let mut last_ch = '\0';
    let mut last_was_string = false;
    while index < statement.len() {
        let Some((ch, ch_len)) = char_at(statement, index) else {
            break;
        };
        if ch == '\'' {
            index = if matches!(last_ch, 'q' | 'Q') {
                qstring_end(statement, index)?
            } else {
                quoted_string_end(statement, index, '\'')?
            };
            last_was_string = true;
        } else if ch.is_whitespace() {
            index += ch_len;
        } else if ch == '-' {
            if let Some(end) = single_line_comment_end(statement, index) {
                index = end;
            } else {
                index += ch_len;
            }
            last_was_string = false;
        } else if ch == '/' {
            if let Some(end) = multiple_line_comment_end(statement, index) {
                index = end;
            } else {
                index += ch_len;
            }
            last_was_string = false;
        } else if ch == '"' {
            index = quoted_string_end(statement, index, '"')?;
            last_was_string = false;
        } else if ch == ':' && !last_was_string {
            let (end, name) = parse_bind_name(statement, index);
            if let Some(name) = name {
                names.push(name);
            }
            index = end;
            last_was_string = false;
        } else {
            index += ch_len;
            last_was_string = false;
        }
        last_ch = ch;
    }
    Ok(names)
}

pub fn is_quoted_bind_name(name: &str) -> bool {
    name.starts_with('"') && name.ends_with('"')
}

pub fn bind_names_equal(left: &str, right: &str) -> bool {
    if is_quoted_bind_name(left) || is_quoted_bind_name(right) {
        left == right
    } else {
        left.eq_ignore_ascii_case(right)
    }
}

pub fn bind_name_matches_key(bind_name: &str, key: &str) -> bool {
    // python-oracledb strips a leading ':' from bind keys before lookup
    // (impl/thin/var.pyx:88-94).
    let key = key.strip_prefix(':').unwrap_or(key);
    if is_quoted_bind_name(bind_name) || is_quoted_bind_name(key) {
        bind_name == key
    } else {
        bind_name.eq_ignore_ascii_case(key)
    }
}

pub fn single_quote_end(statement: &str, start: usize) -> usize {
    let bytes = statement.as_bytes();
    let mut index = start + 1;
    while index < bytes.len() {
        if is_single_quote_byte(bytes.get(index)) {
            if is_single_quote_byte(bytes.get(index + 1)) {
                index += 2;
            } else {
                return index + 1;
            }
        } else {
            index += 1;
        }
    }
    statement.len()
}

pub fn generated_object_attr_bind_name(bind_name: &str, attr_name: &str) -> String {
    let bind = bind_name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("ORADB_OBJ_{bind}_{}", attr_name.to_ascii_uppercase())
}

pub fn replace_input_bind_placeholder(
    statement: &str,
    bind_name: &str,
    replacement: &str,
) -> String {
    let lower = statement.to_ascii_lowercase();
    let split = lower.find("returning").unwrap_or(statement.len());
    let (prefix, suffix) = statement.split_at(split);
    format!(
        "{}{}",
        replace_bind_placeholder(prefix, bind_name, replacement),
        suffix
    )
}

pub fn replace_bind_placeholder(statement: &str, bind_name: &str, replacement: &str) -> String {
    let mut result = String::with_capacity(statement.len() + replacement.len());
    let mut index = 0;
    while index < statement.len() {
        let rest = &statement[index..];
        if rest.starts_with('\'') {
            let end = single_quote_end(statement, index);
            result.push_str(&statement[index..end]);
            index = end;
            continue;
        }
        if rest.starts_with(':') {
            let name_start = index + 1;
            let mut name_end = name_start;
            for (offset, ch) in statement[name_start..].char_indices() {
                if is_bind_name_char(ch) {
                    name_end = name_start + offset + ch.len_utf8();
                } else {
                    break;
                }
            }
            if name_end > name_start {
                let found_name = &statement[name_start..name_end];
                if bind_names_equal(found_name, bind_name) {
                    result.push_str(replacement);
                } else {
                    result.push_str(&statement[index..name_end]);
                }
                index = name_end;
                continue;
            }
        }
        let Some(ch) = rest.chars().next() else {
            break;
        };
        result.push(ch);
        index += ch.len_utf8();
    }
    result
}

struct DmlReturningProjectionParts<'a> {
    returning_pos: usize,
    binds_start: usize,
    return_expr: &'a str,
    bind_names: Vec<String>,
}

fn dml_returning_projection_parts(
    statement: &str,
) -> Result<Option<DmlReturningProjectionParts<'_>>> {
    if statement_is_plsql(statement) {
        return Ok(None);
    }
    let lower = statement.to_ascii_lowercase();
    let Some(returning_pos) = lower.find("returning") else {
        return Ok(None);
    };
    let Some(into_relative_pos) = lower[returning_pos..].find("into") else {
        return Ok(None);
    };
    let expr_start = returning_pos + "returning".len();
    let into_start = returning_pos + into_relative_pos;
    let binds_start = into_start + "into".len();
    let return_expr = statement[expr_start..into_start].trim();
    if return_expr.contains(',') || return_expr.is_empty() {
        return Ok(None);
    }
    let bind_names = scan_bind_names(&statement[binds_start..])?;
    Ok(Some(DmlReturningProjectionParts {
        returning_pos,
        binds_start,
        return_expr,
        bind_names,
    }))
}

fn is_single_quote_byte(byte: Option<&u8>) -> bool {
    matches!(byte, Some(b'\''))
}

fn is_double_quote_byte(byte: Option<&u8>) -> bool {
    matches!(byte, Some(b'"'))
}

fn char_at(statement: &str, index: usize) -> Option<(char, usize)> {
    statement[index..]
        .chars()
        .next()
        .map(|ch| (ch, ch.len_utf8()))
}

fn single_line_comment_end(statement: &str, index: usize) -> Option<usize> {
    statement[index..].starts_with("--").then(|| {
        statement[index + 2..]
            .find('\n')
            .map_or(statement.len(), |offset| index + 2 + offset + 1)
    })
}

fn multiple_line_comment_end(statement: &str, index: usize) -> Option<usize> {
    statement[index..].starts_with("/*").then(|| {
        statement[index + 2..]
            .find("*/")
            .map_or(statement.len(), |offset| index + 2 + offset + 2)
    })
}

fn quoted_string_end(statement: &str, start: usize, quote: char) -> Result<usize> {
    let mut index = start + quote.len_utf8();
    while index < statement.len() {
        let Some((ch, ch_len)) = char_at(statement, index) else {
            break;
        };
        index += ch_len;
        if ch == quote {
            if quote == '\'' && matches!(char_at(statement, index), Some(('\'', _))) {
                index += quote.len_utf8();
                continue;
            }
            return Ok(index);
        }
    }
    if quote == '\'' {
        Err(SqlError::MissingEndingSingleQuote)
    } else {
        Err(SqlError::MissingEndingDoubleQuote)
    }
}

fn qstring_end(statement: &str, quote_index: usize) -> Result<usize> {
    let Some((open_sep, open_len)) = char_at(statement, quote_index + 1) else {
        return Err(SqlError::MissingEndingSingleQuote);
    };
    let close_sep = match open_sep {
        '[' => ']',
        '{' => '}',
        '<' => '>',
        '(' => ')',
        _ => open_sep,
    };
    let mut index = quote_index + 1 + open_len;
    let mut exiting_qstring = false;
    while index < statement.len() {
        let Some((ch, ch_len)) = char_at(statement, index) else {
            break;
        };
        if !exiting_qstring && ch == close_sep {
            exiting_qstring = true;
        } else if exiting_qstring {
            if ch == '\'' {
                return Ok(index + ch_len);
            }
            if ch != close_sep {
                exiting_qstring = false;
            }
        }
        index += ch_len;
    }
    Err(SqlError::MissingEndingSingleQuote)
}

fn parse_bind_name(statement: &str, colon_index: usize) -> (usize, Option<String>) {
    let mut index = colon_index + 1;
    while index < statement.len() {
        let Some((ch, ch_len)) = char_at(statement, index) else {
            return (index, None);
        };
        if !ch.is_whitespace() {
            break;
        }
        index += ch_len;
    }
    let Some((first_ch, first_len)) = char_at(statement, index) else {
        return (index, None);
    };
    if first_ch == '"' {
        let mut end = index + first_len;
        while end < statement.len() {
            let Some((ch, ch_len)) = char_at(statement, end) else {
                break;
            };
            end += ch_len;
            if ch == '"' {
                return (end, Some(statement[index..end].to_string()));
            }
        }
        return (statement.len(), Some(statement[index..].to_string()));
    }
    if first_ch.is_numeric() {
        let mut end = index + first_len;
        while end < statement.len() {
            let Some((ch, ch_len)) = char_at(statement, end) else {
                break;
            };
            if !ch.is_numeric() {
                break;
            }
            end += ch_len;
        }
        return (end, Some(statement[index..end].to_string()));
    }
    if !first_ch.is_alphabetic() {
        return (colon_index + 1, None);
    }
    let mut end = index + first_len;
    while end < statement.len() {
        let Some((ch, ch_len)) = char_at(statement, end) else {
            break;
        };
        if !(ch.is_alphanumeric() || matches!(ch, '_' | '$' | '#')) {
            break;
        }
        end += ch_len;
    }
    (end, Some(statement[index..end].to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_plsql_statements_by_first_keyword() {
        assert!(statement_is_plsql(" begin null; end;"));
        assert!(statement_is_plsql("DECLARE v number; begin null; end;"));
        assert!(statement_is_plsql("call pkg.proc(:x)"));
        assert!(!statement_is_plsql("select :x from dual"));
        assert!(!statement_is_plsql("update t set c = :x"));
    }

    #[test]
    fn scans_bind_names_outside_single_quoted_strings() {
        let names = scan_bind_names("select ':skip', 'it''s :skip2', :a, :\"MiX\" from dual")
            .expect("bind scan should succeed");
        assert_eq!(names, vec!["a".to_string(), "\"MiX\"".to_string()]);
    }

    #[test]
    fn reports_unclosed_single_quote() {
        let err = scan_bind_names("select ':not_closed from dual")
            .expect_err("unclosed quote should fail");
        assert_eq!(err, SqlError::MissingEndingSingleQuote);
    }

    #[test]
    fn deduplicates_unquoted_names_case_insensitively() {
        let names = unique_bind_names(":a, :A, :\"A\", :\"A\"").expect("unique names");
        assert_eq!(names, vec!["a".to_string(), "\"A\"".to_string()]);
    }

    #[test]
    fn extracts_dml_returning_bind_names() {
        let names = returning_bind_names(
            "insert into t (value) values (:value) returning id into :id, :row_id",
        )
        .expect("returning bind names");
        assert_eq!(names, vec!["id".to_string(), "row_id".to_string()]);
    }

    #[test]
    fn extracts_single_dml_returning_projection_bind_name() {
        let name = dml_returning_single_bind_name(
            "insert into t (value) values (:value) returning obj into :out",
        )
        .expect("returning statement should parse");
        assert_eq!(name, Some("out".to_string()));

        let name = dml_returning_single_bind_name(
            "insert into t (value) values (:value) returning obj into :out, :extra",
        )
        .expect("returning statement should parse");
        assert_eq!(name, None);
    }

    #[test]
    fn rewrites_single_dml_returning_projection() {
        let statement = "insert into t (value) values (:value) returning obj_col into :out";
        let rewritten = rewrite_dml_returning_projection(statement, "STRINGVALUE")
            .expect("returning statement should parse");
        assert_eq!(
            rewritten,
            Some(
                "insert into t (value) values (:value) returning (obj_col).STRINGVALUE into :out"
                    .to_string()
            )
        );
    }

    #[test]
    fn extracts_unique_plsql_assignment_output_binds() {
        let names = plsql_assignment_bind_names("begin :out := func(:in_value); :OUT := 1; end;")
            .expect("assignment bind names");
        assert_eq!(names, vec!["out".to_string()]);
    }

    #[test]
    fn extracts_plsql_function_return_bind_name() {
        assert_eq!(
            plsql_function_return_bind_name("begin :ret := pkg.func(:arg); end;"),
            Some("ret".to_string())
        );
        assert_eq!(
            plsql_function_return_bind_name("begin pkg.proc(:arg); end;"),
            None
        );
    }

    #[test]
    fn converts_public_bind_names_like_python_oracledb() {
        assert_eq!(public_bind_name("abc"), "ABC");
        assert_eq!(public_bind_name("\"MiX\""), "MiX");
    }

    #[test]
    fn rewrites_bind_placeholders_before_returning_only() {
        assert_eq!(
            generated_object_attr_bind_name("value-1", "attr"),
            "ORADB_OBJ_VALUE_1_ATTR"
        );
        assert_eq!(
            replace_input_bind_placeholder(
                "insert into t values (:value, ':value') returning obj into :value",
                "value",
                "OBJ(:ORADB_OBJ_VALUE_ATTR)"
            ),
            "insert into t values (OBJ(:ORADB_OBJ_VALUE_ATTR), ':value') returning obj into :value"
        );
    }

    #[test]
    fn skips_comments_and_quoted_identifiers_like_reference_parser() {
        assert_eq!(
            public_unique_names(
                "--begin :value2 := :a + :b + :c +:a +3; end;\n\
                 begin :value2 := :a + :c +3; end; -- not a :bind_variable"
            ),
            vec!["VALUE2", "A", "C"]
        );
        assert_eq!(
            public_unique_names(
                "/*--select * from :a where :a = 1\n\
                 select * from table_names where :a = 1*/\n\
                 select :table_name, :value from dual"
            ),
            vec!["TABLE_NAME", "VALUE"]
        );
        assert_eq!(
            public_unique_names(r#"select ":test", :a from dual"#),
            vec!["A"]
        );
        assert_eq!(
            public_unique_names(r#"select "/*_value1" + : "VaLue_2" + :"*/3VALUE" from dual"#),
            vec!["VaLue_2", "*/3VALUE"]
        );
    }

    #[test]
    fn supports_reference_quoted_bind_names() {
        assert_eq!(
            public_unique_names(r#"select :"percent%" from dual"#),
            vec!["percent%"]
        );
        assert_eq!(
            public_unique_names(r#"select : "q?marks" from dual"#),
            vec!["q?marks"]
        );
        assert_eq!(
            public_unique_names(r#"select "col:nns", :"col:ons", :id from dual"#),
            vec!["col:ons", "ID"]
        );
    }

    #[test]
    fn skips_qstrings_and_json_constant_colons() {
        assert_eq!(
            public_unique_names(
                "select :a, q'{This contains ' and \" and : just fine}', :b, \
                 q'[This contains ' and \" and : just fine]', :c, \
                 q'<This contains ' and \" and : just fine>', :d, \
                 q'(This contains ' and \" and : just fine)', :e, \
                 q'$This contains ' and \" and : just fine$', :f from dual"
            ),
            vec!["A", "B", "C", "D", "E", "F"]
        );
        assert_eq!(
            public_unique_names(
                "select json_object('foo':dummy), :bv1, json_object('foo'::bv2), \
                 :bv3, json { 'key1': 57, 'key2' : 58 }, :bv4 from dual"
            ),
            vec!["BV1", "BV2", "BV3", "BV4"]
        );
    }

    #[test]
    fn reports_reference_qstring_errors() {
        assert_eq!(
            scan_bind_names("select q'[something from dual")
                .expect_err("unclosed q-string should fail"),
            SqlError::MissingEndingSingleQuote
        );
        assert_eq!(
            scan_bind_names("select q'[abc'], 5 from dual")
                .expect_err("mismatched q-string should fail"),
            SqlError::MissingEndingSingleQuote
        );
    }

    fn public_unique_names(statement: &str) -> Vec<String> {
        unique_bind_names(statement)
            .expect("statement should parse")
            .iter()
            .map(|name| public_bind_name(name))
            .collect()
    }
}
