//! Query-by-example (QBE) filter translation to SQL/JSON predicates.
//!
//! SODA filter specs are JSON documents describing a query. We translate them
//! to an Oracle SQL `WHERE`-clause fragment built on `JSON_EXISTS` over the
//! content column. Values are inlined into the JSON path expression because
//! Oracle does not accept bind variables inside JSON path predicates; every
//! inlined value is escaped via [`escape_json_path_string`].
//!
//! Supported operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$like`,
//! `$regex`, `$startsWith`, `$hasSubstring`, `$instr`, `$contains`, `$upper`,
//! `$lower`, `$type`, `$exists`, `$date`, `$and`, `$or`, `$nor`, `$not`, and a
//! bare scalar (implicit `$eq`). `$orderby` is extracted separately and does
//! not contribute to the `WHERE` clause.
//!
//! Any operator not in that set returns [`SodaError::NotSupported`] so the
//! caller never silently produces a wrong query.

use serde_json::Value;

use super::error::{Result, SodaError};

/// Translate a SODA QBE filter into a SQL `WHERE`-clause fragment.
///
/// `content_col` is the (already-quoted-or-bare) column expression holding the
/// JSON document (for the default native collection this is `DATA`).
///
/// Returns the boolean SQL fragment (without the leading `WHERE`). An empty
/// filter (`{}`) yields `"1=1"`.
pub fn qbe_to_where_clause(filter: &Value, content_col: &str) -> Result<String> {
    let obj = filter
        .as_object()
        .ok_or_else(|| SodaError::Qbe("filter must be a JSON object".to_string()))?;

    // Separate out the non-predicate $orderby key.
    let mut clauses = Vec::new();
    for (key, val) in obj {
        if key == "$orderby" {
            continue;
        }
        clauses.push(translate_top_level(key, val, content_col)?);
    }

    if clauses.is_empty() {
        return Ok("1=1".to_string());
    }
    Ok(clauses.join(" AND "))
}

/// Extract an `ORDER BY` fragment (without the leading `ORDER BY`) from the
/// filter's `$orderby` key, if present. Supports the list form
/// `[{"path": "name", "order": "desc"}, ...]`.
pub fn extract_orderby(filter: &Value, content_col: &str) -> Result<Option<String>> {
    let Some(spec) = filter.get("$orderby") else {
        return Ok(None);
    };
    let arr = spec.as_array().ok_or_else(|| {
        SodaError::NotSupported("$orderby object form is not supported in thin mode".to_string())
    })?;
    let mut parts = Vec::new();
    for entry in arr {
        let path = entry
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| SodaError::Qbe("$orderby entry requires a string 'path'".to_string()))?;
        let order = entry.get("order").and_then(Value::as_str).unwrap_or("asc");
        let dir = match order.to_ascii_lowercase().as_str() {
            "asc" => "ASC",
            "desc" => "DESC",
            other => {
                return Err(SodaError::Qbe(format!("invalid $orderby order: {other}")));
            }
        };
        // Use JSON_VALUE to project the scalar at the path for ordering.
        let path_expr = field_to_json_path(path);
        parts.push(format!(
            "JSON_VALUE({content_col}, '{}') {dir}",
            escape_sql_literal(&path_expr)
        ));
    }
    if parts.is_empty() {
        return Ok(None);
    }
    Ok(Some(parts.join(", ")))
}

/// Translate a top-level key/value into a SQL boolean fragment.
fn translate_top_level(key: &str, val: &Value, content_col: &str) -> Result<String> {
    match key {
        "$and" => combine_logical(val, content_col, "AND"),
        "$or" => combine_logical(val, content_col, "OR"),
        "$nor" => {
            let inner = combine_logical(val, content_col, "OR")?;
            Ok(format!("NOT ({inner})"))
        }
        _ if key.starts_with('$') => Err(SodaError::NotSupported(format!(
            "top-level operator {key} is not supported"
        ))),
        // A field predicate: key is a path, value is a scalar or operator object.
        _ => translate_field_predicate(key, val, content_col),
    }
}

fn combine_logical(val: &Value, content_col: &str, joiner: &str) -> Result<String> {
    let arr = val
        .as_array()
        .ok_or_else(|| SodaError::Qbe(format!("${} requires an array", joiner.to_lowercase())))?;
    if arr.is_empty() {
        return Ok("1=1".to_string());
    }
    let mut parts = Vec::new();
    for item in arr {
        parts.push(qbe_to_where_clause(item, content_col)?);
    }
    Ok(format!("({})", parts.join(&format!(") {joiner} ("))))
}

/// Translate `{"field": <scalar|operators>}` into a boolean SQL fragment.
fn translate_field_predicate(field: &str, val: &Value, content_col: &str) -> Result<String> {
    let path = field_to_json_path(field);
    match val {
        // Bare scalar -> implicit equality.
        Value::Object(ops) => {
            let mut parts = Vec::new();
            for (op, operand) in ops {
                parts.push(translate_operator(&path, op, operand, content_col)?);
            }
            if parts.is_empty() {
                // {"field": {}} -> field exists
                return Ok(json_exists(content_col, &path, ""));
            }
            Ok(join_and(parts))
        }
        _ => {
            let lit = scalar_to_path_literal(val)?;
            Ok(json_exists(content_col, &path, &format!("?(@ == {lit})")))
        }
    }
}

/// Translate a single operator predicate on a path.
fn translate_operator(path: &str, op: &str, operand: &Value, content_col: &str) -> Result<String> {
    let cmp = |sym: &str| -> Result<String> {
        let lit = scalar_to_path_literal(operand)?;
        Ok(json_exists(content_col, path, &format!("?(@ {sym} {lit})")))
    };
    match op {
        "$eq" => cmp("=="),
        "$ne" => {
            let lit = scalar_to_path_literal(operand)?;
            // != inside a path predicate; matches docs where the value differs.
            Ok(json_exists(content_col, path, &format!("?(@ != {lit})")))
        }
        "$gt" => cmp(">"),
        "$gte" => cmp(">="),
        "$lt" => cmp("<"),
        "$lte" => cmp("<="),
        "$like" => {
            let s = operand_as_str(operand, "$like")?;
            Ok(json_exists(
                content_col,
                path,
                &format!("?(@ like \"{}\")", escape_path_double_quoted(&s)),
            ))
        }
        "$regex" => {
            let s = operand_as_str(operand, "$regex")?;
            Ok(json_exists(
                content_col,
                path,
                &format!("?(@ like_regex \"{}\")", escape_path_double_quoted(&s)),
            ))
        }
        "$startsWith" => {
            let s = operand_as_str(operand, "$startsWith")?;
            Ok(json_exists(
                content_col,
                path,
                &format!("?(@ starts with \"{}\")", escape_path_double_quoted(&s)),
            ))
        }
        // Substring containment: $hasSubstring / $instr / $contains all map to a
        // substring match. We use a regex anchored to "contains" semantics so it
        // works without a JSON search index (which $contains text-index needs).
        "$hasSubstring" | "$instr" | "$contains" => {
            let s = operand_as_str(operand, op)?;
            Ok(json_exists(
                content_col,
                path,
                &format!(
                    "?(@ like_regex \"{}\")",
                    escape_path_double_quoted(&regex_escape(&s))
                ),
            ))
        }
        // Case-folding wrapper: {"$upper": {"$startsWith": "JO"}} etc.
        "$upper" => fold_case(path, operand, content_col, "upper"),
        "$lower" => fold_case(path, operand, content_col, "lower"),
        "$type" => {
            let s = operand_as_str(operand, "$type")?;
            Ok(json_exists(
                content_col,
                path,
                &format!("?(@.type() == \"{}\")", escape_path_double_quoted(&s)),
            ))
        }
        "$exists" => {
            let exists = operand.as_bool().unwrap_or(true);
            let inner = json_exists(content_col, path, "");
            if exists {
                Ok(inner)
            } else {
                Ok(format!("NOT ({inner})"))
            }
        }
        "$not" => {
            let inner = translate_not(path, operand, content_col)?;
            Ok(format!("NOT ({inner})"))
        }
        "$date" => translate_date(path, operand, content_col),
        other => Err(SodaError::NotSupported(format!(
            "QBE operator {other} is not supported"
        ))),
    }
}

/// `$not` wraps either a single operator object or a scalar (implicit eq).
fn translate_not(path: &str, operand: &Value, content_col: &str) -> Result<String> {
    match operand {
        Value::Object(ops) => {
            let mut parts = Vec::new();
            for (op, inner) in ops {
                parts.push(translate_operator(path, op, inner, content_col)?);
            }
            Ok(join_and(parts))
        }
        _ => {
            let lit = scalar_to_path_literal(operand)?;
            Ok(json_exists(content_col, path, &format!("?(@ == {lit})")))
        }
    }
}

/// `$upper` / `$lower` case-folded comparison wrapper.
fn fold_case(path: &str, operand: &Value, content_col: &str, func: &str) -> Result<String> {
    let ops = operand
        .as_object()
        .ok_or_else(|| SodaError::Qbe(format!("${func} requires an operator object")))?;
    let mut parts = Vec::new();
    for (op, inner) in ops {
        let s = operand_as_str(inner, op)?;
        let folded = if func == "upper" {
            s.to_uppercase()
        } else {
            s.to_lowercase()
        };
        // Oracle JSON path uses the item method form `@.upper()` / `@.lower()`,
        // not the SQL function form `upper(@)`.
        let pred = match op.as_str() {
            "$eq" => format!(
                "?(@.{func}() == \"{}\")",
                escape_path_double_quoted(&folded)
            ),
            "$startsWith" => format!(
                "?(@.{func}() starts with \"{}\")",
                escape_path_double_quoted(&folded)
            ),
            "$like" => format!(
                "?(@.{func}() like \"{}\")",
                escape_path_double_quoted(&folded)
            ),
            "$hasSubstring" | "$instr" | "$contains" => format!(
                "?(@.{func}() like_regex \"{}\")",
                escape_path_double_quoted(&regex_escape(&folded))
            ),
            other => {
                return Err(SodaError::NotSupported(format!(
                    "${func} with inner operator {other} is not supported"
                )));
            }
        };
        parts.push(json_exists(content_col, path, &pred));
    }
    Ok(join_and(parts))
}

/// Join one-or-more boolean fragments with ` AND `, wrapping in parentheses
/// only when there is more than one.
fn join_and(parts: Vec<String>) -> String {
    if parts.len() == 1 {
        parts.into_iter().next().unwrap_or_default()
    } else {
        format!("({})", parts.join(" AND "))
    }
}

/// `$date` operator: compares a path holding an ISO-8601 date string.
///
/// SODA stores dates as ISO-8601 strings (`YYYY-MM-DD` / `YYYY-MM-DDThh:mm:ss`),
/// for which lexical comparison is equivalent to chronological comparison. The
/// JSON path `.date()` item method is not available on every server build (it
/// raises ORA-40597 on the 23ai container), so we compare the stored strings
/// directly. Supports `{"$date": "2000-12-15"}` (equality) and
/// `{"$date": {"$gt": "2000-01-01"}}` (and the other comparisons).
fn translate_date(path: &str, operand: &Value, content_col: &str) -> Result<String> {
    let pred = match operand {
        Value::String(s) => {
            format!("?(@ == \"{}\")", escape_path_double_quoted(s))
        }
        Value::Object(ops) => {
            let mut sub = Vec::new();
            for (op, inner) in ops {
                let s = operand_as_str(inner, op)?;
                let sym = match op.as_str() {
                    "$eq" => "==",
                    "$gt" => ">",
                    "$gte" => ">=",
                    "$lt" => "<",
                    "$lte" => "<=",
                    "$ne" => "!=",
                    other => {
                        return Err(SodaError::NotSupported(format!(
                            "$date with inner operator {other} is not supported"
                        )));
                    }
                };
                sub.push(format!("@ {sym} \"{}\"", escape_path_double_quoted(&s)));
            }
            format!("?({})", sub.join(" && "))
        }
        _ => {
            return Err(SodaError::Qbe(
                "$date operand must be a string or operator object".to_string(),
            ));
        }
    };
    Ok(json_exists(content_col, path, &pred))
}

// --- helpers ---------------------------------------------------------------

/// Build a `JSON_EXISTS(col, '$.path<pred>')` fragment.
fn json_exists(content_col: &str, path: &str, pred: &str) -> String {
    let full = format!("{path}{pred}");
    format!(
        "JSON_EXISTS({content_col}, '{}')",
        escape_sql_literal(&full)
    )
}

/// Convert a SODA field reference into a JSON path. The reference can already
/// contain array steps such as `locations[*].city` or `locations[0 to 1].city`.
/// Dotted segments become path steps. Returns a path beginning with `$`.
fn field_to_json_path(field: &str) -> String {
    if field.starts_with('$') {
        // Already a path expression.
        return field.to_string();
    }
    // Split on '.' but keep bracket array steps attached to their segment.
    let mut path = String::from("$");
    for segment in field.split('.') {
        path.push('.');
        path.push_str(segment);
    }
    path
}

/// Render a JSON scalar as a path-predicate literal (numbers/booleans bare,
/// strings double-quoted with escaping). Null is rendered as `null`.
fn scalar_to_path_literal(v: &Value) -> Result<String> {
    match v {
        Value::String(s) => Ok(format!("\"{}\"", escape_path_double_quoted(s))),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Null => Ok("null".to_string()),
        _ => Err(SodaError::Qbe(
            "comparison operand must be a scalar (string, number, bool, null)".to_string(),
        )),
    }
}

fn operand_as_str(v: &Value, op: &str) -> Result<String> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        _ => Err(SodaError::Qbe(format!(
            "{op} operand must be a string or number"
        ))),
    }
}

/// Escape a string for embedding inside a single-quoted SQL literal: double any
/// single quote.
fn escape_sql_literal(s: &str) -> String {
    s.replace('\'', "''")
}

/// Escape a string for embedding inside a double-quoted JSON path string
/// literal: backslash and double-quote.
fn escape_path_double_quoted(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            _ => out.push(ch),
        }
    }
    out
}

/// Escape regex metacharacters so a literal substring becomes a safe regex.
fn regex_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if "\\^$.|?*+()[]{}".contains(ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// Public helper: escape a string for a JSON-path double-quoted literal.
/// Exposed for reuse and to make injection-safety testable.
pub fn escape_json_path_string(s: &str) -> String {
    escape_path_double_quoted(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn where_of(filter: serde_json::Value) -> String {
        qbe_to_where_clause(&filter, "DATA").expect("translate")
    }

    #[test]
    fn empty_filter_is_true() {
        assert_eq!(where_of(json!({})), "1=1");
    }

    #[test]
    fn bare_scalar_is_implicit_eq() {
        let w = where_of(json!({"name": "John"}));
        assert_eq!(w, r#"JSON_EXISTS(DATA, '$.name?(@ == "John")')"#);
    }

    #[test]
    fn eq_operator() {
        let w = where_of(json!({"age": {"$eq": 22}}));
        assert_eq!(w, "JSON_EXISTS(DATA, '$.age?(@ == 22)')");
    }

    #[test]
    fn gt_numeric() {
        let w = where_of(json!({"age": {"$gt": 18}}));
        assert_eq!(w, "JSON_EXISTS(DATA, '$.age?(@ > 18)')");
    }

    #[test]
    fn lt_numeric() {
        let w = where_of(json!({"age": {"$lt": 25}}));
        assert_eq!(w, "JSON_EXISTS(DATA, '$.age?(@ < 25)')");
    }

    #[test]
    fn like_operator() {
        let w = where_of(json!({"name": {"$like": "J%n"}}));
        assert_eq!(w, r#"JSON_EXISTS(DATA, '$.name?(@ like "J%n")')"#);
    }

    #[test]
    fn regex_operator() {
        let w = where_of(json!({"name": {"$regex": ".*[ho]n"}}));
        assert_eq!(w, r#"JSON_EXISTS(DATA, '$.name?(@ like_regex ".*[ho]n")')"#);
    }

    #[test]
    fn nested_dotted_path() {
        let w = where_of(json!({"locations.city": {"$regex": "^Ban.*"}}));
        assert_eq!(
            w,
            r#"JSON_EXISTS(DATA, '$.locations.city?(@ like_regex "^Ban.*")')"#
        );
    }

    #[test]
    fn starts_with() {
        let w = where_of(json!({"name": {"$startsWith": "John"}}));
        assert_eq!(w, r#"JSON_EXISTS(DATA, '$.name?(@ starts with "John")')"#);
    }

    #[test]
    fn has_substring_is_regex_escaped() {
        let w = where_of(json!({"name": {"$hasSubstring": "John"}}));
        assert_eq!(w, r#"JSON_EXISTS(DATA, '$.name?(@ like_regex "John")')"#);
    }

    #[test]
    fn instr_alias() {
        let w = where_of(json!({"name": {"$instr": "John"}}));
        assert_eq!(w, r#"JSON_EXISTS(DATA, '$.name?(@ like_regex "John")')"#);
    }

    #[test]
    fn contains_alias() {
        let w = where_of(json!({"name": {"$contains": "John"}}));
        assert_eq!(w, r#"JSON_EXISTS(DATA, '$.name?(@ like_regex "John")')"#);
    }

    #[test]
    fn upper_starts_with() {
        let w = where_of(json!({"name": {"$upper": {"$startsWith": "jo"}}}));
        assert_eq!(
            w,
            r#"JSON_EXISTS(DATA, '$.name?(@.upper() starts with "JO")')"#
        );
    }

    #[test]
    fn type_array() {
        let w = where_of(json!({"locations": {"$type": "array"}}));
        assert_eq!(
            w,
            r#"JSON_EXISTS(DATA, '$.locations?(@.type() == "array")')"#
        );
    }

    #[test]
    fn not_eq() {
        let w = where_of(json!({"age": {"$not": {"$eq": 22}}}));
        assert_eq!(w, "NOT (JSON_EXISTS(DATA, '$.age?(@ == 22)'))");
    }

    #[test]
    fn not_range_multiple_ops() {
        let w = where_of(json!({"age": {"$not": {"$lt": 30, "$gt": 10}}}));
        // order of keys in a serde_json object is insertion order; $lt then $gt
        assert!(w.starts_with("NOT ("));
        assert!(w.contains("@ < 30"));
        assert!(w.contains("@ > 10"));
    }

    #[test]
    fn or_with_array_wildcard() {
        let w = where_of(json!({
            "$or": [
                {"age": {"$gt": 50}},
                {"locations[*].city": {"$like": "%Ban%"}}
            ]
        }));
        assert_eq!(
            w,
            r#"(JSON_EXISTS(DATA, '$.age?(@ > 50)')) OR (JSON_EXISTS(DATA, '$.locations[*].city?(@ like "%Ban%")'))"#
        );
    }

    #[test]
    fn and_with_range_index() {
        let w = where_of(json!({
            "$and": [
                {"age": {"$gt": 40}},
                {"locations[0 to 1].city": {"$like": "%aras"}}
            ]
        }));
        assert_eq!(
            w,
            r#"(JSON_EXISTS(DATA, '$.age?(@ > 40)')) AND (JSON_EXISTS(DATA, '$.locations[0 to 1].city?(@ like "%aras")'))"#
        );
    }

    #[test]
    fn date_equality() {
        let w = where_of(json!({"birthday": {"$date": "2000-12-15"}}));
        assert_eq!(w, r#"JSON_EXISTS(DATA, '$.birthday?(@ == "2000-12-15")')"#);
    }

    #[test]
    fn date_gt() {
        let w = where_of(json!({"birthday": {"$date": {"$gt": "2000-01-01"}}}));
        assert_eq!(w, r#"JSON_EXISTS(DATA, '$.birthday?(@ > "2000-01-01")')"#);
    }

    #[test]
    fn unsupported_operator_errors() {
        let err = qbe_to_where_clause(&json!({"x": {"$nearby": 1}}), "DATA").unwrap_err();
        assert!(matches!(err, SodaError::NotSupported(_)));
    }

    #[test]
    fn sql_injection_in_string_is_escaped() {
        // A single quote in the value must be doubled so it cannot break out of
        // the SQL literal.
        let w = where_of(json!({"name": "O'Brien"}));
        assert!(w.contains("O''Brien"), "got: {w}");
        // No unescaped single quote that would terminate the literal early.
    }

    #[test]
    fn orderby_extracted() {
        let filter = json!({"$orderby": [{"path": "name", "order": "desc"}]});
        let ob = extract_orderby(&filter, "DATA").unwrap().unwrap();
        assert!(ob.contains("DESC"));
        assert!(ob.contains("JSON_VALUE(DATA"));
        // $orderby alone produces no WHERE predicate.
        assert_eq!(where_of(filter), "1=1");
    }
}
