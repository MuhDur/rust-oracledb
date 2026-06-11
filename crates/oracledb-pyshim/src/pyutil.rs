
use oracledb::protocol::thin::{
    ColumnMetadata, QueryValue, ORA_TYPE_NUM_BLOB,
    ORA_TYPE_NUM_CLOB,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::*;

pub(crate) fn get_string_attr(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<String> {
    obj.getattr(name)?.extract()
}

pub(crate) fn get_optional_string_attr(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<String>> {
    let value = obj.getattr(name)?;
    if value.is_none() {
        Ok(None)
    } else {
        value.extract().map(Some)
    }
}

pub(crate) fn extract_optional_string(value: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    if value.is_none() {
        Ok(None)
    } else {
        value.extract().map(Some)
    }
}

pub(crate) fn get_optional_u32_attr(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<u32>> {
    if !obj.hasattr(name)? {
        return Ok(None);
    }
    let value = obj.getattr(name)?;
    if value.is_none() {
        Ok(None)
    } else {
        value.extract().map(Some)
    }
}

pub(crate) fn get_optional_bool_attr(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<bool>> {
    if !obj.hasattr(name)? {
        return Ok(None);
    }
    let value = obj.getattr(name)?;
    if value.is_none() {
        Ok(None)
    } else {
        value.extract().map(Some)
    }
}

pub(crate) fn normalize_connect_string(dsn: String) -> String {
    dsn.split_once("://")
        .map(|(_, connect_string)| connect_string.to_string())
        .unwrap_or(dsn)
}

pub(crate) fn is_user_without_password_dsn(dsn: &str) -> bool {
    let Some((credentials, connect_string)) = dsn.split_once('@') else {
        return false;
    };
    !credentials.is_empty()
        && !credentials.contains('/')
        && !credentials.contains(':')
        && !connect_string.is_empty()
}

pub(crate) fn get_connect_sdu_attr(obj: &Bound<'_, PyAny>) -> PyResult<Option<u32>> {
    if let Some(sdu) = get_optional_u32_attr(obj, "sdu")? {
        return Ok(Some(sdu));
    }
    if !obj.hasattr("description_list")? {
        return Ok(None);
    }
    let descriptions = obj.getattr("description_list")?.getattr("children")?;
    if descriptions.len()? == 0 {
        return Ok(None);
    }
    let description = descriptions.get_item(0)?;
    get_optional_u32_attr(&description, "sdu")
}

pub(crate) fn get_app_context_attr(obj: &Bound<'_, PyAny>) -> PyResult<Vec<(String, String, String)>> {
    let value = obj.getattr("appcontext")?;
    if value.is_none() {
        return Ok(Vec::new());
    }
    let list = value
        .cast::<PyList>()
        .map_err(|_| PyRuntimeError::new_err("appcontext should be a list"))?;
    list.iter()
        .map(|entry| entry.extract::<(String, String, String)>())
        .collect()
}

pub(crate) fn py_optional_u8_attr(value: &Bound<'_, PyAny>, name: &str) -> PyResult<u8> {
    match value.getattr(name) {
        Ok(attr) => attr.extract::<u8>(),
        Err(_) => Ok(0),
    }
}

pub(crate) fn py_optional_u32_attr(value: &Bound<'_, PyAny>, name: &str) -> PyResult<u32> {
    match value.getattr(name) {
        Ok(attr) => attr.extract::<u32>(),
        Err(_) => Ok(0),
    }
}

pub(crate) fn py_required_u8_attr(value: &Bound<'_, PyAny>, name: &str) -> PyResult<u8> {
    value.getattr(name)?.extract::<u8>()
}

pub(crate) fn py_date_time_fields(
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<(i32, u8, u8, u8, u8, u8, u32)>> {
    if !(value.hasattr("year")? && value.hasattr("month")? && value.hasattr("day")?) {
        return Ok(None);
    }
    let microsecond = py_optional_u32_attr(value, "microsecond")?;
    Ok(Some((
        value.getattr("year")?.extract::<i32>()?,
        py_required_u8_attr(value, "month")?,
        py_required_u8_attr(value, "day")?,
        py_optional_u8_attr(value, "hour")?,
        py_optional_u8_attr(value, "minute")?,
        py_optional_u8_attr(value, "second")?,
        microsecond * 1000,
    )))
}

pub(crate) fn py_type_name(typ: &Bound<'_, PyAny>) -> String {
    typ.getattr("name")
        .or_else(|_| typ.getattr("__name__"))
        .and_then(|value| value.extract::<String>())
        .unwrap_or_default()
}

pub(crate) fn py_value_type_name(value: &Bound<'_, PyAny>) -> String {
    value
        .get_type()
        .getattr("__name__")
        .and_then(|name| name.extract::<String>())
        .unwrap_or_default()
}

pub(crate) fn python_int_from_value(value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let py = value.py();
    if let Ok(text) = value.extract::<String>() {
        return python_int_from_decimal_text(py, &text);
    }
    let builtins = PyModule::import(py, "builtins")?;
    Ok(builtins.getattr("int")?.call1((value,))?.unbind())
}

pub(crate) fn python_int_from_decimal_text(py: Python<'_>, text: &str) -> PyResult<Py<PyAny>> {
    let decimal = PyModule::import(py, "decimal")?
        .getattr("Decimal")?
        .call1((text,))?;
    let builtins = PyModule::import(py, "builtins")?;
    Ok(builtins.getattr("int")?.call1((decimal,))?.unbind())
}

pub(crate) fn quoted_oracle_string(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn user_identifier(value: &str) -> PyResult<String> {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '#'))
    {
        Ok(value.to_ascii_uppercase())
    } else {
        Err(not_implemented("quoted Oracle username"))
    }
}

pub(crate) fn query_value_to_string(value: &Option<QueryValue>) -> Option<String> {
    match value {
        Some(QueryValue::Text(value)) => Some(value.clone()),
        Some(QueryValue::Rowid(value)) => Some(value.clone()),
        Some(QueryValue::Raw(value)) => String::from_utf8(value.clone()).ok(),
        Some(QueryValue::BinaryDouble(value)) => Some(value.clone()),
        Some(QueryValue::Number { text, .. }) => Some(text.clone()),
        Some(QueryValue::DateTime { .. }) => None,
        Some(QueryValue::Array(_)) => None,
        Some(QueryValue::Cursor { .. }) => None,
        Some(QueryValue::Object { .. }) => None,
        Some(QueryValue::Lob { .. }) => None,
        None => None,
    }
}

pub(crate) fn query_value_to_i64(value: &Option<QueryValue>) -> PyResult<i64> {
    query_value_to_string(value)
        .ok_or_else(|| PyRuntimeError::new_err("query returned NULL where integer was expected"))?
        .parse()
        .map_err(runtime_error)
}

pub(crate) fn query_value_to_u32(value: &Option<QueryValue>) -> Option<u32> {
    query_value_to_string(value)?.parse().ok()
}

pub(crate) fn columns_require_define(columns: &[ColumnMetadata]) -> bool {
    columns
        .iter()
        .any(|metadata| matches!(metadata.ora_type_num, ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB))
}

pub(crate) fn query_value_to_i8(value: &Option<QueryValue>) -> Option<i8> {
    query_value_to_string(value)?.parse().ok()
}

pub(crate) fn sql_identifier(value: &str) -> PyResult<String> {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '#'))
    {
        Ok(value.to_string())
    } else {
        Err(not_implemented("quoted Oracle identifier"))
    }
}

pub(crate) fn first_sql_keyword(statement: &str) -> String {
    statement
        .trim_start()
        .split(|ch: char| !ch.is_ascii_alphabetic())
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
}

pub(crate) fn parse_alter_session_value(statement: &str, key: &str) -> Option<String> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    let prefix = format!("alter session set {key}");
    if !lower.starts_with(&prefix) {
        return None;
    }
    let mut value = trimmed.get(prefix.len()..)?.trim_start();
    if let Some(stripped) = value.strip_prefix('=') {
        value = stripped.trim_start();
    }
    value
        .split_whitespace()
        .next()
        .map(|value| value.trim_matches('"').to_string())
        .filter(|value| !value.is_empty())
}
