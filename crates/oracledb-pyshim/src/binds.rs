use std::sync::{Arc, Mutex};

use oracledb::protocol::sql;
use oracledb::protocol::thin::{output_bind as output_only_bind, returning_output_bind, BindValue};
use oracledb::Connection as RustConnection;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};

use crate::*;

/// PL/SQL assignment-target binds are in/out when a concrete value was
/// supplied (e.g. `begin :v := :v + 5; end;` with `:v` pre-set). The
/// reference always writes the var's current value for PL/SQL binds; the
/// server's io vector reports the out direction regardless. Only valueless
/// binds degrade to typed output placeholders.
pub(crate) fn plsql_output_bind(value: BindValue) -> BindValue {
    match value {
        BindValue::Text(_)
        | BindValue::Raw(_)
        | BindValue::Number(_)
        | BindValue::BinaryInteger(_)
        | BindValue::BinaryDouble(_)
        | BindValue::DateTime { .. }
        | BindValue::Timestamp { .. }
        | BindValue::Lob { .. } => value,
        other => output_only_bind(other),
    }
}

#[allow(clippy::too_many_arguments)] // pre-existing lint at pre-split HEAD 978491a; not movement-induced
pub(crate) fn extract_bind_values(
    py: Python<'_>,
    statement: &str,
    parameters: Option<&Bound<'_, PyAny>>,
    keyword_parameters: Option<&Bound<'_, PyAny>>,
    named_input_sizes: &[(String, Py<PyAny>)],
    has_positional_input_sizes: bool,
    previous_bind_names: &[String],
    previous_bind_vars: &[Py<ThinVar>],
) -> PyResult<Vec<BindValue>> {
    let has_parameters = has_bind_payload(parameters)?;
    let has_keywords = has_bind_payload(keyword_parameters)?;
    if has_parameters && has_keywords {
        return Err(raise_oracledb_driver_error("ERR_ARGS_AND_KEYWORD_ARGS"));
    }
    if let Some(value) = keyword_parameters.filter(|_| has_keywords) {
        let dict = value.cast::<PyDict>()?;
        return extract_named_bind_values(
            py,
            statement,
            Some(dict),
            named_input_sizes,
            previous_bind_names,
            previous_bind_vars,
            false,
        );
    }
    let Some(value) = parameters else {
        if !named_input_sizes.is_empty() {
            return extract_named_bind_values(
                py,
                statement,
                None,
                named_input_sizes,
                previous_bind_names,
                previous_bind_vars,
                false,
            );
        }
        return Ok(Vec::new());
    };
    if !has_parameters {
        if has_positional_input_sizes {
            let row_values = positional_bind_items(value)?;
            if row_values.is_empty() {
                if let Some(name) = unique_sql_bind_names(statement)?.first() {
                    return Err(dpy_bind_error(
                        "DPY-4010",
                        format!(
                            "a bind variable replacement value for placeholder \":{name}\" was not provided"
                        ),
                    ));
                }
                return Ok(Vec::new());
            }
            return extract_positional_bind_values_for_execute(
                py,
                statement,
                value,
                named_input_sizes,
            );
        }
        if !named_input_sizes.is_empty() {
            return extract_named_bind_values(
                py,
                statement,
                None,
                named_input_sizes,
                previous_bind_names,
                previous_bind_vars,
                false,
            );
        }
        return Ok(Vec::new());
    }
    if let Ok(dict) = value.cast::<PyDict>() {
        return extract_named_bind_values(
            py,
            statement,
            Some(dict),
            named_input_sizes,
            previous_bind_names,
            previous_bind_vars,
            false,
        );
    }
    extract_positional_bind_values_for_execute(py, statement, value, named_input_sizes)
}

pub(crate) enum BindSourceKind {
    Parameters,
    Keywords,
}

pub(crate) fn thin_var_null_object_type(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<DbObjectTypeImpl>> {
    let Some(var) = thin_var_from_value(value)? else {
        return Ok(None);
    };
    let var = var.borrow(py);
    let Some(object_type) = var.object_type.clone() else {
        return Ok(None);
    };
    if var.get_py_value(py)?.bind(py).is_none() {
        return Ok(Some(object_type));
    }
    Ok(None)
}

pub(crate) fn object_bind_sql_expr(
    py: Python<'_>,
    bind_name: &str,
    value: &Bound<'_, PyAny>,
    effective_dict: &Bound<'_, PyDict>,
    allow_null_var_cast: bool,
) -> PyResult<Option<String>> {
    if let Some(object) = py_db_object_impl(value)? {
        let object_type = object.object_type.clone();
        if object_type.is_collection {
            if object_type.is_assoc_array || object_type.package_name.is_some() {
                return Ok(None);
            }
            let collection_values = object
                .collection_values
                .lock()
                .map_err(runtime_error)?
                .iter()
                .map(|value| value.clone_ref(py))
                .collect::<Vec<_>>();
            let mut constructor_args = Vec::with_capacity(collection_values.len());
            for (index, element_value) in collection_values.into_iter().enumerate() {
                let element_value_bound = element_value.bind(py);
                if element_value_bound.is_none() {
                    constructor_args.push("null".to_string());
                    continue;
                }
                let generated_name =
                    sql::generated_object_attr_bind_name(bind_name, &format!("E{index}"));
                if let Some(expr) = object_bind_sql_expr(
                    py,
                    &generated_name,
                    element_value_bound,
                    effective_dict,
                    true,
                )? {
                    constructor_args.push(expr);
                } else {
                    constructor_args.push(format!(":{generated_name}"));
                    effective_dict.set_item(&generated_name, element_value)?;
                }
            }
            return Ok(Some(format!(
                "{}({})",
                object_type._get_fqn(),
                constructor_args.join(", ")
            )));
        }
        if object_type.attrs.is_empty() {
            return Ok(None);
        }
        let mut constructor_args = Vec::with_capacity(object_type.attrs.len());
        for attr in &object_type.attrs {
            let attr_value = object.attr_bind_value(py, &attr.name)?;
            let attr_value_bound = attr_value.bind(py);
            if attr_value_bound.is_none() {
                constructor_args.push("null".to_string());
                continue;
            }
            let generated_name = sql::generated_object_attr_bind_name(bind_name, &attr.name);
            if let Some(expr) =
                object_bind_sql_expr(py, &generated_name, attr_value_bound, effective_dict, true)?
            {
                constructor_args.push(expr);
            } else {
                constructor_args.push(object_attr_bind_sql_expr(
                    attr,
                    attr_value_bound,
                    &generated_name,
                )?);
                effective_dict.set_item(&generated_name, attr_value)?;
            }
        }
        return Ok(Some(format!(
            "{}({})",
            object_type._get_fqn(),
            constructor_args.join(", ")
        )));
    }

    if allow_null_var_cast {
        if let Some(object_type) = thin_var_null_object_type(py, value)? {
            if !object_type.is_collection {
                return Ok(Some(format!("cast(null as {})", object_type._get_fqn())));
            }
        }
    }

    Ok(None)
}

pub(crate) fn object_attr_bind_sql_expr(
    attr: &DbObjectAttrImpl,
    value: &Bound<'_, PyAny>,
    bind_name: &str,
) -> PyResult<String> {
    if attr.dbtype_name == "DB_TYPE_BLOB" && value.cast::<PyString>().is_ok() {
        return Ok(format!("utl_raw.cast_to_raw(:{bind_name})"));
    }
    Ok(format!(":{bind_name}"))
}

pub(crate) fn plsql_function_return_bind_name(statement: &str) -> Option<String> {
    sql::plsql_function_return_bind_name(statement)
}

pub(crate) fn rewrite_object_bind_dict(
    py: Python<'_>,
    statement: &str,
    effective_dict: &Bound<'_, PyDict>,
) -> PyResult<(String, bool)> {
    let function_return_name = plsql_function_return_bind_name(statement);
    let dml_return_names = statement_return_bind_names(statement)?;
    let mut bind_entries = Vec::new();
    for (key, value) in effective_dict.iter() {
        bind_entries.push((key.extract::<String>()?, value.clone().unbind()));
    }

    let mut effective_statement = statement.to_string();
    let mut changed = false;
    for (key, value) in bind_entries {
        let is_function_return_bind = function_return_name
            .as_deref()
            .is_some_and(|name| bind_names_equal(name, &key));
        let is_dml_return_bind = dml_return_names
            .iter()
            .any(|name| bind_names_equal(name, &key));
        let value = value.bind(py);
        let Some(sql_expr) = object_bind_sql_expr(
            py,
            &key,
            value,
            effective_dict,
            !(is_function_return_bind || is_dml_return_bind),
        )?
        else {
            continue;
        };
        effective_statement =
            sql::replace_input_bind_placeholder(&effective_statement, &key, &sql_expr);
        let _ = effective_dict.del_item(&key);
        changed = true;
    }

    Ok((effective_statement, changed))
}

pub(crate) fn positional_bind_dict_if_complete<'py>(
    py: Python<'py>,
    statement: &str,
    value: &Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyDict>>> {
    let row_values = positional_bind_items(value)?;
    let names = unique_sql_bind_names(statement)?;
    if row_values.len() != names.len() {
        return Ok(None);
    }

    let effective_dict = PyDict::new(py);
    for (name, value) in names.iter().zip(row_values.iter()) {
        effective_dict.set_item(name, value)?;
    }
    Ok(Some(effective_dict))
}

#[allow(clippy::type_complexity)] // pre-existing lint at pre-split HEAD 978491a; not movement-induced
pub(crate) fn prepare_object_execute_inputs(
    py: Python<'_>,
    statement: &str,
    parameters: Option<&Bound<'_, PyAny>>,
    keyword_parameters: Option<&Bound<'_, PyAny>>,
) -> PyResult<(String, Option<Py<PyAny>>, Option<Py<PyAny>>)> {
    let original_parameters = parameters.map(|value| value.clone().unbind());
    let original_keywords = keyword_parameters.map(|value| value.clone().unbind());
    let has_parameters = has_bind_payload(parameters)?;
    let has_keywords = has_bind_payload(keyword_parameters)?;
    if has_parameters && has_keywords {
        return Ok((
            statement.to_string(),
            original_parameters,
            original_keywords,
        ));
    }
    let (source_kind, source_dict): (BindSourceKind, Bound<'_, PyDict>) = if has_keywords {
        let Some(value) = keyword_parameters else {
            return Ok((
                statement.to_string(),
                original_parameters,
                original_keywords,
            ));
        };
        let Ok(dict) = value.cast::<PyDict>() else {
            return Ok((
                statement.to_string(),
                original_parameters,
                original_keywords,
            ));
        };
        (BindSourceKind::Keywords, dict.clone())
    } else if has_parameters {
        let Some(value) = parameters else {
            return Ok((
                statement.to_string(),
                original_parameters,
                original_keywords,
            ));
        };
        let dict = if let Ok(dict) = value.cast::<PyDict>() {
            dict.clone()
        } else {
            let Some(dict) = positional_bind_dict_if_complete(py, statement, value)? else {
                return Ok((
                    statement.to_string(),
                    original_parameters,
                    original_keywords,
                ));
            };
            dict
        };
        (BindSourceKind::Parameters, dict)
    } else {
        return Ok((
            statement.to_string(),
            original_parameters,
            original_keywords,
        ));
    };

    let effective_dict = PyDict::new(py);
    for (key, value) in source_dict.iter() {
        effective_dict.set_item(&key, &value)?;
    }

    let (mut effective_statement, mut changed) =
        rewrite_object_bind_dict(py, statement, &effective_dict)?;

    if let Some(statement) =
        rewrite_object_return_projection(&effective_statement, &effective_dict)?
    {
        effective_statement = statement;
        changed = true;
    }

    if !changed {
        return Ok((
            statement.to_string(),
            original_parameters,
            original_keywords,
        ));
    }
    match source_kind {
        BindSourceKind::Parameters => Ok((
            effective_statement,
            Some(effective_dict.unbind().into()),
            None,
        )),
        BindSourceKind::Keywords => Ok((
            effective_statement,
            None,
            Some(effective_dict.unbind().into()),
        )),
    }
}

pub(crate) fn rewrite_object_return_projection(
    statement: &str,
    parameters: &Bound<'_, PyDict>,
) -> PyResult<Option<String>> {
    let Some(return_name) =
        sql::dml_returning_single_bind_name(statement).map_err(sql_parse_error)?
    else {
        return Ok(None);
    };
    let Some(value) = get_named_bind_value(parameters, &return_name)? else {
        return Ok(None);
    };
    let Some((_object_type, attr_name)) = thin_var_object_return_projection(value.py(), &value)?
    else {
        return Ok(None);
    };
    sql::rewrite_dml_returning_projection(statement, &attr_name).map_err(sql_parse_error)
}

pub(crate) fn thin_var_object_return_projection(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<(DbObjectTypeImpl, String)>> {
    let Some(var) = thin_var_from_value(value)? else {
        return Ok(None);
    };
    let var = var.borrow(py);
    let Some(object_type) = var.object_type.clone() else {
        return Ok(None);
    };
    let Some(attr_name) = var
        .object_return_attr
        .clone()
        .or_else(|| object_type.default_scalar_return_attr().map(str::to_string))
    else {
        return Ok(None);
    };
    Ok(Some((object_type, attr_name)))
}

pub(crate) fn has_bind_payload(value: Option<&Bound<'_, PyAny>>) -> PyResult<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    if value.is_none() {
        return Ok(false);
    }
    Ok(value.len()? > 0)
}

pub(crate) fn positional_bind_items<'py>(
    value: &Bound<'py, PyAny>,
) -> PyResult<Vec<Bound<'py, PyAny>>> {
    if value.cast::<PyDict>().is_ok() || value.cast::<PyString>().is_ok() {
        return Err(raise_oracledb_driver_error(
            "ERR_WRONG_EXECUTE_PARAMETERS_TYPE",
        ));
    }
    if let Ok(tuple) = value.cast::<PyTuple>() {
        return Ok(tuple.iter().collect());
    }
    if let Ok(list) = value.cast::<PyList>() {
        return Ok(list.iter().collect());
    }
    value
        .try_iter()
        .map_err(|_| raise_oracledb_driver_error("ERR_WRONG_EXECUTE_PARAMETERS_TYPE"))?
        .collect()
}

pub(crate) fn extract_positional_bind_values_for_execute(
    py: Python<'_>,
    statement: &str,
    value: &Bound<'_, PyAny>,
    named_input_sizes: &[(String, Py<PyAny>)],
) -> PyResult<Vec<BindValue>> {
    let row_values = positional_bind_items(value)?;
    let names = unique_sql_bind_names(statement)?;
    let return_names = statement_return_bind_names(statement)?;
    let plsql_output_names = statement_plsql_output_bind_names(statement)?;
    let input_count = names
        .iter()
        .filter(|name| {
            !return_names
                .iter()
                .any(|return_name| bind_names_equal(return_name, name))
                && !plsql_output_names
                    .iter()
                    .any(|output_name| bind_names_equal(output_name, name))
        })
        .count();
    let has_all_bind_values = row_values.len() == names.len();
    let has_input_only_values = row_values.len() == input_count;
    if !has_all_bind_values && !has_input_only_values {
        return Err(dpy_bind_error(
            "DPY-4009",
            format!(
                "{input_count} positional bind values are required but {} were provided",
                row_values.len()
            ),
        ));
    }
    let mut input_index = 0;
    let mut values = Vec::with_capacity(names.len());
    for (position, name) in names.iter().enumerate() {
        let is_return_bind = return_names
            .iter()
            .any(|return_name| bind_names_equal(return_name, name));
        let is_plsql_output_bind = plsql_output_names
            .iter()
            .any(|output_name| bind_names_equal(output_name, name));
        if is_return_bind || is_plsql_output_bind {
            let bind = if has_all_bind_values {
                py_value_to_bind(&row_values[position])?
            } else {
                let Some(input_size_var) =
                    positional_input_size_value(py, named_input_sizes, position)
                else {
                    return Err(dpy_bind_error(
                        "DPY-4010",
                        format!(
                            "a bind variable replacement value for placeholder \":{name}\" was not provided"
                        ),
                    ));
                };
                py_value_to_bind(input_size_var.bind(py))?
            };
            values.push(if is_return_bind {
                returning_output_bind(bind)
            } else {
                plsql_output_bind(bind)
            });
            continue;
        }

        let value = if has_all_bind_values {
            row_values[position].clone()
        } else {
            let value = row_values[input_index].clone();
            input_index += 1;
            value
        };
        let bind = if let Some(input_size_var) =
            positional_input_size_value(py, named_input_sizes, position)
        {
            if let Ok(var) = input_size_var.bind(py).extract::<PyRef<'_, ThinVar>>() {
                var.set_py_value(Some(value.clone().unbind()))?;
                var.to_bind_value(py)?
            } else {
                py_value_to_execute_bind(&value)?
            }
        } else {
            py_value_to_execute_bind(&value)?
        };
        values.push(bind);
    }
    Ok(values)
}

pub(crate) fn extract_positional_bind_values_with_input_sizes(
    py: Python<'_>,
    statement: &str,
    value: &Bound<'_, PyAny>,
    named_input_sizes: &[(String, Py<PyAny>)],
) -> PyResult<Vec<BindValue>> {
    if value.cast::<PyDict>().is_ok() || value.cast::<PyString>().is_ok() {
        return Err(raise_oracledb_driver_error(
            "ERR_WRONG_EXECUTE_PARAMETERS_TYPE",
        ));
    }
    let row_values = positional_bind_items(value)?;
    let names = unique_sql_bind_names(statement)?;
    let return_names = statement_return_bind_names(statement)?;
    let plsql_output_names = statement_plsql_output_bind_names(statement)?;
    let input_count = names
        .iter()
        .filter(|name| {
            !return_names
                .iter()
                .any(|return_name| bind_names_equal(return_name, name))
                && !plsql_output_names
                    .iter()
                    .any(|output_name| bind_names_equal(output_name, name))
        })
        .count();
    let has_all_bind_values = row_values.len() == names.len();
    let has_input_only_values = row_values.len() == input_count;
    if !has_all_bind_values && !has_input_only_values {
        return Err(dpy_bind_error(
            "DPY-4009",
            format!(
                "{input_count} positional bind values are required but {} were provided",
                row_values.len()
            ),
        ));
    }
    let mut input_index = 0;
    let mut values = Vec::with_capacity(names.len());
    for (position, name) in names.iter().enumerate() {
        let is_return_bind = return_names
            .iter()
            .any(|return_name| bind_names_equal(return_name, name));
        let is_plsql_output_bind = plsql_output_names
            .iter()
            .any(|output_name| bind_names_equal(output_name, name));
        if is_return_bind || is_plsql_output_bind {
            let value = if has_all_bind_values {
                py_value_to_bind(&row_values[position])?
            } else {
                let Some(input_size_var) =
                    positional_input_size_value(py, named_input_sizes, position)
                else {
                    return Err(dpy_bind_error(
                        "DPY-4010",
                        format!(
                            "a bind variable replacement value for placeholder \":{name}\" was not provided"
                        ),
                    ));
                };
                py_value_to_bind(input_size_var.bind(py))?
            };
            values.push(if is_return_bind {
                returning_output_bind(value)
            } else {
                plsql_output_bind(value)
            });
            continue;
        }
        let value = if has_all_bind_values {
            row_values[position].clone()
        } else {
            let value = row_values[input_index].clone();
            input_index += 1;
            value
        };
        let bind = if let Some(input_size_var) =
            positional_input_size_value(py, named_input_sizes, position)
        {
            if let Ok(var) = input_size_var.bind(py).extract::<PyRef<'_, ThinVar>>() {
                bind_recording_executemany_input_value(py, &var, &value)?
            } else {
                py_value_to_execute_bind(&value)?
            }
        } else {
            py_value_to_execute_bind(&value)?
        };
        values.push(bind);
    }
    Ok(values)
}

pub(crate) fn extract_named_bind_values(
    py: Python<'_>,
    statement: &str,
    parameters: Option<&Bound<'_, PyDict>>,
    named_input_sizes: &[(String, Py<PyAny>)],
    previous_bind_names: &[String],
    previous_bind_vars: &[Py<ThinVar>],
    record_input_size_values: bool,
) -> PyResult<Vec<BindValue>> {
    let names = unique_sql_bind_names(statement)?;
    let return_names = statement_return_bind_names(statement)?;
    let plsql_output_names = statement_plsql_output_bind_names(statement)?;
    if let Some(parameters) = parameters {
        for (key, _) in parameters.iter() {
            let key = key.extract::<String>()?;
            if !names.iter().any(|name| bind_name_matches_key(name, &key)) {
                return Err(dpy_bind_error(
                    "DPY-4008",
                    format!("no bind placeholder named \":{key}\" was found in the SQL text"),
                ));
            }
        }
    }
    names
        .iter()
        .map(|name| {
            let is_return_bind = return_names
                .iter()
                .any(|return_name| bind_names_equal(return_name, name));
            let is_plsql_output_bind = plsql_output_names
                .iter()
                .any(|output_name| bind_names_equal(output_name, name));
            if let Some(parameters) = parameters {
                if let Some(value) = get_named_bind_value(parameters, name)? {
                    let value = if let Some(input_size_var) =
                        named_input_size_value(py, named_input_sizes, name)
                    {
                        if let Ok(var) = input_size_var.bind(py).extract::<PyRef<'_, ThinVar>>() {
                            if record_input_size_values && !is_return_bind && !is_plsql_output_bind
                            {
                                bind_recording_executemany_input_value(py, &var, &value)?
                            } else {
                                var.set_py_value(Some(value.clone().unbind()))?;
                                var.to_bind_value(py)?
                            }
                        } else {
                            py_value_to_execute_bind(&value)?
                        }
                    } else {
                        py_value_to_execute_bind(&value)?
                    };
                    return Ok(if is_return_bind {
                        returning_output_bind(value)
                    } else if is_plsql_output_bind {
                        plsql_output_bind(value)
                    } else {
                        value
                    });
                }
            }
            if let Some(value) = named_input_size_value(py, named_input_sizes, name) {
                let value = py_value_to_bind(value.bind(py))?;
                return Ok(if is_return_bind {
                    returning_output_bind(value)
                } else if is_plsql_output_bind {
                    plsql_output_bind(value)
                } else {
                    value
                });
            }
            if is_return_bind || is_plsql_output_bind {
                if let Some(var) =
                    previous_bind_var_by_name(py, previous_bind_names, previous_bind_vars, name)
                {
                    let value = var.borrow(py).to_bind_value(py)?;
                    return Ok(if is_return_bind {
                        returning_output_bind(value)
                    } else {
                        output_only_bind(value)
                    });
                }
            }
            Err(dpy_bind_error(
                "DPY-4010",
                format!(
                    "a bind variable replacement value for placeholder \":{name}\" was not provided"
                ),
            ))
        })
        .collect()
}

pub(crate) fn extract_bind_rows(
    py: Python<'_>,
    statement: &str,
    parameters: &Bound<'_, PyAny>,
    named_input_sizes: &[(String, Py<PyAny>)],
) -> PyResult<Vec<Vec<BindValue>>> {
    if parameters.is_none() {
        return Ok(Vec::new());
    }
    if let Ok(num_iters) = parameters.extract::<usize>() {
        if unique_sql_bind_names(statement)?.is_empty() {
            return Ok(vec![Vec::new(); num_iters]);
        }
        if !named_input_sizes.is_empty() {
            let mut rows = Vec::with_capacity(num_iters);
            for _ in 0..num_iters {
                rows.push(extract_input_size_bind_values(
                    py,
                    statement,
                    named_input_sizes,
                )?);
            }
            return Ok(rows);
        }
    }
    if parameters.cast::<PyString>().is_ok() {
        return Err(raise_wrong_executemany_parameters_type());
    }
    clear_input_size_var_values(py, named_input_sizes)?;
    let rows = if let Ok(list) = parameters.cast::<PyList>() {
        list.iter().collect::<Vec<_>>()
    } else if let Ok(tuple) = parameters.cast::<PyTuple>() {
        tuple.iter().collect::<Vec<_>>()
    } else {
        return Err(raise_wrong_executemany_parameters_type());
    };
    let mut row_style = None;
    let mut bind_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let current_style = if row.cast::<PyDict>().is_ok() {
            ExecutemanyRowStyle::Named
        } else if row.cast::<PyString>().is_ok() {
            return Err(raise_wrong_executemany_parameters_type());
        } else if row.cast::<PyList>().is_ok()
            || row.cast::<PyTuple>().is_ok()
            || row.try_iter().is_ok()
        {
            ExecutemanyRowStyle::Positional
        } else {
            return Err(raise_wrong_executemany_parameters_type());
        };
        if row_style
            .replace(current_style)
            .is_some_and(|style| style != current_style)
        {
            return Err(raise_oracledb_driver_error(
                "ERR_MIXED_POSITIONAL_AND_NAMED_BINDS",
            ));
        }
        if current_style == ExecutemanyRowStyle::Named {
            let dict = row.cast::<PyDict>()?;
            bind_rows.push(extract_named_bind_values(
                py,
                statement,
                Some(dict),
                named_input_sizes,
                &[],
                &[],
                true,
            )?);
        } else {
            bind_rows.push(extract_positional_bind_values_with_input_sizes(
                py,
                statement,
                &row,
                named_input_sizes,
            )?);
        }
    }
    Ok(bind_rows)
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum ExecutemanyRowStyle {
    Named,
    Positional,
}

pub(crate) fn bind_value_is_output(value: &BindValue) -> bool {
    matches!(
        value,
        BindValue::Output { .. } | BindValue::ReturnOutput { .. } | BindValue::ObjectOutput { .. }
    )
}

// d49: migrate to oracledb (iterative PL/SQL executemany policy belongs on driver)
pub(crate) fn bind_rows_need_iterative_plsql(
    statement: &str,
    bind_rows: &[Vec<BindValue>],
) -> bool {
    statement_is_plsql(statement)
        && bind_rows
            .iter()
            .any(|row| row.iter().any(bind_value_is_output))
}

pub(crate) fn clear_input_size_var_values(
    py: Python<'_>,
    named_input_sizes: &[(String, Py<PyAny>)],
) -> PyResult<()> {
    for (_, input_size_var) in named_input_sizes {
        if let Ok(var) = input_size_var.bind(py).extract::<PyRef<'_, ThinVar>>() {
            var.clear_returned_values()?;
        }
    }
    Ok(())
}

pub(crate) fn bind_recording_executemany_input_value(
    py: Python<'_>,
    var: &PyRef<'_, ThinVar>,
    value: &Bound<'_, PyAny>,
) -> PyResult<BindValue> {
    var.set_bind_py_value(Some(value.clone().unbind()))?;
    let bind = var.to_bind_value(py)?;
    var.push_returned_py_value(value.clone().unbind())?;
    Ok(bind)
}

pub(crate) fn extract_input_size_bind_values(
    py: Python<'_>,
    statement: &str,
    named_input_sizes: &[(String, Py<PyAny>)],
) -> PyResult<Vec<BindValue>> {
    let names = unique_sql_bind_names(statement)?;
    let return_names = statement_return_bind_names(statement)?;
    let plsql_output_names = statement_plsql_output_bind_names(statement)?;
    names
        .iter()
        .enumerate()
        .map(|(position, name)| {
            let Some(input_size_var) =
                input_size_value_for_bind(py, named_input_sizes, name, position)
            else {
                return Err(dpy_bind_error(
                    "DPY-4010",
                    format!(
                        "a bind variable replacement value for placeholder \":{name}\" was not provided"
                    ),
                ));
            };
            let value = py_value_to_bind(input_size_var.bind(py))?;
            let is_return_bind = return_names
                .iter()
                .any(|return_name| bind_names_equal(return_name, name));
            let is_plsql_output_bind = plsql_output_names
                .iter()
                .any(|output_name| bind_names_equal(output_name, name));
            Ok(if is_return_bind {
                returning_output_bind(value)
            } else if is_plsql_output_bind {
                plsql_output_bind(value)
            } else {
                value
            })
        })
        .collect()
}

pub(crate) fn extract_bind_var_objects(
    py: Python<'_>,
    statement: &str,
    parameters: Option<&Bound<'_, PyAny>>,
    keyword_parameters: Option<&Bound<'_, PyAny>>,
    named_input_sizes: &[(String, Py<PyAny>)],
    previous_bind_names: &[String],
    previous_bind_vars: &[Py<ThinVar>],
) -> PyResult<Vec<Py<ThinVar>>> {
    let has_parameters = has_bind_payload(parameters)?;
    let has_keywords = has_bind_payload(keyword_parameters)?;
    if has_parameters && has_keywords {
        return Ok(Vec::new());
    }
    if let Some(value) = keyword_parameters.filter(|_| has_keywords) {
        return extract_named_bind_var_objects(
            py,
            statement,
            Some(value.cast::<PyDict>()?),
            named_input_sizes,
            previous_bind_names,
            previous_bind_vars,
        );
    }
    let Some(value) = parameters else {
        if !named_input_sizes.is_empty() {
            return extract_named_bind_var_objects(
                py,
                statement,
                None,
                named_input_sizes,
                previous_bind_names,
                previous_bind_vars,
            );
        }
        return Ok(Vec::new());
    };
    if !has_parameters {
        if !named_input_sizes.is_empty() {
            return extract_named_bind_var_objects(
                py,
                statement,
                None,
                named_input_sizes,
                previous_bind_names,
                previous_bind_vars,
            );
        }
        return Ok(Vec::new());
    }
    if let Ok(dict) = value.cast::<PyDict>() {
        return extract_named_bind_var_objects(
            py,
            statement,
            Some(dict),
            named_input_sizes,
            previous_bind_names,
            previous_bind_vars,
        );
    }
    extract_positional_bind_var_objects_for_execute(py, statement, value, named_input_sizes)
}

pub(crate) fn extract_positional_bind_var_objects_for_execute(
    py: Python<'_>,
    statement: &str,
    value: &Bound<'_, PyAny>,
    named_input_sizes: &[(String, Py<PyAny>)],
) -> PyResult<Vec<Py<ThinVar>>> {
    let row_values = positional_bind_items(value)?;
    let names = unique_sql_bind_names(statement)?;
    let return_names = statement_return_bind_names(statement)?;
    let plsql_output_names = statement_plsql_output_bind_names(statement)?;
    let input_count = names
        .iter()
        .filter(|name| {
            !return_names
                .iter()
                .any(|return_name| bind_names_equal(return_name, name))
                && !plsql_output_names
                    .iter()
                    .any(|output_name| bind_names_equal(output_name, name))
        })
        .count();
    let has_all_bind_values = row_values.len() == names.len();
    let has_input_only_values = row_values.len() == input_count;
    if !has_all_bind_values && !has_input_only_values {
        return Ok(Vec::new());
    }

    let mut input_index = 0;
    let mut values = Vec::with_capacity(names.len());
    for (position, name) in names.iter().enumerate() {
        let is_return_bind = return_names
            .iter()
            .any(|return_name| bind_names_equal(return_name, name));
        let is_plsql_output_bind = plsql_output_names
            .iter()
            .any(|output_name| bind_names_equal(output_name, name));
        if is_return_bind || is_plsql_output_bind {
            if has_all_bind_values {
                values.push(bind_var_from_value(py, &row_values[position])?);
            } else if let Some(input_size_var) =
                positional_input_size_value(py, named_input_sizes, position)
            {
                values.push(bind_var_from_value(py, input_size_var.bind(py))?);
            }
            continue;
        }

        let value = if has_all_bind_values {
            row_values[position].clone()
        } else {
            let value = row_values[input_index].clone();
            input_index += 1;
            value
        };
        if let Some(input_size_var) = positional_input_size_value(py, named_input_sizes, position) {
            if let Ok(var) = input_size_var.bind(py).extract::<PyRef<'_, ThinVar>>() {
                var.set_py_value(Some(value.clone().unbind()))?;
            }
            values.push(bind_var_from_value(py, input_size_var.bind(py))?);
        } else {
            values.push(bind_var_from_value(py, &value)?);
        }
    }
    Ok(values)
}

pub(crate) fn extract_named_bind_var_objects(
    py: Python<'_>,
    statement: &str,
    parameters: Option<&Bound<'_, PyDict>>,
    named_input_sizes: &[(String, Py<PyAny>)],
    previous_bind_names: &[String],
    previous_bind_vars: &[Py<ThinVar>],
) -> PyResult<Vec<Py<ThinVar>>> {
    let mut values = Vec::new();
    let return_names = statement_return_bind_names(statement)?;
    let plsql_output_names = statement_plsql_output_bind_names(statement)?;
    for name in unique_sql_bind_names(statement)? {
        let input_size_var = named_input_size_value(py, named_input_sizes, &name);
        let is_return_bind = return_names
            .iter()
            .any(|return_name| bind_names_equal(return_name, &name));
        let is_plsql_output_bind = plsql_output_names
            .iter()
            .any(|output_name| bind_names_equal(output_name, &name));
        if let Some(parameters) = parameters {
            if let Some(value) = get_named_bind_value(parameters, &name)? {
                if let Some(input_size_var) = input_size_var {
                    if let Ok(var) = input_size_var.bind(py).extract::<PyRef<'_, ThinVar>>() {
                        var.set_py_value(Some(value.clone().unbind()))?;
                    }
                    values.push(bind_var_from_value(py, input_size_var.bind(py))?);
                } else {
                    values.push(bind_var_from_value(py, &value)?);
                }
                continue;
            }
        }
        if let Some(input_size_var) = input_size_var {
            values.push(bind_var_from_value(py, input_size_var.bind(py))?);
        } else if is_return_bind || is_plsql_output_bind {
            if let Some(var) =
                previous_bind_var_by_name(py, previous_bind_names, previous_bind_vars, &name)
            {
                values.push(var);
            }
        }
    }
    Ok(values)
}

pub(crate) fn named_input_size_value(
    py: Python<'_>,
    named_input_sizes: &[(String, Py<PyAny>)],
    name: &str,
) -> Option<Py<PyAny>> {
    named_input_sizes
        .iter()
        .find(|(key, _)| bind_name_matches_key(name, key))
        .map(|(_, value)| value.clone_ref(py))
}

pub(crate) fn previous_bind_var_by_name(
    py: Python<'_>,
    previous_bind_names: &[String],
    previous_bind_vars: &[Py<ThinVar>],
    name: &str,
) -> Option<Py<ThinVar>> {
    previous_bind_names
        .iter()
        .position(|previous_name| bind_names_equal(previous_name, name))
        .and_then(|index| previous_bind_vars.get(index))
        .map(|value| value.clone_ref(py))
}

pub(crate) fn positional_input_size_value(
    py: Python<'_>,
    named_input_sizes: &[(String, Py<PyAny>)],
    zero_based_position: usize,
) -> Option<Py<PyAny>> {
    named_input_size_value(
        py,
        named_input_sizes,
        &(zero_based_position + 1).to_string(),
    )
}

pub(crate) fn input_size_value_for_bind(
    py: Python<'_>,
    named_input_sizes: &[(String, Py<PyAny>)],
    name: &str,
    zero_based_position: usize,
) -> Option<Py<PyAny>> {
    positional_input_size_value(py, named_input_sizes, zero_based_position)
        .or_else(|| named_input_size_value(py, named_input_sizes, name))
}

pub(crate) fn extract_executemany_bind_var_objects(
    py: Python<'_>,
    statement: &str,
    parameters: &Bound<'_, PyAny>,
    named_input_sizes: &[(String, Py<PyAny>)],
) -> PyResult<Vec<Py<ThinVar>>> {
    if !parameters.is_none() && parameters.extract::<usize>().is_err() {
        let first_row = if let Ok(list) = parameters.cast::<PyList>() {
            if list.is_empty() {
                None
            } else {
                Some(list.get_item(0)?)
            }
        } else if let Ok(tuple) = parameters.cast::<PyTuple>() {
            if tuple.is_empty() {
                None
            } else {
                Some(tuple.get_item(0)?)
            }
        } else {
            None
        };
        if let Some(row) = first_row {
            if let Ok(dict) = row.cast::<PyDict>() {
                return extract_named_bind_var_objects(
                    py,
                    statement,
                    Some(dict),
                    named_input_sizes,
                    &[],
                    &[],
                );
            }
            if row.cast::<PyString>().is_err()
                && (row.cast::<PyList>().is_ok()
                    || row.cast::<PyTuple>().is_ok()
                    || row.try_iter().is_ok())
            {
                return extract_positional_bind_var_objects_for_execute(
                    py,
                    statement,
                    &row,
                    named_input_sizes,
                );
            }
        }
    }
    unique_sql_bind_names(statement)?
        .iter()
        .enumerate()
        .map(|(position, name)| {
            if let Some(value) = input_size_value_for_bind(py, named_input_sizes, name, position) {
                bind_var_from_value(py, value.bind(py))
            } else {
                Py::new(py, ThinVar::from_py_value(None))
            }
        })
        .collect()
}

pub(crate) fn get_named_bind_value<'py>(
    parameters: &Bound<'py, PyDict>,
    name: &str,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    if let Some(value) = parameters.get_item(name)? {
        return Ok(Some(value));
    }
    if is_quoted_bind_name(name) {
        return Ok(None);
    }
    for (key, value) in parameters.iter() {
        let key = key.extract::<String>()?;
        // Reference strips a leading ':' from keys (impl/thin/var.pyx:88-94).
        let key = key.strip_prefix(':').unwrap_or(&key);
        if key.eq_ignore_ascii_case(name) {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

// d49: migrate to oracledb-protocol (sql.rs statement analytics)
pub(crate) fn unique_sql_bind_names(statement: &str) -> PyResult<Vec<String>> {
    sql::unique_bind_names(statement).map_err(sql_parse_error)
}

pub(crate) fn public_bind_name(name: &str) -> String {
    sql::public_bind_name(name)
}

pub(crate) fn statement_return_bind_names(statement: &str) -> PyResult<Vec<String>> {
    sql::returning_bind_names(statement).map_err(sql_parse_error)
}

// d49: migrate to oracledb-protocol (sql.rs statement analytics)
pub(crate) fn statement_plsql_assignment_bind_names(statement: &str) -> PyResult<Vec<String>> {
    sql::plsql_assignment_bind_names(statement).map_err(sql_parse_error)
}

pub(crate) fn statement_plsql_output_bind_names(statement: &str) -> PyResult<Vec<String>> {
    let mut names = statement_plsql_assignment_bind_names(statement)?;
    if !statement_is_plsql(statement) {
        return Ok(names);
    }
    let lower = statement.to_ascii_lowercase();
    let bytes = statement.as_bytes();
    let mut into_search_start = 0;
    while let Some(into_relative_pos) = lower[into_search_start..].find("into") {
        let into_pos = into_search_start + into_relative_pos;
        let mut bind_start = into_pos + "into".len();
        while bytes
            .get(bind_start)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            bind_start += 1;
        }
        if matches!(bytes.get(bind_start), Some(b':')) {
            let tail = &lower[bind_start..];
            let end = tail
                .find(" from ")
                .map(|relative| bind_start + relative)
                .or_else(|| tail.find(';').map(|relative| bind_start + relative))
                .unwrap_or(statement.len());
            for name in
                sql::scan_bind_names(&statement[bind_start..end]).map_err(sql_parse_error)?
            {
                if !names
                    .iter()
                    .any(|existing| bind_names_equal(existing, &name))
                {
                    names.push(name);
                }
            }
        }
        into_search_start = bind_start.saturating_add(1);
    }
    let mut search_start = 0;
    while let Some(returning_relative_pos) = lower[search_start..].find("returning") {
        let returning_pos = search_start + returning_relative_pos;
        let Some(into_relative_pos) = lower[returning_pos..].find("into") else {
            break;
        };
        let into_pos = returning_pos + into_relative_pos + "into".len();
        let end = statement[into_pos..]
            .find(';')
            .map(|relative| into_pos + relative)
            .unwrap_or(statement.len());
        for name in sql::scan_bind_names(&statement[into_pos..end]).map_err(sql_parse_error)? {
            if !names
                .iter()
                .any(|existing| bind_names_equal(existing, &name))
            {
                names.push(name);
            }
        }
        search_start = end;
    }
    Ok(names)
}

pub(crate) fn statement_is_plsql(statement: &str) -> bool {
    sql::statement_is_plsql(statement)
}

pub(crate) fn is_quoted_bind_name(name: &str) -> bool {
    sql::is_quoted_bind_name(name)
}

// d49: migrate to oracledb-protocol (sql.rs statement analytics)
pub(crate) fn validate_parse_bind_names(statement: &str) -> PyResult<()> {
    for name in unique_sql_bind_names(statement)? {
        if !is_quoted_bind_name(&name) && name.eq_ignore_ascii_case("ROWID") {
            return Err(ora_database_error(
                "ORA-01745: invalid host/bind variable name",
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_dml_returning_duplicate_binds(statement: &str) -> PyResult<()> {
    if statement_is_plsql(statement) {
        return Ok(());
    }
    let lower = statement.to_ascii_lowercase();
    let Some(returning_pos) = lower.find("returning") else {
        return Ok(());
    };
    let input_names = unique_sql_bind_names(&statement[..returning_pos])?;
    let return_names = statement_return_bind_names(statement)?;
    for return_name in return_names {
        if input_names
            .iter()
            .any(|input_name| bind_names_equal(input_name, &return_name))
        {
            return Err(raise_dml_returning_dup_bind(&public_bind_name(
                &return_name,
            )));
        }
    }
    Ok(())
}

pub(crate) fn bind_names_equal(left: &str, right: &str) -> bool {
    sql::bind_names_equal(left, right)
}

pub(crate) fn bind_name_matches_key(bind_name: &str, key: &str) -> bool {
    sql::bind_name_matches_key(bind_name, key)
}

pub(crate) fn sql_parse_error(err: sql::SqlError) -> PyErr {
    match err {
        sql::SqlError::MissingEndingSingleQuote => {
            raise_oracledb_driver_error("ERR_MISSING_ENDING_SINGLE_QUOTE")
        }
        sql::SqlError::MissingEndingDoubleQuote => {
            raise_oracledb_driver_error("ERR_MISSING_ENDING_DOUBLE_QUOTE")
        }
    }
}

pub(crate) fn is_public_cursor_value(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(value.hasattr("_impl")? && value.hasattr("connection")? && value.hasattr("arraysize")?)
}

pub(crate) fn validate_public_cursor_is_open(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if !is_public_cursor_value(value)? {
        return Ok(false);
    }
    let impl_obj = value.getattr("_impl")?;
    if impl_obj.is_none() {
        return Err(raise_oracledb_driver_error("ERR_CURSOR_NOT_OPEN"));
    }
    Ok(impl_obj.extract::<PyRef<'_, ThinCursorImpl>>().is_ok()
        || impl_obj.extract::<PyRef<'_, AsyncThinCursorImpl>>().is_ok())
}

pub(crate) fn validate_cursor_bind_value(
    executing_cursor: &Bound<'_, PyAny>,
    executing_connection: &Arc<Mutex<Option<RustConnection>>>,
    value: &Bound<'_, PyAny>,
) -> PyResult<()> {
    if std::ptr::eq(value.as_ptr(), executing_cursor.as_ptr()) {
        return Err(raise_oracledb_driver_error("ERR_SELF_BIND_NOT_SUPPORTED"));
    }
    if !is_public_cursor_value(value)? {
        return Ok(());
    }
    let impl_obj = value.getattr("_impl")?;
    if impl_obj.is_none() {
        return Err(raise_oracledb_driver_error("ERR_CURSOR_NOT_OPEN"));
    }
    if let Ok(cursor_impl) = impl_obj.extract::<PyRef<'_, ThinCursorImpl>>() {
        if !Arc::ptr_eq(&cursor_impl.connection, executing_connection) {
            return Err(raise_oracledb_driver_error("ERR_CURSOR_DIFF_CONNECTION"));
        }
    } else if let Ok(cursor_impl) = impl_obj.extract::<PyRef<'_, AsyncThinCursorImpl>>() {
        if !Arc::ptr_eq(&cursor_impl.inner.connection, executing_connection) {
            return Err(raise_oracledb_driver_error("ERR_CURSOR_DIFF_CONNECTION"));
        }
    }
    Ok(())
}

pub(crate) fn validate_cursor_bind_container(
    executing_cursor: &Bound<'_, PyAny>,
    executing_connection: &Arc<Mutex<Option<RustConnection>>>,
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_none() {
        return Ok(());
    }
    if let Ok(dict) = value.cast::<PyDict>() {
        for (_, item) in dict.iter() {
            validate_cursor_bind_value(executing_cursor, executing_connection, &item)?;
        }
        return Ok(());
    }
    if let Ok(tuple) = value.cast::<PyTuple>() {
        for item in tuple.iter() {
            validate_cursor_bind_value(executing_cursor, executing_connection, &item)?;
        }
        return Ok(());
    }
    if let Ok(list) = value.cast::<PyList>() {
        for item in list.iter() {
            validate_cursor_bind_value(executing_cursor, executing_connection, &item)?;
        }
        return Ok(());
    }
    validate_cursor_bind_value(executing_cursor, executing_connection, value)
}

pub(crate) fn validate_cursor_bind_parameters(
    executing_cursor: &Bound<'_, PyAny>,
    executing_connection: &Arc<Mutex<Option<RustConnection>>>,
    parameters: Option<&Bound<'_, PyAny>>,
    keyword_parameters: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    validate_cursor_bind_container(executing_cursor, executing_connection, parameters)?;
    validate_cursor_bind_container(executing_cursor, executing_connection, keyword_parameters)
}
