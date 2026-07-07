//! Driver-native PL/SQL routine calls (GH#13, bead `a4-plsql-routine`).
//!
//! A [`RoutineCall`] builds and executes the anonymous PL/SQL block that invokes
//! a stored **procedure** or **function**, so callers get a typed
//! `call_routine(...)` surface instead of hand-writing `BEGIN ... END;` and
//! wiring OUT binds themselves. It is a thin ergonomic layer over the existing
//! execute + OUT-bind machinery ([`Connection::execute`],
//! [`crate::ExecuteOutcome::out_binds`]): positional arguments map to the
//! routine's formal parameters in order; IN arguments carry values, OUT
//! arguments register typed placeholders, and a function additionally has a
//! typed RETURN read back through [`RoutineOutcome`].
//!
//! # Scope
//!
//! v1 covers **IN**, **OUT**, and **function RETURN** binds — the surface the
//! downstream `plsql-mcp` consumer needs to stop hand-rolling `call_routine`
//! over `execute_raw`. **IN OUT** is intentionally not offered: the driver's
//! [`BindValue`] models a bind as *either* an input value *or* an output
//! placeholder, with no combined variant, so a faithful IN OUT bind needs a
//! protocol-level bind-model extension (tracked as a follow-up). The live
//! bind round-trip is exercised on the version matrix; the offline tests here
//! pin the generated block, the bind layout, and the OUT/return value mapping.

use std::borrow::Cow;

use oracledb_protocol::thin::{
    BindValue, QueryValue, CS_FORM_IMPLICIT, ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_VARCHAR,
};

use asupersync::Cx;

use crate::{block_on_io, BlockingConnection, Connection, ExecuteOutcome, FromSql, Params, Result};

/// NUMBER is a fixed 22-byte wire value (`ORA_TYPE_SIZE_NUMBER`, crate-private
/// upstream); an OUT NUMBER placeholder reserves exactly that.
const NUMBER_BUFFER_SIZE: u32 = 22;

/// The Oracle type an OUT (or function-RETURN) placeholder expects, so the
/// driver registers the bind with the right wire metadata before the call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OutType {
    /// `VARCHAR2` OUT, sized to `buffer_size` bytes (the server caps a bind at
    /// 32767).
    Varchar { buffer_size: u32 },
    /// `NUMBER` OUT.
    Number,
}

impl OutType {
    /// The wire placeholder for this OUT type. `is_return` selects the function
    /// RETURN form ([`BindValue::ReturnOutput`]) over a plain OUT
    /// ([`BindValue::Output`]).
    fn placeholder(self, is_return: bool) -> BindValue {
        let (ora_type_num, csfrm, buffer_size) = match self {
            OutType::Varchar { buffer_size } => {
                (ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, buffer_size)
            }
            OutType::Number => (ORA_TYPE_NUM_NUMBER, 0, NUMBER_BUFFER_SIZE),
        };
        if is_return {
            BindValue::ReturnOutput {
                ora_type_num,
                csfrm,
                buffer_size,
            }
        } else {
            BindValue::Output {
                ora_type_num,
                csfrm,
                buffer_size,
            }
        }
    }
}

/// One positional argument to a routine call.
#[derive(Clone, Debug)]
enum RoutineArg {
    /// An IN argument carrying a bound value.
    In(BindValue),
    /// An OUT argument registering a typed placeholder read back after the call.
    Out(OutType),
}

/// A driver-native call to a PL/SQL stored procedure or function (GH#13).
///
/// Build with [`RoutineCall::procedure`] or [`RoutineCall::function`], append
/// positional arguments with [`arg_in`](Self::arg_in) / [`arg_out`](Self::arg_out)
/// in the routine's parameter order, then run it with
/// [`Connection::call_routine`]. Example (a function `pkg.add(a, b)`):
///
/// ```no_run
/// # use oracledb::{Connection, RoutineCall, OutType};
/// # async fn demo(conn: &mut Connection, cx: &asupersync::Cx) -> oracledb::Result<()> {
/// let outcome = conn
///     .call_routine(
///         cx,
///         RoutineCall::function("pkg.add", OutType::Number)
///             .arg_in(2i64)
///             .arg_in(3i64),
///     )
///     .await?;
/// let sum: Option<i64> = outcome.returned_as()?;
/// # let _ = sum;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct RoutineCall {
    name: String,
    /// `Some` for a function (the RETURN type); `None` for a procedure.
    return_type: Option<OutType>,
    args: Vec<RoutineArg>,
}

impl RoutineCall {
    /// A call to the stored **procedure** `name` (e.g. `"pkg.do_thing"`). The
    /// name is emitted verbatim into the generated block, so schema/package
    /// qualification and quoting are the caller's to supply.
    pub fn procedure(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            return_type: None,
            args: Vec::new(),
        }
    }

    /// A call to the stored **function** `name` returning `returns`; the RETURN
    /// value is read back via [`RoutineOutcome::returned`].
    pub fn function(name: impl Into<String>, returns: OutType) -> Self {
        Self {
            name: name.into(),
            return_type: Some(returns),
            args: Vec::new(),
        }
    }

    /// Append an IN argument. Any [`ToSql`](crate::ToSql) value works.
    #[must_use]
    pub fn arg_in(mut self, value: impl crate::ToSql) -> Self {
        self.args.push(RoutineArg::In(value.to_sql()));
        self
    }

    /// Append an OUT argument of the given type, read back by declaration order
    /// via [`RoutineOutcome::out`].
    #[must_use]
    pub fn arg_out(mut self, out: OutType) -> Self {
        self.args.push(RoutineArg::Out(out));
        self
    }

    /// Whether this is a function call (has a RETURN).
    fn is_function(&self) -> bool {
        self.return_type.is_some()
    }

    /// Builds the anonymous PL/SQL block and the positional bind row. Placeholder
    /// numbering is 1-based; a function RETURN takes `:1` and the arguments
    /// follow (`:2`, `:3`, ...), so the bind row is `[return?, args...]` in wire
    /// order — matching the order OUT values come back in.
    fn build(&self) -> (String, Vec<BindValue>) {
        let mut binds = Vec::with_capacity(self.args.len() + usize::from(self.is_function()));
        let mut next: u32 = 1;
        let return_placeholder = self.return_type.map(|ret| {
            binds.push(ret.placeholder(true));
            let placeholder = next;
            next += 1;
            placeholder
        });
        let mut arg_placeholders = Vec::with_capacity(self.args.len());
        for arg in &self.args {
            match arg {
                RoutineArg::In(value) => binds.push(value.clone()),
                RoutineArg::Out(out) => binds.push(out.placeholder(false)),
            }
            arg_placeholders.push(next);
            next += 1;
        }
        let call_expr = if arg_placeholders.is_empty() {
            self.name.clone()
        } else {
            let list = arg_placeholders
                .iter()
                .map(|placeholder| format!(":{placeholder}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({list})", self.name)
        };
        let block = match return_placeholder {
            Some(placeholder) => format!("BEGIN :{placeholder} := {call_expr}; END;"),
            None => format!("BEGIN {call_expr}; END;"),
        };
        (block, binds)
    }
}

/// The OUT and function-RETURN values produced by a [`RoutineCall`].
///
/// Values are keyed by declaration order: [`returned`](Self::returned) is the
/// function RETURN (if any), and [`out(n)`](Self::out) is the `n`-th OUT
/// argument (0-based, counting only OUT arguments — IN arguments are skipped).
#[derive(Clone, Debug)]
pub struct RoutineOutcome {
    /// Output values in wire/bind order: `[return?, out-args...]`.
    outputs: Vec<Option<QueryValue>>,
    has_return: bool,
}

impl RoutineOutcome {
    fn from_outputs(outputs: Vec<Option<QueryValue>>, has_return: bool) -> Self {
        Self {
            outputs,
            has_return,
        }
    }

    fn from_execute(outcome: &ExecuteOutcome, has_return: bool) -> Self {
        let outputs = outcome
            .out_binds()
            .values()
            .iter()
            .map(|(_, value)| value.clone())
            .collect();
        Self::from_outputs(outputs, has_return)
    }

    /// The function RETURN value, or `None` for a procedure (or a NULL return).
    pub fn returned(&self) -> Option<&QueryValue> {
        if self.has_return {
            self.outputs.first().and_then(Option::as_ref)
        } else {
            None
        }
    }

    /// The `index`-th OUT argument's value (0-based over OUT arguments only), or
    /// `None` if out of range or NULL.
    pub fn out(&self, index: usize) -> Option<&QueryValue> {
        let offset = usize::from(self.has_return);
        self.outputs.get(index + offset).and_then(Option::as_ref)
    }

    /// The function RETURN converted to `T` (`Ok(None)` for a procedure or a
    /// NULL return).
    pub fn returned_as<T: FromSql>(&self) -> Result<Option<T>> {
        self.returned()
            .map(|value| T::from_sql(value).map_err(crate::Error::from))
            .transpose()
    }

    /// The `index`-th OUT argument converted to `T` (`Ok(None)` if absent/NULL).
    pub fn out_as<T: FromSql>(&self, index: usize) -> Result<Option<T>> {
        self.out(index)
            .map(|value| T::from_sql(value).map_err(crate::Error::from))
            .transpose()
    }
}

impl Connection {
    /// Call a PL/SQL stored procedure or function (GH#13). Builds the anonymous
    /// block, binds the arguments positionally, executes it, and returns the
    /// OUT / RETURN values in a [`RoutineOutcome`]. See [`RoutineCall`].
    pub async fn call_routine(&mut self, cx: &Cx, call: RoutineCall) -> Result<RoutineOutcome> {
        let has_return = call.is_function();
        let (block, binds) = call.build();
        let outcome = self
            .execute(cx, &block, Params::Positional(Cow::Owned(binds)))
            .await?;
        Ok(RoutineOutcome::from_execute(&outcome, has_return))
    }
}

impl BlockingConnection {
    /// Blocking wrapper for [`Connection::call_routine`].
    pub fn call_routine(connection: &mut Connection, call: RoutineCall) -> Result<RoutineOutcome> {
        block_on_io(|cx| async move { connection.call_routine(&cx, call).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn procedure_block_positional_in_args() {
        let (block, binds) = RoutineCall::procedure("pkg.do_thing")
            .arg_in(1i64)
            .arg_in("x")
            .build();
        assert_eq!(block, "BEGIN pkg.do_thing(:1, :2); END;");
        assert_eq!(binds.len(), 2);
        assert!(matches!(binds[0], BindValue::Number(_)));
        assert!(matches!(binds[1], BindValue::Text(_)));
    }

    #[test]
    fn procedure_no_args_omits_parens() {
        let (block, binds) = RoutineCall::procedure("housekeeping").build();
        assert_eq!(block, "BEGIN housekeeping; END;");
        assert!(binds.is_empty());
    }

    #[test]
    fn procedure_with_out_registers_output_placeholder() {
        let (block, binds) = RoutineCall::procedure("get_count")
            .arg_out(OutType::Number)
            .build();
        assert_eq!(block, "BEGIN get_count(:1); END;");
        assert_eq!(binds.len(), 1);
        assert!(matches!(
            binds[0],
            BindValue::Output {
                ora_type_num: ORA_TYPE_NUM_NUMBER,
                ..
            }
        ));
    }

    #[test]
    fn function_block_return_takes_first_placeholder() {
        let (block, binds) = RoutineCall::function("pkg.add", OutType::Number)
            .arg_in(2i64)
            .arg_in(3i64)
            .build();
        assert_eq!(block, "BEGIN :1 := pkg.add(:2, :3); END;");
        assert_eq!(binds.len(), 3);
        assert!(matches!(
            binds[0],
            BindValue::ReturnOutput {
                ora_type_num: ORA_TYPE_NUM_NUMBER,
                ..
            }
        ));
        assert!(matches!(binds[1], BindValue::Number(_)));
        assert!(matches!(binds[2], BindValue::Number(_)));
    }

    #[test]
    fn function_no_args_returns_bare_call() {
        let (block, binds) =
            RoutineCall::function("current_ts", OutType::Varchar { buffer_size: 64 }).build();
        assert_eq!(block, "BEGIN :1 := current_ts; END;");
        assert_eq!(binds.len(), 1);
        assert!(matches!(
            binds[0],
            BindValue::ReturnOutput {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                buffer_size: 64,
                ..
            }
        ));
    }

    #[test]
    fn out_varchar_uses_implicit_charset_form() {
        assert!(matches!(
            OutType::Varchar { buffer_size: 200 }.placeholder(false),
            BindValue::Output {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_IMPLICIT,
                buffer_size: 200,
            }
        ));
    }

    #[test]
    fn outcome_maps_return_then_out_args_by_declaration_order() {
        // Wire order for a function with two OUT args: [return, out0, out1].
        let outputs = vec![
            Some(QueryValue::Text("ret".into())),
            Some(QueryValue::Text("a".into())),
            Some(QueryValue::Text("b".into())),
        ];
        let outcome = RoutineOutcome::from_outputs(outputs, true);
        assert_eq!(outcome.returned(), Some(&QueryValue::Text("ret".into())));
        assert_eq!(outcome.out(0), Some(&QueryValue::Text("a".into())));
        assert_eq!(outcome.out(1), Some(&QueryValue::Text("b".into())));
        assert_eq!(outcome.out(2), None);
    }

    #[test]
    fn procedure_outcome_has_no_return_and_out_starts_at_zero() {
        let outputs = vec![
            Some(QueryValue::Text("first".into())),
            Some(QueryValue::Text("second".into())),
        ];
        let outcome = RoutineOutcome::from_outputs(outputs, false);
        assert_eq!(outcome.returned(), None);
        assert_eq!(outcome.out(0), Some(&QueryValue::Text("first".into())));
        assert_eq!(outcome.out(1), Some(&QueryValue::Text("second".into())));
    }
}
