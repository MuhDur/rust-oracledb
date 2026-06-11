#![forbid(unsafe_code)]


use pyo3::prelude::*;

mod errors;
mod async_bridge;
mod hooks;
mod pyutil;
mod binds;
mod convert;
mod lob;
mod var;
mod typehandler;
mod dbobject;
mod metadata;
mod conn;
mod cursor;
mod async_cursor;
mod async_conn;
mod pool;

pub(crate) use errors::*;
pub(crate) use async_bridge::*;
pub(crate) use hooks::*;
pub(crate) use pyutil::*;
pub(crate) use binds::*;
pub(crate) use convert::*;
pub(crate) use lob::*;
pub(crate) use var::*;
pub(crate) use typehandler::*;
pub(crate) use dbobject::*;
pub(crate) use metadata::*;
pub(crate) use conn::*;
pub(crate) use cursor::*;
pub(crate) use async_cursor::*;
pub(crate) use async_conn::*;
pub(crate) use pool::*;

#[pyfunction]
fn init_thin_impl(_package: &Bound<'_, PyAny>) -> PyResult<()> {
    Ok(())
}

#[pymodule]
fn oracledb_pyshim(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_thin_impl, m)?)?;
    m.add_function(wrap_pyfunction!(record_next_connect_args, m)?)?;
    m.add_function(wrap_pyfunction!(discard_pending_connect_args, m)?)?;
    m.add_class::<ThinConnImpl>()?;
    m.add_class::<ThinLob>()?;
    m.add_class::<AsyncThinLob>()?;
    m.add_class::<DbObjectTypeImpl>()?;
    m.add_class::<DbObjectAttrImpl>()?;
    m.add_class::<DbObjectImpl>()?;
    m.add_class::<ThinCursorImpl>()?;
    m.add_class::<AsyncThinCursorImpl>()?;
    m.add_class::<FetchMetadataImpl>()?;
    m.add_class::<ExecutemanyManager>()?;
    m.add_class::<AsyncThinConnImpl>()?;
    m.add_class::<ThinPoolImpl>()?;
    m.add_class::<AsyncThinPoolImpl>()?;
    m.add_class::<EndUserSecurityContextImpl>()?;
    Ok(())
}
