#![forbid(unsafe_code)]

use pyo3::prelude::*;

mod async_bridge;
mod async_conn;
mod async_cursor;
mod binds;
mod conn;
mod convert;
mod cursor;
mod dbobject;
mod errors;
mod hooks;
mod lob;
mod metadata;
mod pipeline;
mod pool;
mod pyutil;
mod typehandler;
mod var;

pub(crate) use async_bridge::*;
pub(crate) use async_conn::*;
pub(crate) use async_cursor::*;
pub(crate) use binds::*;
pub(crate) use conn::*;
pub(crate) use convert::*;
pub(crate) use cursor::*;
pub(crate) use dbobject::*;
pub(crate) use errors::*;
pub(crate) use hooks::*;
pub(crate) use lob::*;
pub(crate) use metadata::*;
pub(crate) use pipeline::*;
pub(crate) use pool::*;
pub(crate) use pyutil::*;
pub(crate) use typehandler::*;
pub(crate) use var::*;

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
    m.add_class::<PipelineOpResultShimImpl>()?;
    m.add_class::<EndUserSecurityContextImpl>()?;
    Ok(())
}
