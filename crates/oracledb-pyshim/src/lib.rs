// The crate denies unsafe code everywhere except the single audited
// `arrow_capsule` module, which carries `#[allow(unsafe_code)]` for the Arrow
// C Data Interface PyCapsule export. See that module for SAFETY documentation.
#![deny(unsafe_code)]

use pyo3::prelude::*;

mod aq;
mod arrow_capsule;
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
mod soda;
mod subscr;
mod typehandler;
mod var;
mod vector;

pub(crate) use arrow_capsule::*;
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
pub(crate) use subscr::*;
pub(crate) use typehandler::*;
pub(crate) use var::*;
pub(crate) use vector::*;

#[pyfunction]
fn init_thin_impl(_package: &Bound<'_, PyAny>) -> PyResult<()> {
    Ok(())
}

#[pymodule]
fn oracledb_pyshim(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_thin_impl, m)?)?;
    m.add_function(wrap_pyfunction!(record_next_connect_args, m)?)?;
    m.add_function(wrap_pyfunction!(discard_pending_connect_args, m)?)?;
    m.add_function(wrap_pyfunction!(record_next_pool_args, m)?)?;
    m.add_function(wrap_pyfunction!(discard_pending_pool_args, m)?)?;
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
    m.add_class::<DataFrameImpl>()?;
    m.add_class::<ArrowArrayImpl>()?;
    m.add_class::<ArrowSchemaImpl>()?;
    m.add_class::<AsyncDataFrameBatchIter>()?;
    m.add_class::<ImmediateAwaitable>()?;
    m.add_class::<ThinSubscrImpl>()?;
    m.add_class::<aq::ThinQueueImpl>()?;
    m.add_class::<aq::AsyncThinQueueImpl>()?;
    m.add_class::<aq::ThinDeqOptionsImpl>()?;
    m.add_class::<aq::ThinEnqOptionsImpl>()?;
    m.add_class::<aq::ThinMsgPropsImpl>()?;
    m.add_class::<soda::ThinSodaDbImpl>()?;
    m.add_class::<soda::ThinSodaCollImpl>()?;
    m.add_class::<soda::ThinSodaDocImpl>()?;
    m.add_class::<soda::ThinSodaDocCursorImpl>()?;
    Ok(())
}
