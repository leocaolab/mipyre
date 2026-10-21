//! Serves HTTP: [`plan`] makes the decisions and touches nothing, and [`io`] does the IO.
//!
//! The package has no pyo3 and no `#[global_allocator]`, because binding it to Python is the engine's job.

pub mod io;
pub mod plan;
mod sync;
