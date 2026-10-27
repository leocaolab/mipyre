//! Gives the process lanes' settings their Python classes, `StartStrategy`, `FailurePolicy` and `LaneRecipe`, and
//! makes the server's `LaneSet` from Python's arguments.
//!
//! The values belong to the process lanes; each class here is converted from or to its value once, at this edge.

use std::time::Duration;

use pyo3::prelude::*;

use super::lane_set::PyLaneSet;
use mipyre_process::{create_server_lanes, FailurePolicy, LaneRecipe, StartStrategy};

/// Says how a worker gets the app.
#[pyclass(module = "mipyre.engine", name = "StartStrategy", eq, eq_int, frozen)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PyStartStrategy {
    Import,
    Fork,
}

impl From<StartStrategy> for PyStartStrategy {
    fn from(start: StartStrategy) -> Self {
        match start {
            StartStrategy::Import => PyStartStrategy::Import,
            StartStrategy::Fork => PyStartStrategy::Fork,
        }
    }
}

impl From<PyStartStrategy> for StartStrategy {
    fn from(start: PyStartStrategy) -> Self {
        match start {
            PyStartStrategy::Import => StartStrategy::Import,
            PyStartStrategy::Fork => StartStrategy::Fork,
        }
    }
}

#[pymethods]
impl PyStartStrategy {
    /// Parses `"import"` or `"fork"`.
    ///
    /// # Errors
    ///
    /// Raises `ValueError` naming any other text.
    #[staticmethod]
    fn parse(text: &str) -> PyResult<Self> {
        text.parse::<StartStrategy>()
            .map(Self::from)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    #[getter]
    fn name(&self) -> String {
        StartStrategy::from(*self).to_string()
    }
}

/// Says what a lost lane or a failed host does to the server run (`MIPYRE_PROCESS_HOST_FAILURE`).
#[pyclass(module = "mipyre.engine", name = "FailurePolicy", eq, eq_int, frozen, from_py_object)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PyFailurePolicy {
    Isolate,
    Exit,
}

impl From<FailurePolicy> for PyFailurePolicy {
    fn from(policy: FailurePolicy) -> Self {
        match policy {
            FailurePolicy::Isolate => PyFailurePolicy::Isolate,
            FailurePolicy::Exit => PyFailurePolicy::Exit,
        }
    }
}

impl From<PyFailurePolicy> for FailurePolicy {
    fn from(policy: PyFailurePolicy) -> Self {
        match policy {
            PyFailurePolicy::Isolate => FailurePolicy::Isolate,
            PyFailurePolicy::Exit => FailurePolicy::Exit,
        }
    }
}

#[pymethods]
impl PyFailurePolicy {
    /// Parses `"isolate"` or `"exit"`.
    ///
    /// # Errors
    ///
    /// Raises `ValueError` naming any other text.
    #[staticmethod]
    fn parse(text: &str) -> PyResult<Self> {
        text.parse::<FailurePolicy>()
            .map(Self::from)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    #[getter]
    fn name(&self) -> String {
        FailurePolicy::from(*self).to_string()
    }
}

/// One app's process lanes, their flags settled: what `LaneSet` and the template's
/// setup are made from. Python reads it; `mipyre._process_lane.server.start` builds the lanes.
#[pyclass(module = "mipyre.engine", name = "LaneRecipe", frozen)]
pub(crate) struct PyLaneRecipe(LaneRecipe);

impl From<LaneRecipe> for PyLaneRecipe {
    fn from(recipe: LaneRecipe) -> Self {
        PyLaneRecipe(recipe)
    }
}

#[pymethods]
impl PyLaneRecipe {
    #[getter]
    fn start(&self) -> PyStartStrategy {
        self.0.start.into()
    }

    #[getter]
    fn workers(&self) -> usize {
        self.0.workers
    }

    #[getter]
    fn timeout_ms(&self) -> u64 {
        self.0.timeout_ms
    }

    #[getter]
    fn queue_per_worker(&self) -> usize {
        self.0.queue_per_worker
    }

    #[getter]
    fn slot_bytes(&self) -> usize {
        self.0.slot_bytes
    }

    #[getter]
    fn worker_spin_us(&self) -> u64 {
        self.0.worker_spin_us
    }

    #[getter]
    fn host_failure(&self) -> PyFailurePolicy {
        self.0.host_failure.into()
    }
}

/// Makes the server's lanes.
///
/// # Errors
///
/// Raises `ValueError` saying which setting the lanes refuse.
#[pyfunction]
#[pyo3(signature = (
    workers, slot_bytes=256 * 1024, depth=8, timeout_ms=30_000, queue_per_worker=128,
    server_spin_us=20, on_failure=PyFailurePolicy::Isolate
))]
pub(crate) fn _create_lanes(
    workers: usize,
    slot_bytes: usize,
    depth: u32,
    timeout_ms: u64,
    queue_per_worker: usize,
    server_spin_us: u64,
    on_failure: PyFailurePolicy,
) -> PyResult<PyLaneSet> {
    create_server_lanes(
        workers,
        slot_bytes,
        depth,
        Duration::from_millis(timeout_ms),
        queue_per_worker,
        Duration::from_micros(server_spin_us),
        on_failure.into(),
    )
    .map(PyLaneSet::from)
    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_start_strategy_crosses_and_comes_back_as_itself() {
        for start in [StartStrategy::Import, StartStrategy::Fork] {
            assert_eq!(StartStrategy::from(PyStartStrategy::from(start)), start);
        }
    }

    #[test]
    fn every_failure_policy_crosses_and_comes_back_as_itself() {
        for policy in [FailurePolicy::Isolate, FailurePolicy::Exit] {
            assert_eq!(FailurePolicy::from(PyFailurePolicy::from(policy)), policy);
        }
    }
}
