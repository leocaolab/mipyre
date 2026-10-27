//! Reads a run's phase for the slot protocol's tests and its models.

use super::{run_of, RunId, UnitPhase, UnitState};
use crate::sync::Ordering;

/// Returns the phase of `run`, or `None` if it is no longer the slot's current run.
pub(super) fn phase_of_run<H>(slot: &UnitState<H>, run: RunId) -> Option<UnitPhase> {
    let word = slot.state.load(Ordering::Acquire);
    (run_of(word) == run).then(|| UnitPhase::from_word(word))
}
