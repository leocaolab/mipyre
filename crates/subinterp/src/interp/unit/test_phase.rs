//! Reads a unit's phase for the slot protocol's tests and its models.

use super::word::{UnitId, UnitPhase};
use super::SlotInner;

/// Returns the phase of `unit`, or `None` if it is no longer the slot's current unit.
pub(super) fn phase_of_unit<H: super::Interpreter>(slot: &SlotInner<H>, unit: UnitId) -> Option<UnitPhase> {
    let state = slot.load();
    (state.unit == unit).then_some(state.phase)
}
