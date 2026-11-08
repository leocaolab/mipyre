//! Encodes a unit slot's state in one `u64` and decides each of its transitions.
//!
//! Every `u64` decodes to exactly one state, so the atomic that holds it cannot hold an illegal
//! one. The transitions are pure; the slot's compare-and-swap loop applies them.

/// The units begun on a slot, counted, which an interruption must match to land.
///
/// It wraps after `2^UNIT_BITS` units, which is safe because an interrupter holds a target for
/// one request, far short of `2^54`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct UnitId(u64);

/// Whether a unit runs or ended, and what its interruption state is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum UnitPhase {
    /// `outstanding`: an interruption was set on the unit's thread and not taken back; it may
    /// have been raised since.
    Running { outstanding: bool },
    /// `interrupted`: an interruption was set during the unit, even one the worker took back.
    Ended { interrupted: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct UnitState {
    pub(super) unit: UnitId,
    /// Interrupters counted in and not yet left; the end waits for them.
    pub(super) in_flight: u8,
    pub(super) phase: UnitPhase,
}

/// Why an interrupter is not counted in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AdmitError {
    /// The unit it aims at ended or another one began.
    UnitOver,
    /// 255 interrupters are counted in already.
    Full,
}

/// One slot state as the atomic holds it: `[unit:54][in_flight:8][ended:1][flag:1]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SlotWord(u64);

const FLAG: u64 = 1;
const ENDED: u64 = 1 << 1;
const COUNT_SHIFT: u32 = 2;
const COUNT_BITS: u32 = 8;
const UNIT_SHIFT: u32 = COUNT_SHIFT + COUNT_BITS;
const UNIT_BITS: u32 = 64 - UNIT_SHIFT;
const UNIT_MASK: u64 = (1 << UNIT_BITS) - 1;

/// One interrupter in the count, as `fetch_sub` takes it off the word.
pub(super) const ONE_INTERRUPTER: u64 = 1 << COUNT_SHIFT;

impl UnitId {
    fn next(self) -> UnitId {
        UnitId((self.0 + 1) & UNIT_MASK)
    }
}

impl SlotWord {
    /// The state of a slot made before its first unit: the unit before the first has ended.
    pub(super) const NEW: SlotWord = SlotWord(ENDED);

    pub(super) fn from_bits(bits: u64) -> SlotWord {
        SlotWord(bits)
    }

    pub(super) fn bits(self) -> u64 {
        self.0
    }

    #[inline]
    pub(super) fn encode(state: UnitState) -> SlotWord {
        let (ended, flag) = match state.phase {
            UnitPhase::Running { outstanding } => (false, outstanding),
            UnitPhase::Ended { interrupted } => (true, interrupted),
        };
        SlotWord(
            (state.unit.0 << UNIT_SHIFT)
                | (u64::from(state.in_flight) << COUNT_SHIFT)
                | (u64::from(ended) * ENDED)
                | u64::from(flag),
        )
    }

    #[inline]
    pub(super) fn decode(self) -> UnitState {
        let flag = self.0 & FLAG != 0;
        UnitState {
            unit: UnitId(self.0 >> UNIT_SHIFT),
            in_flight: (self.0 >> COUNT_SHIFT) as u8,
            phase: if self.0 & ENDED == 0 {
                UnitPhase::Running { outstanding: flag }
            } else {
                UnitPhase::Ended { interrupted: flag }
            },
        }
    }
}

/// Begins the unit after the last one, keeping the count; `None` while the last one runs, which
/// only a leaked unit leaves so.
#[inline]
pub(super) fn begin(state: UnitState) -> Option<UnitState> {
    match state.phase {
        UnitPhase::Running { .. } => None,
        UnitPhase::Ended { .. } => Some(UnitState {
            unit: state.unit.next(),
            phase: UnitPhase::Running { outstanding: false },
            ..state
        }),
    }
}

/// Ends the running unit; `taken_back` says whether its worker took an interruption back during
/// it. `None` when it had ended already.
#[inline]
pub(super) fn end(state: UnitState, taken_back: bool) -> Option<UnitState> {
    match state.phase {
        UnitPhase::Running { outstanding } => Some(UnitState {
            phase: UnitPhase::Ended {
                interrupted: outstanding || taken_back,
            },
            ..state
        }),
        UnitPhase::Ended { .. } => None,
    }
}

/// Takes back an outstanding interruption; `None` when none is.
pub(super) fn take_back(state: UnitState) -> Option<UnitState> {
    (state.phase == UnitPhase::Running { outstanding: true }).then_some(UnitState {
        phase: UnitPhase::Running { outstanding: false },
        ..state
    })
}

/// Marks an interruption of `unit` as set; `None` unless `unit` is the one running.
pub(super) fn interrupt(state: UnitState, unit: UnitId) -> Option<UnitState> {
    (state.unit == unit && matches!(state.phase, UnitPhase::Running { .. })).then_some(UnitState {
        phase: UnitPhase::Running { outstanding: true },
        ..state
    })
}

/// Counts an interrupter of `unit` in, unless that unit is over or the count is full.
pub(super) fn admit(word: SlotWord, unit: UnitId) -> Result<SlotWord, AdmitError> {
    let state = word.decode();
    if state.unit != unit || matches!(state.phase, UnitPhase::Ended { .. }) {
        return Err(AdmitError::UnitOver);
    }
    let in_flight = state.in_flight.checked_add(1).ok_or(AdmitError::Full)?;
    Ok(SlotWord::encode(UnitState { in_flight, ..state }))
}

#[cfg(kani)]
mod proofs {
    use super::*;

    fn any_state() -> UnitState {
        let unit: u64 = kani::any();
        kani::assume(unit <= UNIT_MASK);
        UnitState {
            unit: UnitId(unit),
            in_flight: kani::any(),
            phase: if kani::any() {
                UnitPhase::Running {
                    outstanding: kani::any(),
                }
            } else {
                UnitPhase::Ended {
                    interrupted: kani::any(),
                }
            },
        }
    }

    #[kani::proof]
    fn every_state_encodes_to_a_word_that_decodes_back_to_it() {
        let state = any_state();
        assert_eq!(SlotWord::encode(state).decode(), state);
    }

    #[kani::proof]
    fn every_word_is_the_encoding_of_the_state_it_decodes_to() {
        let word = SlotWord(kani::any());
        assert_eq!(SlotWord::encode(word.decode()), word);
    }

    #[kani::proof]
    fn a_visit_leaving_changes_only_the_count() {
        let state = any_state();
        kani::assume(state.in_flight >= 1);
        let left = SlotWord(SlotWord::encode(state).0 - ONE_INTERRUPTER).decode();
        assert_eq!(
            left,
            UnitState {
                in_flight: state.in_flight - 1,
                ..state
            }
        );
    }

    #[kani::proof]
    fn admission_counts_in_only_a_running_unit_it_aims_at_and_never_past_full() {
        let state = any_state();
        let unit = UnitId(kani::any());
        match admit(SlotWord::encode(state), unit) {
            Ok(word) => {
                assert!(matches!(state.phase, UnitPhase::Running { .. }) && state.unit == unit);
                assert_eq!(
                    word.decode(),
                    UnitState {
                        in_flight: state.in_flight + 1,
                        ..state
                    }
                );
            }
            Err(AdmitError::UnitOver) => {
                assert!(state.unit != unit || matches!(state.phase, UnitPhase::Ended { .. }));
            }
            Err(AdmitError::Full) => assert_eq!(state.in_flight, u8::MAX),
        }
    }

    fn keeps_unit_and_count(state: UnitState, next: Option<UnitState>) {
        if let Some(next) = next {
            assert_eq!((next.unit, next.in_flight), (state.unit, state.in_flight));
        }
    }

    #[kani::proof]
    fn no_transition_touches_the_count_and_only_begin_moves_the_unit() {
        let state = any_state();
        keeps_unit_and_count(state, end(state, kani::any()));
        keeps_unit_and_count(state, take_back(state));
        keeps_unit_and_count(state, interrupt(state, UnitId(kani::any())));
        if let Some(next) = begin(state) {
            assert_eq!(next.in_flight, state.in_flight);
            assert_eq!(next.unit, state.unit.next());
            assert_ne!(next.unit, state.unit);
        }
    }

    #[kani::proof]
    fn a_unit_begins_only_after_the_last_ended_and_ends_once() {
        let state = any_state();
        let taken_back: bool = kani::any();
        assert_eq!(begin(state).is_some(), matches!(state.phase, UnitPhase::Ended { .. }));
        if let Some(ended) = end(state, taken_back) {
            let UnitPhase::Running { outstanding } = state.phase else {
                panic!("only a running unit ends")
            };
            assert_eq!(
                ended.phase,
                UnitPhase::Ended {
                    interrupted: outstanding || taken_back
                }
            );
            assert!(end(ended, kani::any()).is_none());
        }
    }

    #[kani::proof]
    fn an_interruption_marks_only_its_running_unit_and_a_take_back_clears_it() {
        let state = any_state();
        let unit = UnitId(kani::any());
        match interrupt(state, unit) {
            Some(marked) => {
                assert!(state.unit == unit && matches!(state.phase, UnitPhase::Running { .. }));
                assert_eq!(marked.phase, UnitPhase::Running { outstanding: true });
                assert_eq!(
                    take_back(marked).map(|s| s.phase),
                    Some(UnitPhase::Running { outstanding: false })
                );
            }
            None => assert!(state.unit != unit || matches!(state.phase, UnitPhase::Ended { .. })),
        }
    }
}
