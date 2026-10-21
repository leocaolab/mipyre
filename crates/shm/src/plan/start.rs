//! Reads a start report: what the peer of one start attempt says came of it.
//!
//! The host clears the report before each attempt; the peer of that attempt writes `state` once, and on a failure
//! first the length of the note it left (its traceback).

use super::slot_len::{decode_len, SlotLengthError};

/// No peer has reported on the attempt yet.
const NOT_REPORTED: u32 = 0;
const READY: u32 = 1;
const FAILED: u32 = 2;

/// What the peer says came of its start attempt, its note's length checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartClaim {
    Ready,
    /// It could not start, and left a note of `note_len` bytes.
    Failed { note_len: u32 },
}

/// A start report holds a value no peer writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StartReportError {
    #[error("the peer's start report says state {0}, which no peer writes")]
    UnknownState(u32),
    #[error("the peer's start report note: {0}")]
    NoteLength(#[source] SlotLengthError),
}

/// Returns the peer's claim from the report's `state` and `note_len`, or `None` before it reported; `max` is the
/// note area's size.
pub fn decode_start(state: u32, note_len: u32, max: u32) -> Result<Option<StartClaim>, StartReportError> {
    match state {
        NOT_REPORTED => Ok(None),
        READY => Ok(Some(StartClaim::Ready)),
        FAILED => {
            let note_len = decode_len(note_len, max).map_err(StartReportError::NoteLength)?;
            Ok(Some(StartClaim::Failed { note_len }))
        }
        unknown => Err(StartReportError::UnknownState(unknown)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOTE_BYTES: u32 = 8 << 10;

    #[test]
    fn a_report_not_written_yet_says_nothing() {
        assert_eq!(decode_start(NOT_REPORTED, 0, NOTE_BYTES), Ok(None));
    }

    #[test]
    fn a_ready_peer_is_ready() {
        assert_eq!(decode_start(READY, 0, NOTE_BYTES), Ok(Some(StartClaim::Ready)));
    }

    #[test]
    fn a_failed_peers_note_fills_at_most_the_note_area() {
        assert_eq!(
            decode_start(FAILED, NOTE_BYTES, NOTE_BYTES),
            Ok(Some(StartClaim::Failed { note_len: NOTE_BYTES }))
        );
        assert_eq!(
            decode_start(FAILED, NOTE_BYTES + 1, NOTE_BYTES),
            Err(StartReportError::NoteLength(SlotLengthError::PastSlot { len: NOTE_BYTES + 1, max: NOTE_BYTES }))
        );
    }

    #[test]
    fn a_state_no_peer_writes_is_refused_with_its_value() {
        assert_eq!(decode_start(FAILED + 1, 0, NOTE_BYTES), Err(StartReportError::UnknownState(FAILED + 1)));
        assert_eq!(decode_start(u32::MAX, 0, NOTE_BYTES), Err(StartReportError::UnknownState(u32::MAX)));
    }
}
