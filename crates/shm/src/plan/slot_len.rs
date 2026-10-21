//! Reads a slot header: the length a slot's writer recorded for the message in it.

/// A slot header claims a message longer than the slot holds; no correct writer records one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SlotLengthError {
    #[error("the slot says its message is {len} bytes; a slot holds {max}")]
    PastSlot { len: u32, max: u32 },
}

/// Returns the message length `raw` the slot header holds, when it fits the `max` bytes a slot holds.
#[inline]
pub fn decode_len(raw: u32, max: u32) -> Result<u32, SlotLengthError> {
    if raw > max {
        return Err(SlotLengthError::PastSlot { len: raw, max });
    }
    Ok(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_length_up_to_the_slot_is_its_message() {
        assert_eq!(decode_len(4092, 4092), Ok(4092));
        assert_eq!(decode_len(0, 4092), Ok(0));
        assert_eq!(decode_len(u32::MAX, u32::MAX), Ok(u32::MAX));
    }

    #[test]
    fn a_length_past_the_slot_is_refused_with_both_values() {
        assert_eq!(decode_len(4093, 4092), Err(SlotLengthError::PastSlot { len: 4093, max: 4092 }));
        assert_eq!(decode_len(u32::MAX, 0), Err(SlotLengthError::PastSlot { len: u32::MAX, max: 0 }));
    }
}

#[cfg(kani)]
mod proofs {
    use super::*;

    /// A length accepted is the header's own value and fits the slot; one refused is reported with both values.
    #[kani::proof]
    fn a_length_accepted_fits_the_slot() {
        let (raw, max): (u32, u32) = (kani::any(), kani::any());
        match decode_len(raw, max) {
            Ok(len) => assert!(len == raw && len <= max),
            Err(SlotLengthError::PastSlot { len, max: said }) => assert!(len == raw && said == max && raw > max),
        }
    }
}
