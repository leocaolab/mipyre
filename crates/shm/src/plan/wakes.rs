//! Decides whether the bytes a peer wrote into its host's wake pipe are ones a correct peer writes: one per answer it
//! publishes while the host said it sleeps, so never more than the answers the host read plus one per slot of the
//! ring (answers published and not yet read).

/// The peer wrote more wake bytes than answers it published: what it wrote, and the room the host had left.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the peer wrote {bytes} wake bytes where the answers it published leave room for {room}")]
pub struct WakeFloodError {
    pub bytes: u64,
    pub room: u64,
}

/// Takes `bytes` heard from the pipe out of `room` and returns the room left; bytes past it are the peer's fault.
#[inline]
pub fn spend_wakes(room: u64, bytes: u64) -> Result<u64, WakeFloodError> {
    room.checked_sub(bytes).ok_or(WakeFloodError { bytes, room })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_up_to_the_room_are_spent_and_one_past_it_is_the_peers_fault() {
        assert_eq!(spend_wakes(5, 5), Ok(0));
        assert_eq!(spend_wakes(5, 4), Ok(1));
        assert_eq!(spend_wakes(5, 6), Err(WakeFloodError { bytes: 6, room: 5 }));
        assert_eq!(spend_wakes(0, 1), Err(WakeFloodError { bytes: 1, room: 0 }));
    }
}
