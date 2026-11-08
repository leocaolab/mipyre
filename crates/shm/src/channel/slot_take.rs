//! Whatever bytes the peer left in an answer slot, the caller takes the answer whole or refuses it with the length the
//! slot claimed, and reads nothing past the slot: on heap memory, so it runs under Miri. The fuzz target that throws
//! any bytes at the slot is mipyre_shm_harness's `slot`.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::BytesMut;

use super::memory::sealed::Access;
use super::{Channel, HeapMemory, PeerFault, RequesterEnd};
use crate::plan::geometry::{Geometry, SlotBytes};
use crate::plan::ring::{RingDepth, Seq};
use crate::plan::slot_len::SlotLengthError;
use crate::protocol::doorbell::{AtomicU32, Doorbell};
use crate::sync::Ordering::Relaxed;

const SLOT_BYTES: usize = 64;

struct StubDoorbell;

impl Doorbell for StubDoorbell {
    type Outcome = ();
    type Error = Infallible;
    fn ring(&self, _: &AtomicU32) -> Result<(), Infallible> {
        Ok(())
    }
}

/// Lays `raw` over the answer slot of request 0 as the peer would (its first four bytes the length word, the rest
/// the message bytes, cut to the slot), publishes it, and takes it as the caller does.
fn take_a_slot_holding(raw: &[u8]) {
    let g = Geometry::new(RingDepth::new(2).unwrap(), SlotBytes::new(SLOT_BYTES).unwrap()).unwrap();
    let room = g.max_message();
    let channel = Arc::new(Channel::new(HeapMemory::new(&g)));
    let mut caller = RequesterEnd::new(Arc::clone(&channel), StubDoorbell);
    caller.request_slot().unwrap().publish().unwrap();

    let mut len = [0; 4];
    let (head, payload) = raw.split_at(raw.len().min(4));
    len[..head.len()].copy_from_slice(head);
    let len = u32::from_le_bytes(len);
    let mut image = vec![0; room as usize];
    let written = payload.len().min(image.len());
    image[..written].copy_from_slice(&payload[..written]);
    let memory = &channel.memory;
    let slot = memory.slot(g.response_offset(Seq::new(0)));
    slot.len_word().store(len, Relaxed);
    slot.copy_prefix_in(payload);
    memory.header().resp_head.store(1, Relaxed);

    let mut dst = BytesMut::from(&b"before"[..]);
    let taken = caller.responses().unwrap().take_next(&mut dst);
    if len <= room {
        assert_eq!(taken, Ok(Some(Seq::new(0))));
        assert_eq!(&dst[..6], b"before");
        assert_eq!(&dst[6..], &image[..len as usize]);
        return;
    }
    let fault = PeerFault::Length(SlotLengthError::PastSlot { len, max: room });
    assert_eq!(taken, Err(fault));
    assert_eq!(
        &dst[..],
        b"before",
        "a refused answer leaves the caller's bytes as they were"
    );
    assert!(caller.request_slot().is_none(), "a faulted channel takes no request");
    assert_eq!(caller.responses().err(), Some(fault), "the fault is kept");
}

#[test]
fn a_slot_claiming_its_room_is_taken_and_one_byte_more_is_refused() {
    take_a_slot_holding(&56u32.to_le_bytes());
    take_a_slot_holding(&57u32.to_le_bytes());
    take_a_slot_holding(&u32::MAX.to_le_bytes());
    take_a_slot_holding(&[]);
}
