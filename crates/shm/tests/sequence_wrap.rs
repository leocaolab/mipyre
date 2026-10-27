//! Runs the two ends across the wrap of the ring's u32 sequence, as a lane does after 2^32 requests: only
//! the ends' own code moves the sequence, so the test sends that many requests, each answered with its own number.
//!
//! It takes some 20 s optimized (minutes unoptimized), so it is the nightly run's, built only under
//! `--cfg shm_wrap_nightly`: `RUSTFLAGS="--cfg shm_wrap_nightly" cargo test -p mipyre_shm --release --test sequence_wrap`.
#![cfg(all(shm_wrap_nightly, not(any(miri, shm_model))))]

use std::convert::Infallible;
use std::sync::Arc;

use bytes::BytesMut;
use mipyre_shm::channel::{Channel, HeapMemory, HostEnd, PeerEnd, Wait};
use mipyre_shm::plan::geometry::{Geometry, SlotBytes};
use mipyre_shm::plan::ring::{RingDepth, Seq};
use mipyre_shm::plan::spin_window::SpinWindow;
use mipyre_shm::protocol::doorbell::{AtomicU32, Doorbell, Parker};

struct NoBell;

impl Doorbell for NoBell {
    type Outcome = ();
    type Error = Infallible;
    fn ring(&self, _: &AtomicU32) -> Result<(), Infallible> {
        Ok(())
    }
}

struct NoSleep;

impl Parker for NoSleep {
    type Error = Infallible;
    fn park(&self, _: &AtomicU32, _: u32, _: u64) -> Result<(), Infallible> {
        Ok(())
    }
}

/// Every request across the wrap is answered once, in order, with its own number; the requests in flight together
/// at the wrap never share a slot, which a depth that does not divide 2^32 would make them do.
#[test]
fn the_ends_carry_every_request_across_the_wrap_of_the_sequence() {
    const DEPTH: u32 = 8;
    let geometry = Geometry::new(RingDepth::new(DEPTH).unwrap(), SlotBytes::new(16).unwrap()).unwrap();
    let channel = Arc::new(Channel::new(HeapMemory::new(&geometry)));
    let mut host = HostEnd::new(Arc::clone(&channel), NoBell);
    let mut peer = PeerEnd::new(channel, NoBell, NoSleep);
    let total = (1u64 << 32) + 4 * u64::from(DEPTH);
    let mut buf = BytesMut::with_capacity(64);
    let (mut sent, mut read) = (0u64, 0u64);
    while read < total {
        while sent < total {
            let Some(mut slot) = host.request_slot() else { break };
            slot.write(|w| w.put(&sent.to_le_bytes())).unwrap();
            slot.publish().unwrap_or_else(|never| match never {});
            sent += 1;
        }
        while let Ok(Wait::Request(request)) = peer.wait_for_request(SpinWindow::until(0), || 0, 1) {
            let mut n = [0u8; 8];
            n.copy_from_slice(&request.bytes().unwrap());
            request
                .answer(|w| w.put(&n).unwrap())
                .unwrap_or_else(|never| match never {});
        }
        let mut ready = host.answers().unwrap();
        while let Some(seq) = ready.take_next(&mut buf).unwrap() {
            assert_eq!(seq, Seq::new(read as u32));
            assert_eq!(&buf[..], &read.to_le_bytes()[..], "request {read}");
            buf.clear();
            read += 1;
        }
    }
    assert_eq!(host.window().read(), Seq::new(total as u32));
}
