//! A request's round trip through the channel allocates nothing: the binary's
//! global allocator counts every allocation, and the count does not move across many round trips, the host's sleep
//! and wake among them.
//!
//! One test in its own binary, so no other test allocates while it counts. The model build (`--cfg shm_model`) has no
//! heap memory to run it on.
#![cfg(not(shm_model))]
#![expect(clippy::unwrap_used, reason = "the round trip's helper fails the test by panicking")]

use std::alloc::System;
use std::convert::Infallible;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use bytes::BytesMut;
use mipyre_microbench::{allocations_counted, AllocationCounter, ALLOCATIONS};
use mipyre_shm::channel::{sleep_all, wake_all, Channel, HeapMemory, HostEnd, PeerEnd, Wait};
use mipyre_shm::plan::geometry::{Geometry, SlotBytes};
use mipyre_shm::plan::ring::RingDepth;
use mipyre_shm::plan::spin_window::SpinWindow;
use mipyre_shm::protocol::doorbell::{AtomicU32, Doorbell, Parker};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

struct Bell;

impl Doorbell for Bell {
    type Outcome = ();
    type Error = Infallible;
    fn ring(&self, _: &AtomicU32) -> Result<(), Infallible> {
        Ok(())
    }
}

struct Awake;

impl Parker for Awake {
    type Error = Infallible;
    fn park(&self, _: &AtomicU32, _: u32, _: u64) -> Result<(), Infallible> {
        Ok(())
    }
}

/// One request out and its answer back: the host writes and publishes, the peer reads it in place and answers, the
/// host checks for news, sleeps and wakes, and takes the answer into `dst`, which has the room.
fn round_trip(
    hosts: &mut [HostEnd<HeapMemory, Bell>; 1],
    peer: &mut PeerEnd<HeapMemory, Bell, Awake>,
    dst: &mut BytesMut,
    request: &[u8],
) {
    let host = &mut hosts[0];
    let mut slot = host.request_slot().unwrap();
    slot.write(|w| w.put(request)).unwrap();
    slot.publish().unwrap();
    let Ok(Wait::Request(taken)) = peer.wait_for_request(SpinWindow::until(0), || 0, 1) else {
        panic!("the request was published")
    };
    let answer = taken.bytes().unwrap();
    let len = answer.len();
    taken.answer(|w| w.put(&request[..len]).unwrap()).unwrap();
    assert!(hosts[0].has_news());
    sleep_all(hosts);
    wake_all(hosts);
    assert!(hosts[0].running().unwrap().is_none());
    dst.clear();
    let mut ready = hosts[0].answers().unwrap();
    assert!(ready.take_next(dst).unwrap().is_some());
    assert!(ready.take_next(dst).unwrap().is_none());
    assert_eq!(&dst[..], request);
}

#[test]
fn a_round_trip_through_the_channel_allocates_nothing() {
    assert!(allocations_counted(), "the counting allocator is the global one");
    let g = Geometry::new(RingDepth::new(4).unwrap(), SlotBytes::new(4096).unwrap()).unwrap();
    let channel = Arc::new(Channel::new(HeapMemory::new(&g)));
    let mut hosts = [HostEnd::new(Arc::clone(&channel), Bell)];
    let mut peer = PeerEnd::new(channel, Bell, Awake);
    let mut dst = BytesMut::with_capacity(g.max_message() as usize);
    let request = vec![7u8; g.max_message() as usize];

    let before = ALLOCATIONS.load(Relaxed);
    // Past the ring's depth many times over, so every slot is reused.
    for _ in 0..64 {
        round_trip(&mut hosts, &mut peer, &mut dst, &request);
    }
    assert_eq!(ALLOCATIONS.load(Relaxed) - before, 0);
}
