//! Holds the host to its promise about a live, correct peer (b/717, b/833): whatever such a peer does between the
//! host's read of `published` and its read of `taken`, the host never takes it for a faulty one. The peer is a forked
//! process on the real mapping that never sleeps, so its counters move as fast as they can while the host reads them.
#![cfg(not(any(miri, shm_model)))]

use std::convert::Infallible;
use std::os::fd::AsFd;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use mipyre_shm::adapter::doorbell::FutexDoorbell;
use mipyre_shm::adapter::mapped::MappedMemory;
use mipyre_shm::channel::{Channel, HostEnd, PeerEnd, Wait};
use mipyre_shm::plan::geometry::{Geometry, SlotBytes};
use mipyre_shm::plan::ring::{RingDepth, Seq};
use mipyre_shm::plan::spin_window::SpinWindow;
use mipyre_shm::protocol::doorbell::{AtomicU32, Doorbell, Parker};

/// The status a forked child ends with when its body panicked.
const CHILD_PANICKED: i32 = 101;

struct NoBell;

impl Doorbell for NoBell {
    type Outcome = ();
    type Error = Infallible;
    fn ring(&self, _: &AtomicU32) -> Result<(), Infallible> {
        Ok(())
    }
}

/// A parker the peer never reaches: its spin window never ends.
struct NeverParks;

impl Parker for NeverParks {
    type Error = Infallible;
    fn park(&self, _: &AtomicU32, _: u32, _: u64) -> Result<(), Infallible> {
        panic!("a peer whose spin never ends parked")
    }
}

/// Forks a child that runs `body` and ends with its status, as `tests/process.rs` does.
fn fork_child(body: impl FnOnce() -> i32) -> libc::pid_t {
    // SAFETY: the child runs `body` and `_exit`s; it never returns into the harness.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork: {}", std::io::Error::last_os_error());
    if pid == 0 {
        // SAFETY: `_exit` runs nothing of this process's.
        std::panic::set_hook(Box::new(|_| unsafe { libc::_exit(CHILD_PANICKED) }));
        let status = body();
        // SAFETY: as above.
        unsafe { libc::_exit(status) };
    }
    pid
}

fn exit_status(pid: libc::pid_t) -> i32 {
    let mut status = 0;
    // SAFETY: `status` is a writable int and `pid` is this process's child.
    assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
    assert!(libc::WIFEXITED(status), "the peer ended with wait status {status}");
    libc::WEXITSTATUS(status)
}

/// A live peer answering as fast as it can, while the host asks after its running request and takes its answers
/// on every turn: no read of the two counters is ever a fault, the running request is always one the host sent and
/// has not read, and every answer comes once, in order.
///
/// The limit is 60 s: nothing here waits on a clock (the peer spins, the host polls), and the run takes about a
/// second unoptimized; only a host or peer that stopped moving ends there.
#[test]
fn a_live_peer_moving_its_counters_as_fast_as_it_can_is_never_a_fault() {
    const REQUESTS: u32 = 2_000_000;
    let geometry = Geometry::new(RingDepth::new(8).unwrap(), SlotBytes::new(64).unwrap()).unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::Builder::new()
        .name("host and live peer".into())
        .spawn(move || {
            let (memory, shm) = MappedMemory::create(&geometry).unwrap();
            let mut host = HostEnd::new(Arc::new(Channel::new(memory)), FutexDoorbell);
            let peer = fork_child(|| {
                let memory = MappedMemory::attach(&geometry, shm.as_fd()).unwrap();
                let mut end = PeerEnd::new(Arc::new(Channel::new(memory)), NoBell, NeverParks);
                for _ in 0..REQUESTS {
                    let Ok(Wait::Request(request)) = end.wait_for_request(SpinWindow::until(u64::MAX), || 0, 1) else {
                        panic!("a peer that never parks waits until a request comes");
                    };
                    let bytes = request.bytes().unwrap().to_vec();
                    request
                        .answer(|w| w.put(&bytes).unwrap())
                        .unwrap_or_else(|never| match never {});
                }
                0
            });
            let (mut sent, mut read, mut running_seen) = (0u32, 0u32, 0u64);
            let mut buf = BytesMut::new();
            while read < REQUESTS {
                while sent < REQUESTS {
                    let Some(mut slot) = host.request_slot() else { break };
                    slot.write(|w| w.put(&sent.to_le_bytes())).unwrap();
                    slot.publish().unwrap_or_else(|never| match never {});
                    sent += 1;
                }
                let window = host.window();
                match host.running() {
                    Ok(Some(seq)) => {
                        assert!(
                            seq.since(window.read()) < window.in_flight(),
                            "{seq} outside {window:?}"
                        );
                        running_seen += 1;
                    }
                    Ok(None) => {}
                    Err(fault) => panic!("a correct live peer was taken for a faulty one: {fault:?} at {window:?}"),
                }
                let mut ready = host.answers().unwrap();
                while let Some(seq) = ready.take_next(&mut buf).unwrap() {
                    assert_eq!((seq, &buf[..]), (Seq::new(read), &read.to_le_bytes()[..]));
                    buf.clear();
                    read += 1;
                }
            }
            tx.send((exit_status(peer), running_seen)).unwrap();
        })
        .unwrap();
    let (status, running_seen) = rx
        .recv_timeout(Duration::from_secs(60))
        .expect("the host and peer finished within 60 s");
    assert_eq!(status, 0);
    assert!(
        running_seen > 0,
        "the host never saw the peer running a request, so no race was tried"
    );
}
