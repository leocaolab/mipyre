//! The channel against a reference model (b/739's ring, design `shm-channel.md` §4.2): one sequence of operations,
//! the peer's corrupting writes among them, goes to both ends of a channel on heap memory and to a model a reader can
//! check by eye, and every step's outcome must agree.
//!
//! The model keeps the host's requests in flight in a `VecDeque`, believes a counter the peer wrote only when it is
//! one of those requests (or `sent`), and reads every slot from a plain copy of what was last written there. It never
//! does the ring's arithmetic; that is the Kani proofs' job in `plan/ring.rs`.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;

use bytes::BytesMut;
use proptest::prelude::*;

use super::memory::sealed::Access;
use super::{
    sleep_all, wake_all, Channel, DeadPeerAnswer, DeadPeerError, HeapMemory, HostEnd, MessageView, PeerEnd, PeerFault,
    SleepVerdict, Wait,
};
use crate::plan::geometry::{Geometry, SlotBytes};
use crate::plan::ring::{PeerCounterError, RingDepth, Seq, Window};
use crate::plan::slot_len::SlotLengthError;
use crate::plan::spin_window::SpinWindow;
use crate::protocol::doorbell::{AtomicU32, Doorbell, Parker};
use crate::sync::Ordering::Relaxed;

const SLOT_BYTES: usize = 64;
/// What a slot of `SLOT_BYTES` holds after its 8-byte header.
const ROOM: u32 = 56;

/// One thing an end does, or the peer writes where no correct peer would.
#[derive(Clone, Debug)]
enum Op {
    /// The host sends a request of this many bytes (past the slot's room it is refused, and sent empty).
    Send(u32),
    /// The peer takes the next request and does not answer it yet.
    PeerTake,
    /// The peer answers the request it took, or else takes the next one and answers it, with this many bytes.
    PeerAnswer(u32),
    /// The host takes every answer it believes is ready.
    Collect,
    /// The host asks which request the peer is running.
    Running,
    /// The host says it is about to sleep and looks once more.
    HostSleeps,
    HostWakes,
    /// The peer exits; the host takes over from it and a new peer starts where the counters were left.
    PeerDies,
    Corrupt(Field, Scribble),
}

/// A word the peer can reach.
#[derive(Clone, Copy, Debug)]
enum Field {
    Taken,
    RespHead,
    /// The length word of the answer slot the host reads next.
    Len,
}

/// What the peer writes into a field: near what it holds, where the believable values are, or anything.
#[derive(Clone, Copy, Debug)]
enum Scribble {
    Near(i8),
    Any(u32),
}

/// An operation of a correct host and a correct peer.
fn honest_op() -> impl Strategy<Value = Op> {
    let len = 0..=ROOM + 4;
    prop_oneof![
        4 => len.clone().prop_map(Op::Send),
        2 => Just(Op::PeerTake),
        4 => len.prop_map(Op::PeerAnswer),
        3 => Just(Op::Collect),
        1 => Just(Op::Running),
        1 => Just(Op::HostSleeps),
        1 => Just(Op::HostWakes),
        1 => Just(Op::PeerDies),
    ]
}

/// An operation of either end, or a write of a peer gone wrong.
fn op() -> impl Strategy<Value = Op> {
    let field = prop_oneof![Just(Field::Taken), Just(Field::RespHead), Just(Field::Len)];
    let scribble = prop_oneof![3 => (-3i8..=3).prop_map(Scribble::Near), 1 => any::<u32>().prop_map(Scribble::Any)];
    prop_oneof![8 => honest_op(), 1 => (field, scribble).prop_map(|(f, v)| Op::Corrupt(f, v))]
}

/// The bytes the host writes for request `seq`, or the peer for its answer.
fn message(seq: u32, len: u32, answer: bool) -> Vec<u8> {
    let tag = if answer { 0xA5 } else { 0x00 };
    (0..len).map(|i| (seq as u8).wrapping_add(i as u8) ^ tag).collect()
}

/// A doorbell that only says it rang.
struct Bell;

impl Doorbell for Bell {
    type Outcome = ();
    type Error = Infallible;
    fn ring(&self, _: &AtomicU32) -> Result<(), Infallible> {
        Ok(())
    }
}

/// A parker whose every sleep ends at once.
struct Awake;

impl Parker for Awake {
    type Error = Infallible;
    fn park(&self, _: &AtomicU32, _: u32, _: u64) -> Result<(), Infallible> {
        Ok(())
    }
}

type Host = HostEnd<HeapMemory, Bell>;
type Peer = PeerEnd<HeapMemory, Bell, Awake>;

/// What was last written into one slot: its length word and its bytes.
#[derive(Clone)]
struct SlotCopy {
    len: u32,
    bytes: Vec<u8>,
}

impl SlotCopy {
    fn message(&self) -> Result<Vec<u8>, SlotLengthError> {
        if self.len > ROOM {
            return Err(SlotLengthError::PastSlot {
                len: self.len,
                max: ROOM,
            });
        }
        Ok(self.bytes[..self.len as usize].to_vec())
    }

    /// Writes a message as a slot writer does: one that does not fit leaves the bytes and records 0.
    fn write(&mut self, message: &[u8]) {
        self.len = 0;
        if message.len() <= ROOM as usize {
            self.bytes[..message.len()].copy_from_slice(message);
            self.len = message.len() as u32;
        }
    }
}

/// What the model says a take-over hands the host: each answer the dead peer published, and the request it died
/// running.
struct TakeOver {
    answered: Vec<(u32, Result<Vec<u8>, SlotLengthError>)>,
    running: Option<u32>,
}

/// The reference model.
struct RingModel {
    depth: u32,
    /// The requests the host sent and has not read the answers of, oldest first.
    flight: VecDeque<u32>,
    sent: u32,
    fault: Option<PeerFault>,
    host_sleeping: bool,
    /// The next request the peer takes, and whether it took it already.
    cursor: u32,
    running: bool,
    taken: u32,
    resp_head: u32,
    requests: Vec<SlotCopy>,
    answers: Vec<SlotCopy>,
}

impl RingModel {
    fn new(depth: u32) -> Self {
        let empty = SlotCopy {
            len: 0,
            bytes: vec![0; ROOM as usize],
        };
        RingModel {
            depth,
            flight: VecDeque::new(),
            sent: 0,
            fault: None,
            host_sleeping: false,
            cursor: 0,
            running: false,
            taken: 0,
            resp_head: 0,
            requests: vec![empty.clone(); depth as usize],
            answers: vec![empty; depth as usize],
        }
    }

    fn read(&self) -> u32 {
        self.flight.front().copied().unwrap_or(self.sent)
    }

    fn slot(&self, seq: u32) -> usize {
        (seq % self.depth) as usize
    }

    /// The requests in flight and then `sent`: every value of `published` a live peer can have written.
    fn believable(&self) -> Vec<u32> {
        self.flight.iter().copied().chain([self.sent]).collect()
    }

    /// Believes `resp_head` when it is one of the believable values.
    fn check_published(&self) -> Result<u32, PeerCounterError> {
        let published = self.resp_head;
        if !self.believable().contains(&published) {
            return Err(PeerCounterError::AnswersPastSent {
                read: Seq::new(self.read()),
                sent: Seq::new(self.sent),
                published: Seq::new(published),
            });
        }
        Ok(published)
    }

    /// Believes `taken` when it lies from `published` up to `sent`; returns the believable values from `published` on.
    fn check_taken(&self, published: u32) -> Result<Vec<u32>, PeerCounterError> {
        let from_published: Vec<u32> = self.believable().into_iter().skip_while(|&s| s != published).collect();
        if !from_published.contains(&self.taken) {
            return Err(self.taken_out_of_range(published));
        }
        Ok(from_published)
    }

    fn taken_out_of_range(&self, published: u32) -> PeerCounterError {
        PeerCounterError::TakenOutOfRange {
            read: Seq::new(self.read()),
            sent: Seq::new(self.sent),
            published: Seq::new(published),
            taken: Seq::new(self.taken),
        }
    }

    fn keep(&mut self, fault: PeerFault) -> PeerFault {
        *self.fault.insert(fault)
    }

    /// Looks at `resp_head` as the host does before it reads anything.
    fn observe(&mut self) -> Result<u32, PeerFault> {
        if let Some(fault) = self.fault {
            return Err(fault);
        }
        self.check_published().map_err(|e| self.keep(PeerFault::Counter(e)))
    }

    fn collect(&mut self) -> Result<Vec<(u32, Vec<u8>)>, PeerFault> {
        let published = self.observe()?;
        let mut out = Vec::new();
        while let Some(&seq) = self.flight.front().filter(|&&s| s != published) {
            match self.answers[self.slot(seq)].message() {
                Ok(bytes) => out.push((seq, bytes)),
                Err(e) => return Err(self.keep(PeerFault::Length(e))),
            }
            self.flight.pop_front();
        }
        Ok(out)
    }

    fn running_request(&mut self) -> Result<Option<u32>, PeerFault> {
        let published = self.observe()?;
        self.check_taken(published)
            .map_err(|e| self.keep(PeerFault::Counter(e)))?;
        Ok((self.taken != published).then_some(published))
    }

    /// Moves `read` and both counters to `next`, as a take-over or an abandon leaves them, for a new peer.
    fn restart_at(&mut self, next: u32) {
        while self.flight.front().is_some_and(|&s| s != next) {
            self.flight.pop_front();
        }
        self.taken = next;
        self.resp_head = next;
        self.fault = None;
        self.cursor = next;
        self.running = false;
    }

    /// Takes over from a dead peer as the host does, or gives up every request when its counters are past belief.
    fn take_over(&mut self) -> Result<TakeOver, PeerCounterError> {
        let checked = self.check_published().and_then(|published| {
            let from_published = self.check_taken(published)?;
            // A dead peer stands still: it took at most the one request after the last it answered.
            let running = match from_published.iter().position(|&s| s == self.taken) {
                Some(0) => None,
                Some(1) => Some(published),
                _ => return Err(self.taken_out_of_range(published)),
            };
            Ok((published, running))
        });
        let (published, running) = match checked {
            Ok(split) => split,
            Err(cause) => {
                self.restart_at(self.sent);
                return Err(cause);
            }
        };
        let answered = self
            .flight
            .iter()
            .take_while(|&&s| s != published)
            .map(|&s| (s, self.answers[self.slot(s)].message()))
            .collect();
        // b/925 (the user's ruling C): the requests the dead peer never took are given up with the one it ran; the
        // next peer starts after every request sent.
        self.restart_at(self.sent);
        Ok(TakeOver { answered, running })
    }
}

/// Both ends of one channel, run by the test, beside the model; and, for the property that a faulted channel leaves
/// its neighbours alone, a second host end whose peer wrote nonsense before the first step.
struct Harness {
    model: RingModel,
    /// `hosts[0]` is the end the model follows; `hosts[1]`, when there, is the faulted neighbour.
    hosts: Vec<Host>,
    peer: Peer,
}

impl Harness {
    fn new(depth: u32, faulted_neighbour: bool) -> Self {
        let g = Geometry::new(RingDepth::new(depth).unwrap(), SlotBytes::new(SLOT_BYTES).unwrap()).unwrap();
        assert_eq!(g.max_message(), ROOM);
        let channel = Arc::new(Channel::new(HeapMemory::new(&g)));
        let mut hosts = vec![HostEnd::new(Arc::clone(&channel), Bell)];
        if faulted_neighbour {
            let neighbour = Arc::new(Channel::new(HeapMemory::new(&g)));
            neighbour.memory.header().resp_head.store(u32::MAX, Relaxed);
            hosts.push(HostEnd::new(neighbour, Bell));
        }
        Harness {
            model: RingModel::new(depth),
            hosts,
            peer: PeerEnd::new(channel, Bell, Awake),
        }
    }

    fn channel(&self) -> &Channel<HeapMemory> {
        self.hosts[0].channel()
    }

    /// Runs one operation on the channel and on the model; `Ok(Some(sent))` for a `Send`, saying whether it was.
    fn step(&mut self, op: &Op) -> Result<Option<bool>, TestCaseError> {
        let mut sent = None;
        match *op {
            Op::Send(len) => sent = Some(self.send(len)?),
            Op::PeerTake => self.peer_take()?,
            Op::PeerAnswer(len) => self.peer_answer(len)?,
            Op::Collect => {
                let real = take_all(&mut self.hosts[0]);
                prop_assert_eq!(real, self.model.collect());
            }
            Op::Running => {
                let real = self.hosts[0].running().map(|r| r.map(Seq::get));
                prop_assert_eq!(real, self.model.running_request());
            }
            Op::HostSleeps => {
                let verdict = sleep_all(&mut self.hosts);
                self.model.host_sleeping = true;
                let found = self.model.fault.is_none()
                    && self
                        .model
                        .observe()
                        .map_or(true, |published| published != self.model.read());
                if self.hosts.len() == 1 {
                    let want = if found {
                        SleepVerdict::Collect
                    } else {
                        SleepVerdict::Park
                    };
                    prop_assert_eq!(verdict, want);
                }
            }
            Op::HostWakes => {
                wake_all(&mut self.hosts);
                self.model.host_sleeping = false;
            }
            Op::PeerDies => self.peer_dies()?,
            Op::Corrupt(field, value) => self.corrupt(field, value),
        }
        self.check_invariants()?;
        Ok(sent)
    }

    fn send(&mut self, len: u32) -> Result<bool, TestCaseError> {
        let m = &mut self.model;
        let has_room = m.fault.is_none() && (m.flight.len() as u32) < m.depth;
        let Some(mut slot) = self.hosts[0].request_slot() else {
            prop_assert!(!has_room, "no request slot, though the model has room");
            return Ok(false);
        };
        prop_assert!(has_room, "a request slot, though the model has none");
        prop_assert_eq!(slot.seq().get(), m.sent);
        let bytes = message(m.sent, len, false);
        let put = slot.write(|w| w.put(&bytes));
        prop_assert_eq!(put.is_ok(), len <= ROOM);
        prop_assert_eq!(slot.publish(), Ok(None), "the peer never sleeps here, so nothing rings");
        let at = m.slot(m.sent);
        m.requests[at].write(&bytes);
        m.flight.push_back(m.sent);
        m.sent = m.sent.wrapping_add(1);
        Ok(true)
    }

    /// Checks a request the peer was handed against the model, which marks it taken.
    fn handed(
        model: &mut RingModel,
        seq: Seq,
        bytes: Result<MessageView<'_>, SlotLengthError>,
    ) -> Result<(), TestCaseError> {
        prop_assert_eq!(seq.get(), model.cursor);
        prop_assert_eq!(
            bytes.map(|view| view.to_vec()),
            model.requests[model.slot(model.cursor)].message()
        );
        model.taken = model.cursor.wrapping_add(1);
        model.running = true;
        Ok(())
    }

    fn peer_take(&mut self) -> Result<(), TestCaseError> {
        let available = self.model.cursor != self.model.sent;
        let model = &mut self.model;
        match self.peer.wait_for_request(SpinWindow::until(0), || 0, 1) {
            Ok(Wait::Request(request)) => {
                prop_assert!(available, "a request handed out, though none was sent");
                Self::handed(model, request.seq(), request.bytes())?;
            }
            Ok(Wait::SliceOver) => prop_assert!(!available, "no request handed out, though one was sent"),
            Err(never) => match never {},
        }
        Ok(())
    }

    fn peer_answer(&mut self, len: u32) -> Result<(), TestCaseError> {
        let available = self.model.running || self.model.cursor != self.model.sent;
        let model = &mut self.model;
        let request = match self.peer.wait_for_request(SpinWindow::until(0), || 0, 1) {
            Ok(Wait::Request(request)) => request,
            Ok(Wait::SliceOver) => {
                prop_assert!(!available, "no request handed out, though one was sent");
                return Ok(());
            }
            Err(never) => match never {},
        };
        prop_assert!(available, "a request handed out, though none was sent");
        Self::handed(model, request.seq(), request.bytes())?;
        let bytes = message(model.cursor, len, true);
        let mut put = None;
        let rung = request.answer(|w| put = Some(w.put(&bytes)));
        prop_assert_eq!(put.map(|p| p.is_ok()), Some(len <= ROOM));
        prop_assert_eq!(rung, Ok(model.host_sleeping.then_some(())));
        let at = model.slot(model.cursor);
        model.answers[at].write(&bytes);
        model.cursor = model.cursor.wrapping_add(1);
        model.resp_head = model.cursor;
        model.running = false;
        Ok(())
    }

    /// The peer exits; the host takes over, and a new peer starts on the counters the take-over left.
    fn peer_dies(&mut self) -> Result<(), TestCaseError> {
        let abandoned = (self.model.read(), self.model.sent);
        let mut answered = Vec::new();
        let real = self.hosts[0].take_over(&mut answered);
        match self.model.take_over() {
            Ok(TakeOver {
                answered: want,
                running,
            }) => {
                let split = real.map_err(|e| TestCaseError::fail(format!("take_over refused: {e}")))?;
                prop_assert_eq!(split.running().map(Seq::get), running);
                let got: Vec<_> = answered
                    .into_iter()
                    .map(|DeadPeerAnswer { seq, message }| (seq.get(), message.map(|b| b.to_vec())))
                    .collect();
                prop_assert_eq!(got, want);
            }
            Err(cause) => {
                let DeadPeerError {
                    cause: got,
                    abandoned: range,
                } = real
                    .err()
                    .ok_or_else(|| TestCaseError::fail("take_over believed counters the model does not"))?;
                prop_assert_eq!(got, cause);
                prop_assert_eq!((range.start().get(), range.end().get()), abandoned);
                prop_assert!(answered.is_empty());
            }
        }
        self.peer = PeerEnd::new(Arc::clone(self.hosts[0].channel()), Bell, Awake);
        Ok(())
    }

    fn corrupt(&mut self, field: Field, scribble: Scribble) {
        let m = &mut self.model;
        let at = m.slot(m.read());
        let word = match field {
            Field::Taken => &mut m.taken,
            Field::RespHead => &mut m.resp_head,
            Field::Len => &mut m.answers[at].len,
        };
        *word = match scribble {
            Scribble::Near(d) => word.wrapping_add_signed(i32::from(d)),
            Scribble::Any(v) => v,
        };
        let value = *word;
        let memory = &self.hosts[0].channel().memory;
        let h = memory.header();
        match field {
            Field::Taken => h.taken.store(value, Relaxed),
            Field::RespHead => h.resp_head.store(value, Relaxed),
            Field::Len => {
                let slot = memory.slot(memory.geometry().answer_slot(Seq::new(m.read())));
                slot.len_word().store(value, Relaxed);
            }
        }
    }

    /// What holds after every step: the host's window is the model's, never past the ring's depth, and both say the
    /// same about news waiting.
    fn check_invariants(&self) -> Result<(), TestCaseError> {
        let (host, m) = (&self.hosts[0], &self.model);
        prop_assert_eq!(host.window(), Window::new(Seq::new(m.read()), Seq::new(m.sent)));
        prop_assert_eq!(host.in_flight(), m.flight.len() as u32);
        prop_assert!(host.in_flight() <= m.depth, "in flight past the depth");
        let moved = m.resp_head != m.read();
        prop_assert_eq!(host.has_answers(), m.fault.is_none() && moved);
        prop_assert_eq!(host.has_news(), m.fault.is_some() || moved);
        let h = self.channel().memory.header();
        prop_assert_eq!(
            (h.taken.load(Relaxed), h.resp_head.load(Relaxed)),
            (m.taken, m.resp_head)
        );
        Ok(())
    }
}

fn take_all(host: &mut Host) -> Result<Vec<(u32, Vec<u8>)>, PeerFault> {
    let mut ready = host.answers()?;
    let mut out = Vec::new();
    loop {
        let mut buf = BytesMut::new();
        match ready.take_next(&mut buf)? {
            Some(seq) => out.push((seq.get(), buf.to_vec())),
            None => return Ok(out),
        }
    }
}

fn run(ops: &[Op], depth: u32, faulted_neighbour: bool) -> Result<Vec<bool>, TestCaseError> {
    let mut harness = Harness::new(depth, faulted_neighbour);
    let mut sends = Vec::new();
    for op in ops {
        sends.extend(harness.step(op)?);
    }
    Ok(sends)
}

proptest! {
    /// Every step of any sequence, the peer's corrupting writes included, comes out of the channel as it comes out of
    /// the model.
    #[test]
    #[cfg_attr(miri, ignore = "proptest reads its regression file, which Miri's isolation refuses; the unit tests \
                               run the same code under Miri")]
    fn the_channel_answers_every_step_as_its_reference_model(
        depth in prop_oneof![Just(2u32), Just(4)],
        ops in prop::collection::vec(op(), 0..64),
    ) {
        run(&ops, depth, false)?;
    }

    /// A channel beside one whose peer wrote nonsense, sharing its host's sleeps, still answers as the model.
    #[test]
    #[cfg_attr(miri, ignore = "proptest reads its regression file, which Miri's isolation refuses; the unit tests \
                               run the same code under Miri")]
    fn a_faulted_neighbour_changes_nothing_on_a_channel(ops in prop::collection::vec(op(), 0..64)) {
        run(&ops, 2, true)?;
    }

    /// A sequence the peer does not corrupt sends at twice the depth every request it sent at the depth.
    #[test]
    #[cfg_attr(miri, ignore = "proptest reads its regression file, which Miri's isolation refuses; the unit tests \
                               run the same code under Miri")]
    fn a_deeper_ring_refuses_no_request_a_shallower_one_took(
        ops in prop::collection::vec(honest_op(), 0..64),
    ) {
        let shallow = run(&ops, 2, false)?;
        let deep = run(&ops, 4, false)?;
        prop_assert!(shallow.iter().zip(&deep).all(|(&s, &d)| !s || d), "{:?} / {:?}", shallow, deep);
    }
}
