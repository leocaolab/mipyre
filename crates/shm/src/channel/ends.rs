//! Holds the channel's two ends: the caller's, which keeps its own window on the ring, its last look at what the peer
//! published and whether the peer ever wrote what no correct peer writes; and the peer's, which waits for requests
//! and answers them, counting how it waited where it waits.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use super::header::{add_sole_writer, NOTE_BYTES};
use super::slot::{PeerRequest, ReadyResponses, RequestSlot};
use super::{Channel, Memory};
use crate::plan::ring::{self, ExitSplit, PeerCounterError, RingWindow, Seq, SeqRange};
use crate::plan::slot_len::SlotLengthError;
use crate::plan::spin_window::SpinWindow;
use crate::plan::start::{self, decode_start, StartClaim, StartReportError};
use crate::plan::wakes::{spend_wakes, WakeFloodError};
use crate::protocol::doorbell::{park_slice, spin, Doorbell, ParkOutcome, Parker};
use crate::sync::{
    fence, AtomicU32, AtomicU64,
    Ordering::{Acquire, Relaxed, Release, SeqCst},
};

/// The peer wrote a value no correct peer writes, so its memory is not to be trusted. The caller's end keeps the first
/// one and reads no slot of the channel again until it takes over from the dead peer. It is a fact about the peer,
/// not the failure of one call: each variant holds the error that measured it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerFault {
    Counter(PeerCounterError),
    Length(SlotLengthError),
    Wakes(WakeFloodError),
}

/// The number the caller picks for one start attempt, which the peer of that attempt reports under. The caller never
/// picks a number twice for one channel, so a peer of an earlier attempt cannot report under a later one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Attempt(u32);

impl Attempt {
    /// Returns the attempt the caller handed the peer as a number.
    #[inline]
    pub const fn new(raw: u32) -> Self {
        Attempt(raw)
    }

    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// What came of a start attempt, as its peer reported it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartOutcome {
    Ready,
    /// The peer could not start; the note it left, at most [`NOTE_BYTES`].
    Failed(Vec<u8>),
}

/// An answer a dead peer published, copied out when the caller took over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadPeerResponse {
    pub seq: Seq,
    pub message: Result<Bytes, SlotLengthError>,
}

/// A dead peer's counters do not fit the caller's window; the caller gave up every request it had sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the dead peer's counters are past belief ({cause}); requests {} to {} are given up", abandoned.start(), abandoned.end())]
pub struct TakeOverError {
    #[source]
    pub cause: PeerCounterError,
    pub abandoned: SeqRange,
}

/// Says whether a caller about to sleep may: only `Park` lets it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SleepDecision {
    Park,
    /// An end has answers the caller has not taken, or a fault it has not reported yet.
    Collect,
}

/// The caller's end of a channel: it sends requests, reads answers and starts peers. Every run makes a new channel,
/// so a new end's zeroed window is true.
pub struct RequesterEnd<M: Memory, B> {
    channel: Arc<Channel<M>>,
    bell: B,
    window: RingWindow,
    /// The peer's `published` counter as the caller last read it and believed it.
    published: Seq,
    fault: Option<PeerFault>,
    /// The wake bytes the peer may still write: one per slot, and one per answer read up to `wakes_counted_to`.
    wake_room: u64,
    wakes_counted_to: Seq,
}

impl<M: Memory, B: Doorbell> RequesterEnd<M, B> {
    pub fn new(channel: Arc<Channel<M>>, bell: B) -> Self {
        let zero = Seq::new(0);
        let wake_room = channel.memory.geometry().depth().get().into();
        RequesterEnd {
            channel,
            bell,
            window: RingWindow::new(zero, zero),
            published: zero,
            fault: None,
            wake_room,
            wakes_counted_to: zero,
        }
    }

    /// Returns the free slot of the next request; `None` when the ring is full or the peer is at fault.
    #[inline]
    pub fn request_slot(&mut self) -> Option<RequestSlot<'_, M, B>> {
        if self.fault.is_some() || !self.window.has_room(self.channel.memory.geometry().depth()) {
            return None;
        }
        Some(RequestSlot::new(&self.channel, &self.bell, &mut self.window))
    }

    /// Reads what the peer published and hands out the answers ready since the last read. The only way to answers.
    ///
    /// # Errors
    ///
    /// The peer's fault, found now or kept from before.
    #[inline]
    pub fn responses(&mut self) -> Result<ReadyResponses<'_, M>, PeerFault> {
        self.observe()?;
        Ok(ReadyResponses::new(
            &self.channel,
            &mut self.window,
            self.published,
            &mut self.fault,
        ))
    }

    /// Returns the oldest request the peer took and had not answered when the caller looked, read `published` first
    /// and `taken` after; for judging how long the peer has been busy.
    ///
    /// # Errors
    ///
    /// The peer's fault, found now or kept from before.
    pub fn running(&mut self) -> Result<Option<Seq>, PeerFault> {
        self.observe()?;
        let taken = Seq::new(self.channel.memory.header().taken.load(Acquire));
        ring::running(&self.window, self.published, taken).map_err(|e| self.keep_fault(PeerFault::Counter(e)))
    }

    /// Reads `published` and believes it once it fits the window; a value that does not is kept as the peer's fault,
    /// and from then on the memory is not read again.
    #[inline(always)]
    fn observe(&mut self) -> Result<(), PeerFault> {
        if let Some(fault) = self.fault {
            return Err(fault);
        }
        let published = Seq::new(self.channel.memory.header().resp_head.load(Acquire));
        match ring::count_ready_responses(&self.window, published) {
            Ok(_) => {
                self.published = published;
                Ok(())
            }
            Err(e) => Err(self.keep_fault(PeerFault::Counter(e))),
        }
    }

    /// Adds the answers read since the last count to the room the peer has for wake bytes. The caller counts often
    /// enough (each round of its poller) that fewer than 2^32 answers pass between two counts.
    #[inline]
    pub fn count_responses(&mut self) {
        let read = self.window.read();
        self.wake_room = self.wake_room.saturating_add(read.since(self.wakes_counted_to).into());
        self.wakes_counted_to = read;
    }

    /// Takes `bytes` the peer wrote into the caller's wake pipe; bytes past what its answers leave room for are kept as
    /// its fault.
    ///
    /// # Errors
    ///
    /// The peer's fault, found now or kept from before.
    pub fn record_wake_bytes(&mut self, bytes: usize) -> Result<(), PeerFault> {
        if let Some(fault) = self.fault {
            return Err(fault);
        }
        self.count_responses();
        match spend_wakes(self.wake_room, bytes as u64) {
            Ok(left) => {
                self.wake_room = left;
                Ok(())
            }
            Err(e) => Err(self.keep_fault(PeerFault::Wakes(e))),
        }
    }

    fn keep_fault(&mut self, fault: PeerFault) -> PeerFault {
        *self.fault.insert(fault)
    }

    /// Starts start attempt `attempt`, a number the caller picked and never picked before for this channel: clears the
    /// last attempt's report and writes the number. The lane hands the number to the peer it starts next, once any
    /// earlier attempt's peer has exited or never started. The attempt's ready report rings the caller once, so its
    /// byte is room the peer has.
    pub fn begin_attempt(&mut self, attempt: Attempt) {
        self.wake_room = self.wake_room.saturating_add(1);
        let area = &self.channel.memory.header().start;
        area.state.store(start::NOT_REPORTED, Relaxed);
        area.note_len.store(0, Relaxed);
        area.attempt.store(attempt.0, Release);
    }

    /// Returns what the peer of `attempt` reported: `Ok(None)` before it reported, or when the report is for another
    /// attempt.
    ///
    /// # Errors
    ///
    /// The report holds a state or a note length no peer writes.
    pub fn start_report(&self, attempt: Attempt) -> Result<Option<StartOutcome>, StartReportError> {
        let memory = &self.channel.memory;
        let area = &memory.header().start;
        let state = area.state.load(Acquire);
        if area.attempt.load(Relaxed) != attempt.0 {
            return Ok(None);
        }
        let note_len = area.note_len.load(Relaxed);
        Ok(match decode_start(state, note_len, NOTE_BYTES)? {
            None => None,
            Some(StartClaim::Ready) => Some(StartOutcome::Ready),
            Some(StartClaim::Failed { note_len }) => {
                let mut note = BytesMut::new();
                memory
                    .note()
                    .append_to(note_len, &mut note)
                    .map_err(StartReportError::NoteLength)?;
                Some(StartOutcome::Failed(note.into()))
            }
        })
    }

    /// Takes over from a peer that exited, in one step: reads its counters, splits the window by them, copies each
    /// answer it published into `answered` and gives up every other request sent, the one it died running and the
    /// ones it never took alike; the next peer starts after them. The fault kept from it is cleared.
    ///
    /// The requests it never took are not handed to the next peer: their bytes sat in memory the dead peer could
    /// write, so the next peer could run a request no client sent. The lane calls it only once it saw the peer exit;
    /// the channel knows no process.
    ///
    /// # Errors
    ///
    /// The counters do not fit the window: every request sent is given up, as [`RequesterEnd::abandon`] does.
    pub fn take_over(&mut self, answered: &mut Vec<DeadPeerResponse>) -> Result<ExitSplit, TakeOverError> {
        let memory = &self.channel.memory;
        let h = memory.header();
        let published = Seq::new(h.resp_head.load(Acquire));
        let taken = Seq::new(h.taken.load(Acquire));
        let split = match ring::exit_split(&self.window, published, taken) {
            Ok(split) => split,
            Err(cause) => {
                return Err(TakeOverError {
                    cause,
                    abandoned: self.abandon(),
                })
            }
        };
        let done = split.responded();
        answered.extend((0..done.len()).map(|i| {
            let seq = done.start().after(i);
            let mut message = BytesMut::new();
            let len = memory
                .slot(memory.geometry().response_offset(seq))
                .take_message(&mut message);
            DeadPeerResponse {
                seq,
                message: len.map(|_| message.freeze()),
            }
        }));
        self.restart_at(self.window.sent());
        Ok(split)
    }

    /// Gives up every request sent and not answered, and returns them; the next peer starts after them.
    pub fn abandon(&mut self) -> SeqRange {
        let given_up = self.window.outstanding();
        self.restart_at(self.window.sent());
        given_up
    }

    /// Moves `read` and the peer's counters to `next`, with nothing kept against the old peer. The old peer could
    /// write the caller's own `req_head` too, so it is written again: the next peer must not take a request the caller
    /// never published.
    fn restart_at(&mut self, next: Seq) {
        let h = self.channel.memory.header();
        h.req_head.store(self.window.sent().get(), Release);
        h.taken.store(next.get(), Release);
        h.resp_head.store(next.get(), Release);
        self.window = RingWindow::new(next, self.window.sent());
        self.published = next;
        self.fault = None;
        self.wake_room = self.channel.memory.geometry().depth().get().into();
        self.wakes_counted_to = next;
    }

    /// Returns the peer's fault the end keeps, if any.
    #[inline]
    pub fn fault(&self) -> Option<PeerFault> {
        self.fault
    }

    #[inline]
    pub fn in_flight(&self) -> u32 {
        self.window.in_flight()
    }

    /// Returns the channel this end is on.
    #[inline]
    pub fn channel(&self) -> &Arc<Channel<M>> {
        &self.channel
    }

    /// Returns the caller's own place on the ring: the answers it read, the requests it sent.
    #[inline]
    pub fn window(&self) -> RingWindow {
        self.window
    }

    /// Says whether the peer published an answer the caller has not taken; an end whose fault is kept says no, as
    /// [`prepare_to_wait`] skips it. For spinning on: it believes nothing, so the answers are still read through
    /// [`RequesterEnd::answers`].
    #[inline]
    pub fn has_responses(&self) -> bool {
        self.fault.is_none() && self.channel.memory.header().resp_head.load(Acquire) != self.window.read().get()
    }

    /// Says whether [`RequesterEnd::answers`] has anything to hand out: an answer published and not taken, or the peer's
    /// fault, kept or new. A caller that reads its ends often looks here first, so an end with nothing new costs it one
    /// load.
    #[inline]
    pub fn has_responses_or_fault(&self) -> bool {
        self.fault.is_some() || self.channel.memory.header().resp_head.load(Acquire) != self.window.read().get()
    }

    fn requester_sleeping(&self) -> &AtomicU32 {
        &self.channel.memory.header().requester_sleeping
    }
}

/// Says to every end's peer that the caller is about to sleep, then reads, once, what each end's peer published,
/// keeping any fault on its end. The caller sleeps only on `Park`; on `Collect` it reads the answers through
/// [`RequesterEnd::answers`] as always. An end whose fault was kept before is skipped, so a faulted peer does not keep
/// the caller awake.
pub fn prepare_to_wait<M: Memory, B: Doorbell>(ends: &mut [RequesterEnd<M, B>]) -> SleepDecision {
    for end in ends.iter() {
        end.requester_sleeping().store(1, Relaxed);
    }
    // One fence for every end: it pairs with each peer's publish (`protocol::doorbell::publish`), so of a peer that
    // published and the caller that is about to sleep one sees the other.
    fence(SeqCst);
    let mut verdict = SleepDecision::Park;
    for end in ends.iter_mut().filter(|e| e.fault.is_none()) {
        let found = end.observe().is_err() || end.published != end.window.read();
        if found {
            verdict = SleepDecision::Collect;
        }
    }
    verdict
}

/// Says to every end's peer that the caller is awake, on the ends [`prepare_to_wait`] was given.
pub fn finish_wait<M: Memory, B: Doorbell>(ends: &mut [RequesterEnd<M, B>]) {
    for end in ends.iter() {
        end.requester_sleeping().store(0, Relaxed);
    }
}

/// How one call of [`PeerEnd::wait_for_request`] ended.
#[must_use = "a request taken is answered, or taken again"]
pub enum WaitOutcome<'p, M: Memory, B: Doorbell> {
    Request(PeerRequest<'p, M, B>),
    /// The slice passed with no request; whoever waits decides whether to wait again.
    SliceOver,
}

/// The peer's end of a channel: it waits for requests, answers them and reports on its start.
pub struct PeerEnd<M: Memory, B, P> {
    channel: Arc<Channel<M>>,
    bell: B,
    parker: P,
    /// The next request to take.
    cursor: Seq,
    /// Whether the wait for `cursor` was counted already, by an earlier call that did not hand it out.
    waiting: bool,
}

impl<M: Memory, B: Doorbell, P: Parker> PeerEnd<M, B, P> {
    /// Returns the end starting at the request after the last one taken: 0 in a new channel, or where the caller's
    /// take-over left the counters.
    pub fn new(channel: Arc<Channel<M>>, bell: B, parker: P) -> Self {
        let cursor = Seq::new(channel.memory.header().taken.load(Acquire));
        PeerEnd {
            channel,
            bell,
            parker,
            cursor,
            waiting: false,
        }
    }

    /// Waits for the next request: spins until `window` is over on the clock `now`, then sleeps one slice of
    /// `slice_ns`. The request is marked taken before it is handed out. A request there at once costs no count; a
    /// wait counts once however many slices it takes.
    ///
    /// # Errors
    ///
    /// The parker's error: the OS refused the sleep.
    pub fn wait_for_request(
        &mut self,
        window: SpinWindow,
        now: impl FnMut() -> u64,
        slice_ns: u64,
    ) -> Result<WaitOutcome<'_, M, B>, P::Error> {
        let h = self.channel.memory.header();
        let seen = self.cursor.get();
        if h.req_head.load(Acquire) == seen {
            let counts = &h.peer_waits;
            if !self.waiting {
                add_sole_writer(&counts.waits, 1);
                self.waiting = true;
            }
            let spun = spin(window, now, || h.req_head.load(Acquire) != seen);
            add_sole_writer(&counts.spin_rounds, spun.rounds.into());
            if !spun.ready {
                let sleeper = Sleeper {
                    parker: &self.parker,
                    parks: &counts.parks,
                };
                if park_slice(&h.req_head, &h.peer_sleeping, seen, slice_ns, &sleeper)? == ParkOutcome::SliceOver {
                    return Ok(WaitOutcome::SliceOver);
                }
            }
        }
        self.waiting = false;
        h.taken.store(seen.wrapping_add(1), Release);
        Ok(WaitOutcome::Request(PeerRequest::new(
            &self.channel,
            &self.bell,
            &mut self.cursor,
        )))
    }

    /// Reports that the peer of `attempt` started, then rings the caller, which waits for its workers on the bell; a
    /// peer of another attempt writes and rings nothing, and gets `Ok(None)`.
    ///
    /// # Errors
    ///
    /// The doorbell's, when it could not ring.
    pub fn report_ready(&self, attempt: Attempt) -> Result<Option<B::Outcome>, B::Error> {
        let area = &self.channel.memory.header().start;
        if area.attempt.load(Acquire) != attempt.0 {
            return Ok(None);
        }
        area.state.store(start::READY, Release);
        self.bell.ring(&area.state).map(Some)
    }

    /// Reports that the peer of `attempt` could not start, leaving `note` (cut to [`NOTE_BYTES`]) for the caller; a peer
    /// of another attempt writes nothing. The peer serves no request after it.
    pub fn report_failed(self, attempt: Attempt, note: &[u8]) {
        let memory = &self.channel.memory;
        let area = &memory.header().start;
        if area.attempt.load(Acquire) != attempt.0 {
            return;
        }
        area.note_len.store(memory.note().copy_prefix_in(note), Relaxed);
        area.state.store(start::FAILED, Release);
    }
}

/// The peer's parker, counting each sleep where it sleeps.
struct Sleeper<'p, P> {
    parker: &'p P,
    parks: &'p AtomicU64,
}

impl<P: Parker> Parker for Sleeper<'_, P> {
    type Error = P::Error;

    fn park(&self, word: &AtomicU32, expect: u32, slice_ns: u64) -> Result<(), P::Error> {
        add_sole_writer(self.parks, 1);
        self.parker.park(word, expect, slice_ns)
    }
}

#[cfg(test)]
#[cfg(not(shm_model))]
mod tests {
    use std::convert::Infallible;

    use super::*;
    use crate::channel::memory::sealed::Access;
    use crate::channel::{HeapMemory, SlotRoomError, Waits};
    use crate::plan::geometry::{Geometry, SlotBytes};
    use crate::plan::ring::RingDepth;

    /// A doorbell that counts its rings.
    #[derive(Default)]
    struct SpyDoorbell(AtomicU32);

    impl SpyDoorbell {
        fn rings(&self) -> u32 {
            self.0.load(Relaxed)
        }
    }

    impl Doorbell for SpyDoorbell {
        type Outcome = ();
        type Error = Infallible;
        fn ring(&self, _: &AtomicU32) -> Result<(), Infallible> {
            self.0.fetch_add(1, Relaxed);
            Ok(())
        }
    }

    /// A parker whose every sleep ends at once, or is refused.
    struct StubParker(Result<(), &'static str>);

    impl Parker for StubParker {
        type Error = &'static str;
        fn park(&self, _: &AtomicU32, _: u32, _: u64) -> Result<(), &'static str> {
            std::thread::yield_now();
            self.0
        }
    }

    type Requester = RequesterEnd<HeapMemory, SpyDoorbell>;
    type Peer = PeerEnd<HeapMemory, SpyDoorbell, StubParker>;

    fn geometry(depth: u32, slot_bytes: usize) -> Geometry {
        Geometry::new(RingDepth::new(depth).unwrap(), SlotBytes::new(slot_bytes).unwrap()).unwrap()
    }

    fn ends(depth: u32) -> (Requester, Peer) {
        let channel = Arc::new(Channel::new(HeapMemory::new(&geometry(depth, 64))));
        (
            RequesterEnd::new(Arc::clone(&channel), SpyDoorbell::default()),
            PeerEnd::new(channel, SpyDoorbell::default(), StubParker(Ok(()))),
        )
    }

    fn header(caller: &Requester) -> &super::super::header::ChannelHeader {
        caller.channel.memory.header()
    }

    fn send(caller: &mut Requester, message: &[u8]) -> Seq {
        let mut slot = caller.request_slot().expect("the ring has room");
        let seq = slot.seq();
        slot.write(|w| w.put(message)).unwrap();
        slot.publish().unwrap();
        seq
    }

    fn wait(peer: &mut Peer) -> Result<WaitOutcome<'_, HeapMemory, SpyDoorbell>, &'static str> {
        peer.wait_for_request(SpinWindow::until(0), || 0, 1)
    }

    /// Takes the next request and answers it with its own bytes followed by `suffix`.
    fn echo(peer: &mut Peer, suffix: &[u8]) -> Seq {
        let Ok(WaitOutcome::Request(request)) = wait(peer) else {
            panic!("a request was sent")
        };
        let (seq, mut answer) = (request.seq(), request.bytes().unwrap().to_vec());
        answer.extend_from_slice(suffix);
        request.respond(|w| w.put(&answer).unwrap()).unwrap();
        seq
    }

    fn take_all(caller: &mut Requester) -> Result<Vec<(Seq, Vec<u8>)>, PeerFault> {
        let mut ready = caller.responses()?;
        let mut out = Vec::new();
        loop {
            let mut buf = BytesMut::new();
            match ready.take_next(&mut buf)? {
                Some(seq) => out.push((seq, buf.to_vec())),
                None => return Ok(out),
            }
        }
    }

    #[test]
    fn a_request_sent_is_taken_answered_and_read_back() {
        let (mut caller, mut peer) = ends(4);
        assert_eq!(send(&mut caller, b"ping"), Seq::new(0));
        assert_eq!(echo(&mut peer, b"!"), Seq::new(0));
        assert_eq!(take_all(&mut caller), Ok(vec![(Seq::new(0), b"ping!".to_vec())]));
        assert_eq!(caller.in_flight(), 0);
        assert_eq!(header(&caller).taken.load(Relaxed), 1);
    }

    #[test]
    fn a_full_ring_gives_no_request_slot() {
        let (mut caller, _peer) = ends(2);
        send(&mut caller, b"a");
        send(&mut caller, b"b");
        assert!(caller.request_slot().is_none());
        assert_eq!(caller.in_flight(), 2);
    }

    #[test]
    fn a_request_slot_dropped_unpublished_sends_nothing() {
        let (mut caller, _peer) = ends(2);
        drop(caller.request_slot().unwrap());
        assert_eq!((caller.in_flight(), header(&caller).req_head.load(Relaxed)), (0, 0));
    }

    #[test]
    fn a_publish_rings_the_peer_only_when_it_said_it_sleeps() {
        let (mut caller, _peer) = ends(4);
        assert_eq!(caller.request_slot().unwrap().publish(), Ok(None));
        header(&caller).peer_sleeping.store(1, Relaxed);
        assert_eq!(caller.request_slot().unwrap().publish(), Ok(Some(())));
        assert_eq!(caller.bell.rings(), 1);
    }

    /// A slot published unwritten holds an empty message, never the length of the request before it in that slot.
    #[test]
    fn a_request_published_unwritten_is_empty_not_the_slots_last_request() {
        let (mut caller, mut peer) = ends(1);
        send(&mut caller, b"earlier");
        echo(&mut peer, b"");
        take_all(&mut caller).unwrap();
        caller.request_slot().unwrap().publish().unwrap();
        let Ok(WaitOutcome::Request(request)) = wait(&mut peer) else {
            panic!("a request was sent")
        };
        assert_eq!(request.bytes().map(|b| b.to_vec()), Ok(Vec::new()));
    }

    #[test]
    fn a_message_that_fits_the_slot_is_written_and_one_byte_more_is_refused() {
        let (mut caller, _peer) = ends(1);
        let room = geometry(1, 64).max_message();
        let mut slot = caller.request_slot().unwrap();
        assert_eq!(
            slot.write(|w| (w.room(), w.put(&[1; 50]), w.put(&[2; 7]))),
            (room, Ok(()), Err(SlotRoomError { room, needed: 57 }))
        );
        assert_eq!(
            slot.write(|w| (w.put(&[3; 6]), w.len())),
            (Ok(()), room),
            "a refused put leaves the length, a later write goes on"
        );
    }

    #[test]
    fn a_request_waiting_already_is_taken_without_counting_a_wait() {
        let (mut caller, mut peer) = ends(4);
        send(&mut caller, b"x");
        let Ok(WaitOutcome::Request(request)) = wait(&mut peer) else {
            panic!("a request was sent")
        };
        assert_eq!(request.seq(), Seq::new(0));
        assert_eq!(
            header(&caller).taken.load(Relaxed),
            1,
            "taken is written before the request is handed out"
        );
        assert_eq!(caller.channel.peer_waits(), Waits::default());
    }

    #[test]
    fn a_wait_counts_once_and_each_slice_counts_a_park() {
        let (caller, mut peer) = ends(4);
        assert!(matches!(wait(&mut peer), Ok(WaitOutcome::SliceOver)));
        assert!(matches!(wait(&mut peer), Ok(WaitOutcome::SliceOver)));
        assert_eq!(
            caller.channel.peer_waits(),
            Waits {
                waits: 1,
                spin_rounds: 0,
                parks: 2,
                wakes: 0
            }
        );
        assert_eq!(header(&caller).peer_sleeping.load(Relaxed), 0);
    }

    #[test]
    fn a_refused_sleep_is_the_parkers_error() {
        let channel = Arc::new(Channel::new(HeapMemory::new(&geometry(4, 64))));
        let mut peer = PeerEnd::new(channel, SpyDoorbell::default(), StubParker(Err("refused")));
        assert!(matches!(
            peer.wait_for_request(SpinWindow::until(0), || 0, 1),
            Err("refused")
        ));
    }

    /// The wait goes on after the refused sleep, so the call after it counts no second wait.
    #[test]
    fn a_wait_counts_once_across_a_refused_sleep() {
        struct StubParker(std::cell::Cell<bool>);
        impl Parker for StubParker {
            type Error = &'static str;
            fn park(&self, _: &AtomicU32, _: u32, _: u64) -> Result<(), &'static str> {
                if self.0.replace(false) {
                    return Err("refused");
                }
                Ok(())
            }
        }
        let channel = Arc::new(Channel::new(HeapMemory::new(&geometry(4, 64))));
        let mut peer = PeerEnd::new(
            Arc::clone(&channel),
            SpyDoorbell::default(),
            StubParker(std::cell::Cell::new(true)),
        );
        assert!(matches!(
            peer.wait_for_request(SpinWindow::until(0), || 0, 1),
            Err("refused")
        ));
        assert!(matches!(
            peer.wait_for_request(SpinWindow::until(0), || 0, 1),
            Ok(WaitOutcome::SliceOver)
        ));
        assert_eq!(
            channel.peer_waits(),
            Waits {
                waits: 1,
                spin_rounds: 0,
                parks: 2,
                wakes: 0
            }
        );
    }

    #[test]
    fn a_request_whose_length_passes_the_slot_is_refused_with_both_values() {
        let (mut caller, mut peer) = ends(1);
        let max = geometry(1, 64).max_message();
        let mut slot = caller.request_slot().unwrap();
        slot.write(|w| w.put(&vec![7; max as usize])).unwrap();
        slot.publish().unwrap();
        {
            let Ok(WaitOutcome::Request(request)) = wait(&mut peer) else {
                panic!("a request was sent")
            };
            assert_eq!(request.bytes().map(|b| b.len()), Ok(max as usize));
        }
        let at = geometry(1, 64).request_offset(Seq::new(0));
        caller.channel.memory.slot(at).len_word().store(max + 1, Relaxed);
        let Ok(WaitOutcome::Request(request)) = wait(&mut peer) else {
            panic!("an unanswered request is taken again")
        };
        assert_eq!(
            request.bytes().map(|b| b.len()),
            Err(SlotLengthError::PastSlot { len: max + 1, max })
        );
    }

    #[test]
    fn an_answer_rings_the_caller_only_when_it_said_it_sleeps_and_counts_the_ring() {
        let (mut caller, mut peer) = ends(4);
        send(&mut caller, b"a");
        send(&mut caller, b"b");
        let Ok(WaitOutcome::Request(request)) = wait(&mut peer) else {
            panic!()
        };
        assert_eq!(request.respond(|_| {}), Ok(None));
        header(&caller).requester_sleeping.store(1, Relaxed);
        let Ok(WaitOutcome::Request(request)) = wait(&mut peer) else {
            panic!()
        };
        assert_eq!(request.respond(|_| {}), Ok(Some(())));
        assert_eq!((peer.bell.rings(), caller.channel.peer_waits().wakes), (1, 1));
    }

    #[test]
    fn answers_up_to_what_was_sent_are_read_and_past_it_the_peer_is_at_fault_for_good() {
        let (mut caller, mut peer) = ends(8);
        for _ in 0..5 {
            send(&mut caller, b"r");
        }
        echo(&mut peer, b"");
        assert_eq!(take_all(&mut caller).map(|a| a.len()), Ok(1));
        header(&caller).resp_head.store(5, Relaxed);
        assert_eq!(caller.responses().map(|_| ()), Ok(()), "every request sent answered");
        header(&caller).resp_head.store(6, Relaxed);
        let fault = PeerFault::Counter(PeerCounterError::AnswersPastSent {
            read: Seq::new(1),
            sent: Seq::new(5),
            published: Seq::new(6),
        });
        assert_eq!(caller.responses().err(), Some(fault));
        header(&caller).resp_head.store(2, Relaxed);
        assert_eq!(
            caller.responses().err(),
            Some(fault),
            "the fault is kept, and the counter is not read again"
        );
        assert!(caller.request_slot().is_none());
        assert!(!caller.has_responses(), "a faulted end has nothing to spin for");
        assert!(caller.has_responses_or_fault(), "but its fault is still to be read");
    }

    /// A peer may write a wake byte per slot and one per answer the caller read; a byte past that is its fault, kept
    /// as any other until the caller takes over, and the next peer starts with a slot's room each again.
    #[test]
    fn wake_bytes_up_to_the_answers_read_and_the_slots_are_heard_and_one_past_them_is_the_peers_fault() {
        let (mut caller, mut peer) = ends(4);
        for _ in 0..2 {
            send(&mut caller, b"r");
            echo(&mut peer, b"");
        }
        assert_eq!(take_all(&mut caller).map(|a| a.len()), Ok(2));
        assert_eq!(caller.record_wake_bytes(5), Ok(()));
        assert_eq!(caller.record_wake_bytes(1), Ok(()));
        let fault = PeerFault::Wakes(WakeFloodError { bytes: 1, room: 0 });
        assert_eq!(caller.record_wake_bytes(1), Err(fault));
        assert_eq!(caller.responses().err(), Some(fault), "the fault is kept");
        assert!(caller.has_responses_or_fault() && caller.request_slot().is_none());
        caller.take_over(&mut Vec::new()).unwrap();
        assert_eq!(caller.record_wake_bytes(4), Ok(()));
        assert_eq!(
            caller.record_wake_bytes(1),
            Err(PeerFault::Wakes(WakeFloodError { bytes: 1, room: 0 }))
        );
    }

    /// The spin's look says yes once an answer is published and no once it is taken.
    #[test]
    fn an_end_has_answers_from_a_publish_until_they_are_taken() {
        let (mut caller, mut peer) = ends(4);
        send(&mut caller, b"r");
        assert!(!caller.has_responses() && !caller.has_responses_or_fault());
        echo(&mut peer, b"");
        assert!(caller.has_responses() && caller.has_responses_or_fault());
        assert_eq!(take_all(&mut caller).map(|a| a.len()), Ok(1));
        assert!(!caller.has_responses() && !caller.has_responses_or_fault());
        assert_eq!(caller.window(), RingWindow::new(Seq::new(1), Seq::new(1)));
    }

    #[test]
    fn an_answer_taken_is_the_callers_own_whatever_the_peer_writes_after() {
        let (mut caller, mut peer) = ends(2);
        send(&mut caller, b"abc");
        echo(&mut peer, b"");
        let mut buf = BytesMut::new();
        assert_eq!(caller.responses().unwrap().take_next(&mut buf), Ok(Some(Seq::new(0))));
        let at = geometry(2, 64).response_offset(Seq::new(0));
        assert_eq!(caller.channel.memory.slot(at).copy_in(0, b"XYZ"), Ok(3));
        assert_eq!(&buf[..], b"abc");
    }

    #[test]
    fn an_answer_up_to_the_slot_is_taken_and_one_byte_more_is_the_peers_fault() {
        let (mut caller, mut peer) = ends(4);
        let max = geometry(4, 64).max_message();
        send(&mut caller, b"a");
        send(&mut caller, b"b");
        echo(&mut peer, b"");
        echo(&mut peer, b"");
        let ring = geometry(4, 64);
        caller
            .channel
            .memory
            .slot(ring.response_offset(Seq::new(0)))
            .len_word()
            .store(max, Relaxed);
        caller
            .channel
            .memory
            .slot(ring.response_offset(Seq::new(1)))
            .len_word()
            .store(max + 1, Relaxed);
        let mut ready = caller.responses().unwrap();
        let mut buf = BytesMut::new();
        assert_eq!(ready.take_next(&mut buf), Ok(Some(Seq::new(0))));
        assert_eq!(buf.len(), max as usize);
        let fault = PeerFault::Length(SlotLengthError::PastSlot { len: max + 1, max });
        assert_eq!(ready.take_next(&mut buf), Err(fault));
        assert_eq!(buf.len(), max as usize, "a refused answer copies nothing");
        assert_eq!(ready.take_next(&mut buf), Err(fault));
        assert!(caller.request_slot().is_none());
        assert_eq!(caller.responses().err(), Some(fault));
    }

    #[test]
    fn answers_left_when_the_handle_is_dropped_come_with_the_next_one() {
        let (mut caller, mut peer) = ends(4);
        send(&mut caller, b"a");
        send(&mut caller, b"b");
        echo(&mut peer, b"");
        echo(&mut peer, b"");
        assert_eq!(
            caller.responses().unwrap().take_next(&mut BytesMut::new()),
            Ok(Some(Seq::new(0)))
        );
        assert_eq!(take_all(&mut caller), Ok(vec![(Seq::new(1), b"b".to_vec())]));
    }

    /// Between the caller's read of `published` and of `taken` a live peer may answer and take more; that is no
    /// fault. `taken` past `sent` is.
    #[test]
    fn a_live_peer_ahead_of_what_the_caller_saw_runs_and_one_past_what_was_sent_is_at_fault() {
        let (mut caller, _peer) = ends(8);
        for _ in 0..5 {
            send(&mut caller, b"r");
        }
        let channel = Arc::clone(&caller.channel);
        let h = channel.memory.header();
        assert_eq!(caller.running(), Ok(None));
        h.taken.store(1, Relaxed);
        assert_eq!(caller.running(), Ok(Some(Seq::new(0))));
        h.resp_head.store(2, Relaxed);
        h.taken.store(5, Relaxed);
        assert_eq!(caller.running(), Ok(Some(Seq::new(2))));
        h.taken.store(6, Relaxed);
        let fault = PeerFault::Counter(PeerCounterError::TakenOutOfRange {
            read: Seq::new(0),
            sent: Seq::new(5),
            published: Seq::new(2),
            taken: Seq::new(6),
        });
        assert_eq!(caller.running(), Err(fault));
        assert!(caller.request_slot().is_none());
    }

    fn two_ends() -> Vec<Requester> {
        (0..2).map(|_| ends(4).0).collect()
    }

    #[test]
    fn a_caller_parks_only_when_no_end_has_an_answer_waiting() {
        let mut requesters = two_ends();
        assert_eq!(prepare_to_wait(&mut requesters), SleepDecision::Park);
        assert!(requesters
            .iter()
            .all(|h| header(h).requester_sleeping.load(Relaxed) == 1));
        send(&mut requesters[1], b"x");
        header(&requesters[1]).taken.store(1, Relaxed);
        header(&requesters[1]).resp_head.store(1, Relaxed);
        assert_eq!(
            prepare_to_wait(&mut requesters),
            SleepDecision::Collect,
            "only the second end has an answer"
        );
        finish_wait(&mut requesters);
        assert!(requesters
            .iter()
            .all(|h| header(h).requester_sleeping.load(Relaxed) == 0));
        assert_eq!(take_all(&mut requesters[1]).map(|a| a.len()), Ok(1));
        assert_eq!(prepare_to_wait(&mut requesters), SleepDecision::Park);
    }

    /// A fault keeps the caller awake once, to report it, and never again (a faulted peer must not spin the caller).
    #[test]
    fn a_fault_keeps_the_caller_awake_once() {
        let mut requesters = two_ends();
        header(&requesters[0]).resp_head.store(9, Relaxed);
        assert_eq!(prepare_to_wait(&mut requesters), SleepDecision::Collect);
        assert!(requesters[0].responses().is_err());
        assert_eq!(prepare_to_wait(&mut requesters), SleepDecision::Park);
    }

    /// The peer answered two, took the third and died: the two answers come out whole, the third is the one it ran,
    /// the last two it never took are given up with it, and the next peer starts after all five.
    #[test]
    fn a_take_over_copies_the_dead_peers_answers_and_gives_up_the_rest() {
        let (mut caller, mut peer) = ends(8);
        for i in 0..5u8 {
            send(&mut caller, &[i]);
        }
        echo(&mut peer, b"a");
        echo(&mut peer, b"b");
        let Ok(WaitOutcome::Request(_running)) = wait(&mut peer) else {
            panic!()
        };
        drop(peer);
        let mut answered = Vec::new();
        let split = caller.take_over(&mut answered).unwrap();
        assert_eq!(split.running(), Some(Seq::new(2)));
        assert_eq!(
            (split.untaken().start(), split.untaken().end()),
            (Seq::new(3), Seq::new(5))
        );
        assert_eq!(
            answered,
            [
                DeadPeerResponse {
                    seq: Seq::new(0),
                    message: Ok(Bytes::from_static(b"\x00a"))
                },
                DeadPeerResponse {
                    seq: Seq::new(1),
                    message: Ok(Bytes::from_static(b"\x01b"))
                },
            ]
        );
        assert_eq!(
            (
                header(&caller).taken.load(Relaxed),
                header(&caller).resp_head.load(Relaxed)
            ),
            (5, 5)
        );
        assert_eq!(caller.in_flight(), 0);
        assert_eq!(take_all(&mut caller), Ok(vec![]));
        let mut next = PeerEnd::new(Arc::clone(&caller.channel), SpyDoorbell::default(), StubParker(Ok(())));
        assert!(
            matches!(wait(&mut next), Ok(WaitOutcome::SliceOver)),
            "the next peer was handed a request the dead one could write"
        );
        send(&mut caller, b"new");
        assert_eq!(
            echo(&mut next, b""),
            Seq::new(5),
            "the next peer starts after every request the dead one held"
        );
    }

    #[test]
    fn a_dead_peers_answer_past_its_slot_is_refused_and_the_rest_are_copied() {
        let (mut caller, mut peer) = ends(4);
        send(&mut caller, b"a");
        send(&mut caller, b"b");
        echo(&mut peer, b"");
        echo(&mut peer, b"");
        let ring = geometry(4, 64);
        let max = ring.max_message();
        caller
            .channel
            .memory
            .slot(ring.response_offset(Seq::new(0)))
            .len_word()
            .store(max + 1, Relaxed);
        let mut answered = Vec::new();
        assert!(caller.take_over(&mut answered).is_ok());
        assert_eq!(
            answered[0].message,
            Err(SlotLengthError::PastSlot { len: max + 1, max })
        );
        assert_eq!(answered[1].message, Ok(Bytes::from_static(b"b")));
    }

    #[test]
    fn a_take_over_after_a_kept_fault_starts_clean() {
        let (mut caller, mut peer) = ends(4);
        send(&mut caller, b"a");
        echo(&mut peer, b"");
        let max = geometry(4, 64).max_message();
        caller
            .channel
            .memory
            .slot(geometry(4, 64).response_offset(Seq::new(0)))
            .len_word()
            .store(max + 1, Relaxed);
        assert!(take_all(&mut caller).is_err());
        let mut answered = Vec::new();
        assert!(caller.take_over(&mut answered).is_ok());
        assert!(answered[0].message.is_err());
        assert!(caller.request_slot().is_some());
    }

    #[test]
    fn counters_past_belief_at_a_take_over_give_up_every_request_sent() {
        let (mut caller, _peer) = ends(8);
        for _ in 0..3 {
            send(&mut caller, b"r");
        }
        header(&caller).taken.store(2, Relaxed);
        let mut answered = Vec::new();
        let err = caller.take_over(&mut answered).unwrap_err();
        assert_eq!(
            err.cause,
            PeerCounterError::TakenOutOfRange {
                read: Seq::new(0),
                sent: Seq::new(3),
                published: Seq::new(0),
                taken: Seq::new(2)
            }
        );
        assert_eq!((err.abandoned.start(), err.abandoned.end()), (Seq::new(0), Seq::new(3)));
        assert!(answered.is_empty());
        assert_eq!(caller.in_flight(), 0);
        assert_eq!(
            (
                header(&caller).taken.load(Relaxed),
                header(&caller).resp_head.load(Relaxed)
            ),
            (3, 3)
        );
        assert!(caller.request_slot().is_some());
    }

    /// The dead peer wrote the caller's `req_head` past what was sent; the next peer takes only what the caller sent.
    #[test]
    fn a_take_over_or_abandon_writes_back_the_requests_the_caller_sent() {
        for give_up in [false, true] {
            let (mut caller, _dead) = ends(8);
            send(&mut caller, b"a");
            header(&caller).req_head.store(5, Relaxed);
            if give_up {
                caller.abandon();
            } else {
                caller.take_over(&mut Vec::new()).unwrap();
            }
            assert_eq!(header(&caller).req_head.load(Relaxed), 1, "abandon: {give_up}");
            let mut next = PeerEnd::new(Arc::clone(&caller.channel), SpyDoorbell::default(), StubParker(Ok(())));
            assert!(
                matches!(wait(&mut next), Ok(WaitOutcome::SliceOver)),
                "abandon: {give_up}"
            );
        }
    }

    #[test]
    fn abandoning_gives_up_what_was_in_flight() {
        let (mut caller, mut peer) = ends(8);
        for _ in 0..3 {
            send(&mut caller, b"r");
        }
        echo(&mut peer, b"");
        take_all(&mut caller).unwrap();
        let given_up = caller.abandon();
        assert_eq!((given_up.start(), given_up.end()), (Seq::new(1), Seq::new(3)));
        assert_eq!(caller.in_flight(), 0);
    }

    #[test]
    fn a_start_attempt_reports_what_its_peer_said_and_nothing_before() {
        let (mut caller, peer) = ends(1);
        let attempt = Attempt::new(1);
        caller.begin_attempt(attempt);
        assert_eq!(caller.start_report(attempt), Ok(None));
        peer.report_ready(attempt).unwrap();
        assert_eq!(caller.start_report(attempt), Ok(Some(StartOutcome::Ready)));
        assert_eq!(
            caller.start_report(Attempt::new(attempt.get() + 1)),
            Ok(None),
            "another attempt's report"
        );
    }

    #[test]
    fn a_ready_report_rings_the_caller_once_and_another_attempts_rings_nothing() {
        let (mut caller, peer) = ends(1);
        let attempt = Attempt::new(1);
        caller.begin_attempt(attempt);
        assert_eq!(peer.report_ready(Attempt::new(2)), Ok(None));
        assert_eq!(peer.bell.rings(), 0);
        assert_eq!(peer.report_ready(attempt), Ok(Some(())));
        assert_eq!(peer.bell.rings(), 1);
    }

    /// Each start attempt gives its peer room for the byte its ready report rings, on top of a slot's.
    #[test]
    fn a_start_attempt_adds_one_wake_byte_of_room() {
        let (mut caller, _) = ends(4);
        caller.begin_attempt(Attempt::new(1));
        assert_eq!(caller.record_wake_bytes(5), Ok(()));
        let (mut caller, _) = ends(4);
        caller.begin_attempt(Attempt::new(1));
        assert_eq!(
            caller.record_wake_bytes(6),
            Err(PeerFault::Wakes(WakeFloodError { bytes: 6, room: 5 }))
        );
    }

    #[test]
    fn a_failed_start_leaves_its_note_cut_to_the_note_area() {
        let (mut caller, peer) = ends(1);
        let attempt = Attempt::new(1);
        caller.begin_attempt(attempt);
        let note: Vec<u8> = (0..NOTE_BYTES as usize + 100).map(|i| i as u8).collect();
        peer.report_failed(attempt, &note);
        assert_eq!(
            caller.start_report(attempt),
            Ok(Some(StartOutcome::Failed(note[..NOTE_BYTES as usize].to_vec())))
        );
    }

    /// A failure an earlier attempt left is not the next attempt's: a replacement that starts is ready.
    #[test]
    fn an_earlier_attempts_failure_is_not_the_next_attempts() {
        let (mut caller, _) = ends(1);
        let first = Attempt::new(1);
        caller.begin_attempt(first);
        PeerEnd::new(Arc::clone(&caller.channel), SpyDoorbell::default(), StubParker(Ok(())))
            .report_failed(first, b"Traceback");
        let second = Attempt::new(2);
        caller.begin_attempt(second);
        assert_eq!(caller.start_report(second), Ok(None));
        let peer = || PeerEnd::new(Arc::clone(&caller.channel), SpyDoorbell::default(), StubParker(Ok(())));
        peer().report_failed(first, b"late");
        let replacement = peer();
        replacement.report_ready(first).unwrap();
        assert_eq!(
            caller.start_report(second),
            Ok(None),
            "a peer of an earlier attempt writes nothing"
        );
        replacement.report_ready(second).unwrap();
        assert_eq!(caller.start_report(second), Ok(Some(StartOutcome::Ready)));
    }

    #[test]
    fn a_start_report_no_peer_writes_is_refused_with_its_value() {
        let (mut caller, _) = ends(1);
        let attempt = Attempt::new(1);
        caller.begin_attempt(attempt);
        let area = &header(&caller).start;
        area.state.store(3, Relaxed);
        assert_eq!(caller.start_report(attempt), Err(StartReportError::UnknownState(3)));
        area.state.store(start::FAILED, Relaxed);
        area.note_len.store(NOTE_BYTES + 1, Relaxed);
        assert_eq!(
            caller.start_report(attempt),
            Err(StartReportError::NoteLength(SlotLengthError::PastSlot {
                len: NOTE_BYTES + 1,
                max: NOTE_BYTES
            }))
        );
    }

    /// A caller thread and a peer thread trade requests over heap memory, as two processes do over a mapping; under
    /// Miri the race detector checks that no slot is touched by both at once.
    #[test]
    fn a_caller_and_a_peer_thread_trade_requests_on_a_heap_channel() {
        const REQUESTS: u32 = if cfg!(miri) { 40 } else { 1000 };
        let (mut caller, mut peer) = ends(4);
        std::thread::scope(|s| {
            s.spawn(move || {
                for _ in 0..REQUESTS {
                    let request = loop {
                        if let Ok(WaitOutcome::Request(request)) = wait(&mut peer) {
                            break request;
                        }
                    };
                    let doubled: Vec<u8> = request.bytes().unwrap().iter().map(|b| b.wrapping_mul(2)).collect();
                    request.respond(|w| w.put(&doubled).unwrap()).unwrap();
                }
            });
            let (mut sent, mut read) = (0u32, 0u32);
            while read < REQUESTS {
                while sent < REQUESTS && caller.request_slot().is_some() {
                    send(&mut caller, &sent.to_le_bytes());
                    sent += 1;
                }
                for (seq, answer) in take_all(&mut caller).unwrap() {
                    assert_eq!(seq, Seq::new(read));
                    assert_eq!(answer, read.to_le_bytes().map(|b| b.wrapping_mul(2)));
                    read += 1;
                }
                std::thread::yield_now();
            }
        });
    }
}
