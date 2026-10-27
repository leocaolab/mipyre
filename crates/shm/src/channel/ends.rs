//! Holds the channel's two ends: the host's, which keeps its own window on the ring, its last look at what the peer
//! published and whether the peer ever wrote what no correct peer writes; and the peer's, which waits for requests
//! and answers them, counting how it waited where it waits.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use super::header::{OwnCounter, NOTE_BYTES};
use super::slot::{PeerRequest, ReadyAnswers, RequestSlot};
use super::{Channel, Memory};
use crate::plan::ring::{self, ExitSplit, PeerCounterError, Seq, SeqRange, Window};
use crate::plan::slot_len::SlotLengthError;
use crate::plan::spin_window::SpinWindow;
use crate::plan::start::{self, decode_start, StartClaim, StartReportError};
use crate::plan::wakes::{spend_wakes, WakeFloodError};
use crate::protocol::doorbell::{park_slice, spin, Doorbell, ParkOutcome, Parker};
use crate::sync::{
    fence, AtomicU32,
    Ordering::{Acquire, Relaxed, Release, SeqCst},
};

/// The peer wrote a value no correct peer writes, so its memory is not to be trusted. The host's end keeps the first
/// one and reads no slot of the channel again until it takes over from the dead peer. It is a fact about the peer,
/// not the failure of one call: each variant holds the error that measured it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerFault {
    Counter(PeerCounterError),
    Length(SlotLengthError),
    Wakes(WakeFloodError),
}

/// The number the host picks for one start attempt, which the peer of that attempt reports under. The host never
/// picks a number twice for one channel, so a peer of an earlier attempt cannot report under a later one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Attempt(u32);

impl Attempt {
    /// Returns the attempt the host handed the peer as a number.
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

/// An answer a dead peer published, copied out when the host took over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadPeerAnswer {
    pub seq: Seq,
    pub message: Result<Bytes, SlotLengthError>,
}

/// A dead peer's counters do not fit the host's window; the host gave up every request it had sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the dead peer's counters are past belief ({cause}); requests {} to {} are given up", abandoned.start(), abandoned.end())]
pub struct DeadPeerError {
    #[source]
    pub cause: PeerCounterError,
    pub abandoned: SeqRange,
}

/// Says whether a host about to sleep may: only `Park` lets it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SleepVerdict {
    Park,
    /// An end has answers the host has not taken, or a fault it has not reported yet.
    Collect,
}

/// The host's end of a channel: it sends requests, reads answers and starts peers. Every run makes a new channel,
/// so a new end's zeroed window is true.
pub struct HostEnd<M: Memory, B> {
    channel: Arc<Channel<M>>,
    bell: B,
    window: Window,
    /// The peer's `published` counter as the host last read it and believed it.
    published: Seq,
    fault: Option<PeerFault>,
    /// The wake bytes the peer may still write: one per slot, and one per answer read up to `wakes_counted_to`.
    wake_room: u64,
    wakes_counted_to: Seq,
}

impl<M: Memory, B: Doorbell> HostEnd<M, B> {
    pub fn new(channel: Arc<Channel<M>>, bell: B) -> Self {
        let zero = Seq::new(0);
        let wake_room = channel.memory.geometry().depth().get().into();
        HostEnd {
            channel,
            bell,
            window: Window::new(zero, zero),
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
    pub fn answers(&mut self) -> Result<ReadyAnswers<'_, M>, PeerFault> {
        self.observe()?;
        Ok(ReadyAnswers::new(
            &self.channel,
            &mut self.window,
            self.published,
            &mut self.fault,
        ))
    }

    /// Returns the oldest request the peer took and had not answered when the host looked, read `published` first
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
        match ring::count_ready_answers(&self.window, published) {
            Ok(_) => {
                self.published = published;
                Ok(())
            }
            Err(e) => Err(self.keep_fault(PeerFault::Counter(e))),
        }
    }

    /// Adds the answers read since the last count to the room the peer has for wake bytes. The host counts often
    /// enough (each round of its poller) that fewer than 2^32 answers pass between two counts.
    #[inline]
    pub fn count_answers(&mut self) {
        let read = self.window.read();
        self.wake_room = self.wake_room.saturating_add(read.since(self.wakes_counted_to).into());
        self.wakes_counted_to = read;
    }

    /// Takes `bytes` the peer wrote into the host's wake pipe; bytes past what its answers leave room for are kept as
    /// its fault.
    ///
    /// # Errors
    ///
    /// The peer's fault, found now or kept from before.
    pub fn hear_wakes(&mut self, bytes: usize) -> Result<(), PeerFault> {
        if let Some(fault) = self.fault {
            return Err(fault);
        }
        self.count_answers();
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

    /// Starts start attempt `attempt`, a number the host picked and never picked before for this channel: clears the
    /// last attempt's report and writes the number. The lane hands the number to the peer it starts next, once any
    /// earlier attempt's peer has exited or never started. The attempt's ready report rings the host once, so its
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
    /// The counters do not fit the window: every request sent is given up, as [`HostEnd::abandon`] does.
    pub fn take_over(&mut self, answered: &mut Vec<DeadPeerAnswer>) -> Result<ExitSplit, DeadPeerError> {
        let memory = &self.channel.memory;
        let h = memory.header();
        let published = Seq::new(h.resp_head.load(Acquire));
        let taken = Seq::new(h.taken.load(Acquire));
        let split = match ring::exit_split(&self.window, published, taken) {
            Ok(split) => split,
            Err(cause) => {
                return Err(DeadPeerError {
                    cause,
                    abandoned: self.abandon(),
                })
            }
        };
        let done = split.answered();
        answered.extend((0..done.len()).map(|i| {
            let seq = done.start().after(i);
            let mut message = BytesMut::new();
            let len = memory
                .slot(memory.geometry().answer_slot(seq))
                .take_message(&mut message);
            DeadPeerAnswer {
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
    /// write the host's own `req_head` too, so it is written again: the next peer must not take a request the host
    /// never published.
    fn restart_at(&mut self, next: Seq) {
        let h = self.channel.memory.header();
        h.req_head.store(self.window.sent().get(), Release);
        h.taken.store(next.get(), Release);
        h.resp_head.store(next.get(), Release);
        self.window = Window::new(next, self.window.sent());
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

    /// Returns the host's own place on the ring: the answers it read, the requests it sent.
    #[inline]
    pub fn window(&self) -> Window {
        self.window
    }

    /// Says whether the peer published an answer the host has not taken; an end whose fault is kept says no, as
    /// [`sleep_all`] skips it. For spinning on: it believes nothing, so the answers are still read through
    /// [`HostEnd::answers`].
    #[inline]
    pub fn has_answers(&self) -> bool {
        self.fault.is_none() && self.channel.memory.header().resp_head.load(Acquire) != self.window.read().get()
    }

    /// Says whether [`HostEnd::answers`] has anything to hand out: an answer published and not taken, or the peer's
    /// fault, kept or new. A host that reads its ends often looks here first, so an end with nothing new costs it one
    /// load.
    #[inline]
    pub fn has_news(&self) -> bool {
        self.fault.is_some() || self.channel.memory.header().resp_head.load(Acquire) != self.window.read().get()
    }

    fn host_sleeping(&self) -> &AtomicU32 {
        &self.channel.memory.header().host_sleeping
    }
}

/// Says to every end's peer that the host is about to sleep, then reads, once, what each end's peer published,
/// keeping any fault on its end. The host sleeps only on `Park`; on `Collect` it reads the answers through
/// [`HostEnd::answers`] as always. An end whose fault was kept before is skipped, so a faulted peer does not keep
/// the host awake.
pub fn sleep_all<M: Memory, B: Doorbell>(ends: &mut [HostEnd<M, B>]) -> SleepVerdict {
    for end in ends.iter() {
        end.host_sleeping().store(1, Relaxed);
    }
    // One fence for every end: it pairs with each peer's publish (`protocol::doorbell::publish`), so of a peer that
    // published and the host that is about to sleep one sees the other.
    fence(SeqCst);
    let mut verdict = SleepVerdict::Park;
    for end in ends.iter_mut().filter(|e| e.fault.is_none()) {
        let found = end.observe().is_err() || end.published != end.window.read();
        if found {
            verdict = SleepVerdict::Collect;
        }
    }
    verdict
}

/// Says to every end's peer that the host is awake, on the ends [`sleep_all`] was given.
pub fn wake_all<M: Memory, B: Doorbell>(ends: &mut [HostEnd<M, B>]) {
    for end in ends.iter() {
        end.host_sleeping().store(0, Relaxed);
    }
}

/// How one call of [`PeerEnd::wait_for_request`] ended.
#[must_use = "a request taken is answered, or taken again"]
pub enum Wait<'p, M: Memory, B: Doorbell> {
    Request(PeerRequest<'p, M, B>),
    /// The slice passed with no request; the caller decides whether to wait again.
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
    /// Returns the end starting at the request after the last one taken: 0 in a new channel, or where the host's
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
    ) -> Result<Wait<'_, M, B>, P::Error> {
        let h = self.channel.memory.header();
        let seen = self.cursor.get();
        if h.req_head.load(Acquire) == seen {
            let counts = &h.peer_waits;
            if !self.waiting {
                counts.waits.add(1);
                self.waiting = true;
            }
            let spun = spin(window, now, || h.req_head.load(Acquire) != seen);
            counts.spin_rounds.add(spun.rounds.into());
            if !spun.ready {
                let sleeper = Sleeper {
                    parker: &self.parker,
                    parks: &counts.parks,
                };
                if park_slice(&h.req_head, &h.peer_sleeping, seen, slice_ns, &sleeper)? == ParkOutcome::SliceOver {
                    return Ok(Wait::SliceOver);
                }
            }
        }
        self.waiting = false;
        h.taken.store(seen.wrapping_add(1), Release);
        Ok(Wait::Request(PeerRequest::new(
            &self.channel,
            &self.bell,
            &mut self.cursor,
        )))
    }

    /// Reports that the peer of `attempt` started, then rings the host, which waits for its workers on the bell; a
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

    /// Reports that the peer of `attempt` could not start, leaving `note` (cut to [`NOTE_BYTES`]) for the host; a peer
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
    parks: &'p OwnCounter,
}

impl<P: Parker> Parker for Sleeper<'_, P> {
    type Error = P::Error;

    fn park(&self, word: &AtomicU32, expect: u32, slice_ns: u64) -> Result<(), P::Error> {
        self.parks.add(1);
        self.parker.park(word, expect, slice_ns)
    }
}

#[cfg(test)]
#[cfg(not(shm_model))]
mod tests {
    use std::convert::Infallible;

    use super::*;
    use crate::channel::memory::sealed::Access;
    use crate::channel::{HeapMemory, SlotRoomError, WaitTotals};
    use crate::plan::geometry::{Geometry, SlotBytes};
    use crate::plan::ring::RingDepth;

    /// A doorbell that counts its rings.
    #[derive(Default)]
    struct Bell(AtomicU32);

    impl Bell {
        fn rings(&self) -> u32 {
            self.0.load(Relaxed)
        }
    }

    impl Doorbell for Bell {
        type Outcome = ();
        type Error = Infallible;
        fn ring(&self, _: &AtomicU32) -> Result<(), Infallible> {
            self.0.fetch_add(1, Relaxed);
            Ok(())
        }
    }

    /// A parker whose every sleep ends at once, or is refused.
    struct Sleep(Result<(), &'static str>);

    impl Parker for Sleep {
        type Error = &'static str;
        fn park(&self, _: &AtomicU32, _: u32, _: u64) -> Result<(), &'static str> {
            std::thread::yield_now();
            self.0
        }
    }

    type Host = HostEnd<HeapMemory, Bell>;
    type Peer = PeerEnd<HeapMemory, Bell, Sleep>;

    fn geometry(depth: u32, slot_bytes: usize) -> Geometry {
        Geometry::new(RingDepth::new(depth).unwrap(), SlotBytes::new(slot_bytes).unwrap()).unwrap()
    }

    fn ends(depth: u32) -> (Host, Peer) {
        let channel = Arc::new(Channel::new(HeapMemory::new(&geometry(depth, 64))));
        (
            HostEnd::new(Arc::clone(&channel), Bell::default()),
            PeerEnd::new(channel, Bell::default(), Sleep(Ok(()))),
        )
    }

    fn header(host: &Host) -> &super::super::header::ChannelHeader {
        host.channel.memory.header()
    }

    fn send(host: &mut Host, message: &[u8]) -> Seq {
        let mut slot = host.request_slot().expect("the ring has room");
        let seq = slot.seq();
        slot.write(|w| w.put(message)).unwrap();
        slot.publish().unwrap();
        seq
    }

    fn wait(peer: &mut Peer) -> Result<Wait<'_, HeapMemory, Bell>, &'static str> {
        peer.wait_for_request(SpinWindow::until(0), || 0, 1)
    }

    /// Takes the next request and answers it with its own bytes followed by `suffix`.
    fn echo(peer: &mut Peer, suffix: &[u8]) -> Seq {
        let Ok(Wait::Request(request)) = wait(peer) else {
            panic!("a request was sent")
        };
        let (seq, mut answer) = (request.seq(), request.bytes().unwrap().to_vec());
        answer.extend_from_slice(suffix);
        request.answer(|w| w.put(&answer).unwrap()).unwrap();
        seq
    }

    fn take_all(host: &mut Host) -> Result<Vec<(Seq, Vec<u8>)>, PeerFault> {
        let mut ready = host.answers()?;
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
        let (mut host, mut peer) = ends(4);
        assert_eq!(send(&mut host, b"ping"), Seq::new(0));
        assert_eq!(echo(&mut peer, b"!"), Seq::new(0));
        assert_eq!(take_all(&mut host), Ok(vec![(Seq::new(0), b"ping!".to_vec())]));
        assert_eq!(host.in_flight(), 0);
        assert_eq!(header(&host).taken.load(Relaxed), 1);
    }

    #[test]
    fn a_full_ring_gives_no_request_slot() {
        let (mut host, _peer) = ends(2);
        send(&mut host, b"a");
        send(&mut host, b"b");
        assert!(host.request_slot().is_none());
        assert_eq!(host.in_flight(), 2);
    }

    #[test]
    fn a_request_slot_dropped_unpublished_sends_nothing() {
        let (mut host, _peer) = ends(2);
        drop(host.request_slot().unwrap());
        assert_eq!((host.in_flight(), header(&host).req_head.load(Relaxed)), (0, 0));
    }

    #[test]
    fn a_publish_rings_the_peer_only_when_it_said_it_sleeps() {
        let (mut host, _peer) = ends(4);
        assert_eq!(host.request_slot().unwrap().publish(), Ok(None));
        header(&host).peer_sleeping.store(1, Relaxed);
        assert_eq!(host.request_slot().unwrap().publish(), Ok(Some(())));
        assert_eq!(host.bell.rings(), 1);
    }

    /// A slot published unwritten holds an empty message, never the length of the request before it in that slot.
    #[test]
    fn a_request_published_unwritten_is_empty_not_the_slots_last_request() {
        let (mut host, mut peer) = ends(1);
        send(&mut host, b"earlier");
        echo(&mut peer, b"");
        take_all(&mut host).unwrap();
        host.request_slot().unwrap().publish().unwrap();
        let Ok(Wait::Request(request)) = wait(&mut peer) else {
            panic!("a request was sent")
        };
        assert_eq!(request.bytes().map(|b| b.to_vec()), Ok(Vec::new()));
    }

    #[test]
    fn a_message_that_fits_the_slot_is_written_and_one_byte_more_is_refused() {
        let (mut host, _peer) = ends(1);
        let room = geometry(1, 64).max_message();
        let mut slot = host.request_slot().unwrap();
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
        let (mut host, mut peer) = ends(4);
        send(&mut host, b"x");
        let Ok(Wait::Request(request)) = wait(&mut peer) else {
            panic!("a request was sent")
        };
        assert_eq!(request.seq(), Seq::new(0));
        assert_eq!(
            header(&host).taken.load(Relaxed),
            1,
            "taken is written before the request is handed out"
        );
        assert_eq!(host.channel.peer_waits(), WaitTotals::default());
    }

    #[test]
    fn a_wait_counts_once_and_each_slice_counts_a_park() {
        let (host, mut peer) = ends(4);
        assert!(matches!(wait(&mut peer), Ok(Wait::SliceOver)));
        assert!(matches!(wait(&mut peer), Ok(Wait::SliceOver)));
        assert_eq!(
            host.channel.peer_waits(),
            WaitTotals {
                waits: 1,
                spin_rounds: 0,
                parks: 2,
                wakes: 0
            }
        );
        assert_eq!(header(&host).peer_sleeping.load(Relaxed), 0);
    }

    #[test]
    fn a_refused_sleep_is_the_parkers_error() {
        let channel = Arc::new(Channel::new(HeapMemory::new(&geometry(4, 64))));
        let mut peer = PeerEnd::new(channel, Bell::default(), Sleep(Err("refused")));
        assert!(matches!(
            peer.wait_for_request(SpinWindow::until(0), || 0, 1),
            Err("refused")
        ));
    }

    /// The wait goes on after the refused sleep, so the call after it counts no second wait.
    #[test]
    fn a_wait_counts_once_across_a_refused_sleep() {
        struct RefuseFirst(std::cell::Cell<bool>);
        impl Parker for RefuseFirst {
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
            Bell::default(),
            RefuseFirst(std::cell::Cell::new(true)),
        );
        assert!(matches!(
            peer.wait_for_request(SpinWindow::until(0), || 0, 1),
            Err("refused")
        ));
        assert!(matches!(
            peer.wait_for_request(SpinWindow::until(0), || 0, 1),
            Ok(Wait::SliceOver)
        ));
        assert_eq!(
            channel.peer_waits(),
            WaitTotals {
                waits: 1,
                spin_rounds: 0,
                parks: 2,
                wakes: 0
            }
        );
    }

    #[test]
    fn a_request_whose_length_passes_the_slot_is_refused_with_both_values() {
        let (mut host, mut peer) = ends(1);
        let max = geometry(1, 64).max_message();
        let mut slot = host.request_slot().unwrap();
        slot.write(|w| w.put(&vec![7; max as usize])).unwrap();
        slot.publish().unwrap();
        {
            let Ok(Wait::Request(request)) = wait(&mut peer) else {
                panic!("a request was sent")
            };
            assert_eq!(request.bytes().map(|b| b.len()), Ok(max as usize));
        }
        let at = geometry(1, 64).request_slot(Seq::new(0));
        host.channel.memory.slot(at).len_word().store(max + 1, Relaxed);
        let Ok(Wait::Request(request)) = wait(&mut peer) else {
            panic!("an unanswered request is taken again")
        };
        assert_eq!(
            request.bytes().map(|b| b.len()),
            Err(SlotLengthError::PastSlot { len: max + 1, max })
        );
    }

    #[test]
    fn an_answer_rings_the_host_only_when_it_said_it_sleeps_and_counts_the_ring() {
        let (mut host, mut peer) = ends(4);
        send(&mut host, b"a");
        send(&mut host, b"b");
        let Ok(Wait::Request(request)) = wait(&mut peer) else {
            panic!()
        };
        assert_eq!(request.answer(|_| {}), Ok(None));
        header(&host).host_sleeping.store(1, Relaxed);
        let Ok(Wait::Request(request)) = wait(&mut peer) else {
            panic!()
        };
        assert_eq!(request.answer(|_| {}), Ok(Some(())));
        assert_eq!((peer.bell.rings(), host.channel.peer_waits().wakes), (1, 1));
    }

    #[test]
    fn answers_up_to_what_was_sent_are_read_and_past_it_the_peer_is_at_fault_for_good() {
        let (mut host, mut peer) = ends(8);
        for _ in 0..5 {
            send(&mut host, b"r");
        }
        echo(&mut peer, b"");
        assert_eq!(take_all(&mut host).map(|a| a.len()), Ok(1));
        header(&host).resp_head.store(5, Relaxed);
        assert_eq!(host.answers().map(|_| ()), Ok(()), "every request sent answered");
        header(&host).resp_head.store(6, Relaxed);
        let fault = PeerFault::Counter(PeerCounterError::AnswersPastSent {
            read: Seq::new(1),
            sent: Seq::new(5),
            published: Seq::new(6),
        });
        assert_eq!(host.answers().err(), Some(fault));
        header(&host).resp_head.store(2, Relaxed);
        assert_eq!(
            host.answers().err(),
            Some(fault),
            "the fault is kept, and the counter is not read again"
        );
        assert!(host.request_slot().is_none());
        assert!(!host.has_answers(), "a faulted end has nothing to spin for");
        assert!(host.has_news(), "but its fault is still to be read");
    }

    /// A peer may write a wake byte per slot and one per answer the host read; a byte past that is its fault, kept
    /// as any other until the host takes over, and the next peer starts with a slot's room each again.
    #[test]
    fn wake_bytes_up_to_the_answers_read_and_the_slots_are_heard_and_one_past_them_is_the_peers_fault() {
        let (mut host, mut peer) = ends(4);
        for _ in 0..2 {
            send(&mut host, b"r");
            echo(&mut peer, b"");
        }
        assert_eq!(take_all(&mut host).map(|a| a.len()), Ok(2));
        assert_eq!(host.hear_wakes(5), Ok(()));
        assert_eq!(host.hear_wakes(1), Ok(()));
        let fault = PeerFault::Wakes(WakeFloodError { bytes: 1, room: 0 });
        assert_eq!(host.hear_wakes(1), Err(fault));
        assert_eq!(host.answers().err(), Some(fault), "the fault is kept");
        assert!(host.has_news() && host.request_slot().is_none());
        host.take_over(&mut Vec::new()).unwrap();
        assert_eq!(host.hear_wakes(4), Ok(()));
        assert_eq!(
            host.hear_wakes(1),
            Err(PeerFault::Wakes(WakeFloodError { bytes: 1, room: 0 }))
        );
    }

    /// The spin's look says yes once an answer is published and no once it is taken.
    #[test]
    fn an_end_has_answers_from_a_publish_until_they_are_taken() {
        let (mut host, mut peer) = ends(4);
        send(&mut host, b"r");
        assert!(!host.has_answers() && !host.has_news());
        echo(&mut peer, b"");
        assert!(host.has_answers() && host.has_news());
        assert_eq!(take_all(&mut host).map(|a| a.len()), Ok(1));
        assert!(!host.has_answers() && !host.has_news());
        assert_eq!(host.window(), Window::new(Seq::new(1), Seq::new(1)));
    }

    #[test]
    fn an_answer_taken_is_the_hosts_own_whatever_the_peer_writes_after() {
        let (mut host, mut peer) = ends(2);
        send(&mut host, b"abc");
        echo(&mut peer, b"");
        let mut buf = BytesMut::new();
        assert_eq!(host.answers().unwrap().take_next(&mut buf), Ok(Some(Seq::new(0))));
        let at = geometry(2, 64).answer_slot(Seq::new(0));
        assert_eq!(host.channel.memory.slot(at).copy_in(0, b"XYZ"), Ok(3));
        assert_eq!(&buf[..], b"abc");
    }

    #[test]
    fn an_answer_up_to_the_slot_is_taken_and_one_byte_more_is_the_peers_fault() {
        let (mut host, mut peer) = ends(4);
        let max = geometry(4, 64).max_message();
        send(&mut host, b"a");
        send(&mut host, b"b");
        echo(&mut peer, b"");
        echo(&mut peer, b"");
        let ring = geometry(4, 64);
        host.channel
            .memory
            .slot(ring.answer_slot(Seq::new(0)))
            .len_word()
            .store(max, Relaxed);
        host.channel
            .memory
            .slot(ring.answer_slot(Seq::new(1)))
            .len_word()
            .store(max + 1, Relaxed);
        let mut ready = host.answers().unwrap();
        let mut buf = BytesMut::new();
        assert_eq!(ready.take_next(&mut buf), Ok(Some(Seq::new(0))));
        assert_eq!(buf.len(), max as usize);
        let fault = PeerFault::Length(SlotLengthError::PastSlot { len: max + 1, max });
        assert_eq!(ready.take_next(&mut buf), Err(fault));
        assert_eq!(buf.len(), max as usize, "a refused answer copies nothing");
        assert_eq!(ready.take_next(&mut buf), Err(fault));
        assert!(host.request_slot().is_none());
        assert_eq!(host.answers().err(), Some(fault));
    }

    #[test]
    fn answers_left_when_the_handle_is_dropped_come_with_the_next_one() {
        let (mut host, mut peer) = ends(4);
        send(&mut host, b"a");
        send(&mut host, b"b");
        echo(&mut peer, b"");
        echo(&mut peer, b"");
        assert_eq!(
            host.answers().unwrap().take_next(&mut BytesMut::new()),
            Ok(Some(Seq::new(0)))
        );
        assert_eq!(take_all(&mut host), Ok(vec![(Seq::new(1), b"b".to_vec())]));
    }

    /// Between the host's read of `published` and of `taken` a live peer may answer and take more; that is no
    /// fault. `taken` past `sent` is.
    #[test]
    fn a_live_peer_ahead_of_what_the_host_saw_runs_and_one_past_what_was_sent_is_at_fault() {
        let (mut host, _peer) = ends(8);
        for _ in 0..5 {
            send(&mut host, b"r");
        }
        let channel = Arc::clone(&host.channel);
        let h = channel.memory.header();
        assert_eq!(host.running(), Ok(None));
        h.taken.store(1, Relaxed);
        assert_eq!(host.running(), Ok(Some(Seq::new(0))));
        h.resp_head.store(2, Relaxed);
        h.taken.store(5, Relaxed);
        assert_eq!(host.running(), Ok(Some(Seq::new(2))));
        h.taken.store(6, Relaxed);
        let fault = PeerFault::Counter(PeerCounterError::TakenOutOfRange {
            read: Seq::new(0),
            sent: Seq::new(5),
            published: Seq::new(2),
            taken: Seq::new(6),
        });
        assert_eq!(host.running(), Err(fault));
        assert!(host.request_slot().is_none());
    }

    fn two_ends() -> Vec<Host> {
        (0..2).map(|_| ends(4).0).collect()
    }

    #[test]
    fn a_host_parks_only_when_no_end_has_an_answer_waiting() {
        let mut hosts = two_ends();
        assert_eq!(sleep_all(&mut hosts), SleepVerdict::Park);
        assert!(hosts.iter().all(|h| header(h).host_sleeping.load(Relaxed) == 1));
        send(&mut hosts[1], b"x");
        header(&hosts[1]).taken.store(1, Relaxed);
        header(&hosts[1]).resp_head.store(1, Relaxed);
        assert_eq!(
            sleep_all(&mut hosts),
            SleepVerdict::Collect,
            "only the second end has an answer"
        );
        wake_all(&mut hosts);
        assert!(hosts.iter().all(|h| header(h).host_sleeping.load(Relaxed) == 0));
        assert_eq!(take_all(&mut hosts[1]).map(|a| a.len()), Ok(1));
        assert_eq!(sleep_all(&mut hosts), SleepVerdict::Park);
    }

    /// A fault keeps the host awake once, to report it, and never again (a faulted peer must not spin the host).
    #[test]
    fn a_fault_keeps_the_host_awake_once() {
        let mut hosts = two_ends();
        header(&hosts[0]).resp_head.store(9, Relaxed);
        assert_eq!(sleep_all(&mut hosts), SleepVerdict::Collect);
        assert!(hosts[0].answers().is_err());
        assert_eq!(sleep_all(&mut hosts), SleepVerdict::Park);
    }

    /// The peer answered two, took the third and died: the two answers come out whole, the third is the one it ran,
    /// the last two it never took are given up with it, and the next peer starts after all five.
    #[test]
    fn a_take_over_copies_the_dead_peers_answers_and_gives_up_the_rest() {
        let (mut host, mut peer) = ends(8);
        for i in 0..5u8 {
            send(&mut host, &[i]);
        }
        echo(&mut peer, b"a");
        echo(&mut peer, b"b");
        let Ok(Wait::Request(_running)) = wait(&mut peer) else {
            panic!()
        };
        drop(peer);
        let mut answered = Vec::new();
        let split = host.take_over(&mut answered).unwrap();
        assert_eq!(split.running(), Some(Seq::new(2)));
        assert_eq!(
            (split.handed().start(), split.handed().end()),
            (Seq::new(3), Seq::new(5))
        );
        assert_eq!(
            answered,
            [
                DeadPeerAnswer {
                    seq: Seq::new(0),
                    message: Ok(Bytes::from_static(b"\x00a"))
                },
                DeadPeerAnswer {
                    seq: Seq::new(1),
                    message: Ok(Bytes::from_static(b"\x01b"))
                },
            ]
        );
        assert_eq!(
            (header(&host).taken.load(Relaxed), header(&host).resp_head.load(Relaxed)),
            (5, 5)
        );
        assert_eq!(host.in_flight(), 0);
        assert_eq!(take_all(&mut host), Ok(vec![]));
        let mut next = PeerEnd::new(Arc::clone(&host.channel), Bell::default(), Sleep(Ok(())));
        assert!(
            matches!(wait(&mut next), Ok(Wait::SliceOver)),
            "the next peer was handed a request the dead one could write"
        );
        send(&mut host, b"new");
        assert_eq!(
            echo(&mut next, b""),
            Seq::new(5),
            "the next peer starts after every request the dead one held"
        );
    }

    #[test]
    fn a_dead_peers_answer_past_its_slot_is_refused_and_the_rest_are_copied() {
        let (mut host, mut peer) = ends(4);
        send(&mut host, b"a");
        send(&mut host, b"b");
        echo(&mut peer, b"");
        echo(&mut peer, b"");
        let ring = geometry(4, 64);
        let max = ring.max_message();
        host.channel
            .memory
            .slot(ring.answer_slot(Seq::new(0)))
            .len_word()
            .store(max + 1, Relaxed);
        let mut answered = Vec::new();
        assert!(host.take_over(&mut answered).is_ok());
        assert_eq!(
            answered[0].message,
            Err(SlotLengthError::PastSlot { len: max + 1, max })
        );
        assert_eq!(answered[1].message, Ok(Bytes::from_static(b"b")));
    }

    #[test]
    fn a_take_over_after_a_kept_fault_starts_clean() {
        let (mut host, mut peer) = ends(4);
        send(&mut host, b"a");
        echo(&mut peer, b"");
        let max = geometry(4, 64).max_message();
        host.channel
            .memory
            .slot(geometry(4, 64).answer_slot(Seq::new(0)))
            .len_word()
            .store(max + 1, Relaxed);
        assert!(take_all(&mut host).is_err());
        let mut answered = Vec::new();
        assert!(host.take_over(&mut answered).is_ok());
        assert!(answered[0].message.is_err());
        assert!(host.request_slot().is_some());
    }

    #[test]
    fn counters_past_belief_at_a_take_over_give_up_every_request_sent() {
        let (mut host, _peer) = ends(8);
        for _ in 0..3 {
            send(&mut host, b"r");
        }
        header(&host).taken.store(2, Relaxed);
        let mut answered = Vec::new();
        let err = host.take_over(&mut answered).unwrap_err();
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
        assert_eq!(host.in_flight(), 0);
        assert_eq!(
            (header(&host).taken.load(Relaxed), header(&host).resp_head.load(Relaxed)),
            (3, 3)
        );
        assert!(host.request_slot().is_some());
    }

    /// The dead peer wrote the host's `req_head` past what was sent; the next peer takes only what the host sent.
    #[test]
    fn a_take_over_or_abandon_writes_back_the_requests_the_host_sent() {
        for give_up in [false, true] {
            let (mut host, _dead) = ends(8);
            send(&mut host, b"a");
            header(&host).req_head.store(5, Relaxed);
            if give_up {
                host.abandon();
            } else {
                host.take_over(&mut Vec::new()).unwrap();
            }
            assert_eq!(header(&host).req_head.load(Relaxed), 1, "abandon: {give_up}");
            let mut next = PeerEnd::new(Arc::clone(&host.channel), Bell::default(), Sleep(Ok(())));
            assert!(matches!(wait(&mut next), Ok(Wait::SliceOver)), "abandon: {give_up}");
        }
    }

    #[test]
    fn abandoning_gives_up_what_was_in_flight() {
        let (mut host, mut peer) = ends(8);
        for _ in 0..3 {
            send(&mut host, b"r");
        }
        echo(&mut peer, b"");
        take_all(&mut host).unwrap();
        let given_up = host.abandon();
        assert_eq!((given_up.start(), given_up.end()), (Seq::new(1), Seq::new(3)));
        assert_eq!(host.in_flight(), 0);
    }

    #[test]
    fn a_start_attempt_reports_what_its_peer_said_and_nothing_before() {
        let (mut host, peer) = ends(1);
        let attempt = Attempt::new(1);
        host.begin_attempt(attempt);
        assert_eq!(host.start_report(attempt), Ok(None));
        peer.report_ready(attempt).unwrap();
        assert_eq!(host.start_report(attempt), Ok(Some(StartOutcome::Ready)));
        assert_eq!(
            host.start_report(Attempt::new(attempt.get() + 1)),
            Ok(None),
            "another attempt's report"
        );
    }

    #[test]
    fn a_ready_report_rings_the_host_once_and_another_attempts_rings_nothing() {
        let (mut host, peer) = ends(1);
        let attempt = Attempt::new(1);
        host.begin_attempt(attempt);
        assert_eq!(peer.report_ready(Attempt::new(2)), Ok(None));
        assert_eq!(peer.bell.rings(), 0);
        assert_eq!(peer.report_ready(attempt), Ok(Some(())));
        assert_eq!(peer.bell.rings(), 1);
    }

    /// Each start attempt gives its peer room for the byte its ready report rings, on top of a slot's.
    #[test]
    fn a_start_attempt_adds_one_wake_byte_of_room() {
        let (mut host, _) = ends(4);
        host.begin_attempt(Attempt::new(1));
        assert_eq!(host.hear_wakes(5), Ok(()));
        let (mut host, _) = ends(4);
        host.begin_attempt(Attempt::new(1));
        assert_eq!(
            host.hear_wakes(6),
            Err(PeerFault::Wakes(WakeFloodError { bytes: 6, room: 5 }))
        );
    }

    #[test]
    fn a_failed_start_leaves_its_note_cut_to_the_note_area() {
        let (mut host, peer) = ends(1);
        let attempt = Attempt::new(1);
        host.begin_attempt(attempt);
        let note: Vec<u8> = (0..NOTE_BYTES as usize + 100).map(|i| i as u8).collect();
        peer.report_failed(attempt, &note);
        assert_eq!(
            host.start_report(attempt),
            Ok(Some(StartOutcome::Failed(note[..NOTE_BYTES as usize].to_vec())))
        );
    }

    /// A failure an earlier attempt left is not the next attempt's: a replacement that starts is ready.
    #[test]
    fn an_earlier_attempts_failure_is_not_the_next_attempts() {
        let (mut host, _) = ends(1);
        let first = Attempt::new(1);
        host.begin_attempt(first);
        PeerEnd::new(Arc::clone(&host.channel), Bell::default(), Sleep(Ok(()))).report_failed(first, b"Traceback");
        let second = Attempt::new(2);
        host.begin_attempt(second);
        assert_eq!(host.start_report(second), Ok(None));
        let peer = || PeerEnd::new(Arc::clone(&host.channel), Bell::default(), Sleep(Ok(())));
        peer().report_failed(first, b"late");
        let replacement = peer();
        replacement.report_ready(first).unwrap();
        assert_eq!(
            host.start_report(second),
            Ok(None),
            "a peer of an earlier attempt writes nothing"
        );
        replacement.report_ready(second).unwrap();
        assert_eq!(host.start_report(second), Ok(Some(StartOutcome::Ready)));
    }

    #[test]
    fn a_start_report_no_peer_writes_is_refused_with_its_value() {
        let (mut host, _) = ends(1);
        let attempt = Attempt::new(1);
        host.begin_attempt(attempt);
        let area = &header(&host).start;
        area.state.store(3, Relaxed);
        assert_eq!(host.start_report(attempt), Err(StartReportError::UnknownState(3)));
        area.state.store(start::FAILED, Relaxed);
        area.note_len.store(NOTE_BYTES + 1, Relaxed);
        assert_eq!(
            host.start_report(attempt),
            Err(StartReportError::NoteLength(SlotLengthError::PastSlot {
                len: NOTE_BYTES + 1,
                max: NOTE_BYTES
            }))
        );
    }

    /// A host thread and a peer thread trade requests over heap memory, as two processes do over a mapping; under
    /// Miri the race detector checks that no slot is touched by both at once.
    #[test]
    fn a_host_and_a_peer_thread_trade_requests_on_a_heap_channel() {
        const REQUESTS: u32 = if cfg!(miri) { 40 } else { 1000 };
        let (mut host, mut peer) = ends(4);
        std::thread::scope(|s| {
            s.spawn(move || {
                for _ in 0..REQUESTS {
                    let request = loop {
                        if let Ok(Wait::Request(request)) = wait(&mut peer) {
                            break request;
                        }
                    };
                    let doubled: Vec<u8> = request.bytes().unwrap().iter().map(|b| b.wrapping_mul(2)).collect();
                    request.answer(|w| w.put(&doubled).unwrap()).unwrap();
                }
            });
            let (mut sent, mut read) = (0u32, 0u32);
            while read < REQUESTS {
                while sent < REQUESTS && host.request_slot().is_some() {
                    send(&mut host, &sent.to_le_bytes());
                    sent += 1;
                }
                for (seq, answer) in take_all(&mut host).unwrap() {
                    assert_eq!(seq, Seq::new(read));
                    assert_eq!(answer, read.to_le_bytes().map(|b| b.wrapping_mul(2)));
                    read += 1;
                }
                std::thread::yield_now();
            }
        });
    }
}
