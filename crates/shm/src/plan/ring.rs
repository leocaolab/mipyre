//! Checks the counters the peer writes against the host's own window on the ring.
//!
//! The host keeps what it sent and what it read ([`Window`]) privately and never reads them back from shared
//! memory. The peer writes two counters: `published`, one past the last answer it published, and `taken`, `k + 1`
//! once it began request `k`. Either is believed only once it fits the window. Sequences wrap at 2^32, so every
//! comparison is a distance from `read`.
//!
//! The host reads `published` before `taken`, both with `Acquire`. A peer writes `taken = k + 1` before it publishes
//! answer `k`, so whatever it does between the two reads, `published ≤ taken ≤ sent` holds for what the host read;
//! `taken − published` is at most 1 only while the peer stands still, which is what a dead peer does.

use std::fmt;
use std::num::NonZeroU32;

/// A request's place in the ring's sequence; it wraps at 2^32, and request `k`'s answer is answer `k`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Seq(u32);

impl Seq {
    #[inline]
    pub const fn new(raw: u32) -> Self {
        Seq(raw)
    }

    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// Returns the request `n` after this one, across the wrap.
    #[inline]
    pub const fn after(self, n: u32) -> Seq {
        Seq(self.0.wrapping_add(n))
    }

    /// Returns how many requests lie from `earlier` up to this one, across the wrap.
    #[inline]
    pub const fn since(self, earlier: Seq) -> u32 {
        self.0.wrapping_sub(earlier.0)
    }

    /// Returns the slot this request (and its answer) takes in a ring of `depth` slots.
    #[inline]
    pub const fn slot_index(self, depth: RingDepth) -> usize {
        (self.0 & (depth.0.get() - 1)) as usize
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Counts slots per ring; it is a power of two, because only [`RingDepth::new`] makes one.
///
/// The sequence number is a `u32` that wraps, and only a depth that divides 2^32 keeps consecutive requests in
/// distinct slots across the wrap: at depth 3, requests `u32::MAX` and `0` share a slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingDepth(NonZeroU32);

impl RingDepth {
    pub fn new(depth: u32) -> Option<Self> {
        NonZeroU32::new(depth).filter(|d| d.is_power_of_two()).map(RingDepth)
    }

    #[inline]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// The host's own place on the ring: the answers before `read` it has read, the requests before `sent` it has sent.
/// The requests in `[read, sent)` hold their slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Window {
    read: Seq,
    sent: Seq,
}

impl Window {
    #[inline]
    pub const fn new(read: Seq, sent: Seq) -> Self {
        Window { read, sent }
    }

    #[inline]
    pub const fn read(&self) -> Seq {
        self.read
    }

    #[inline]
    pub const fn sent(&self) -> Seq {
        self.sent
    }

    /// Returns how many requests hold a slot: sent, and their answers not yet read.
    #[inline]
    pub const fn in_flight(&self) -> u32 {
        self.sent.since(self.read)
    }

    /// Returns whether one more request fits a ring of `depth` slots.
    #[inline]
    pub const fn has_room(&self, depth: RingDepth) -> bool {
        self.in_flight() < depth.get()
    }

    /// Returns the requests that hold a slot, `[read, sent)`.
    #[inline]
    pub const fn outstanding(&self) -> SeqRange {
        SeqRange {
            start: self.read,
            end: self.sent,
        }
    }
}

/// The requests from `start` up to, not including, `end`, across the wrap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeqRange {
    start: Seq,
    end: Seq,
}

impl SeqRange {
    #[inline]
    pub const fn start(&self) -> Seq {
        self.start
    }

    #[inline]
    pub const fn end(&self) -> Seq {
        self.end
    }

    #[inline]
    pub const fn len(&self) -> u32 {
        self.end.since(self.start)
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.start.0 == self.end.0
    }
}

/// How a dead peer left the requests the host had sent it: the ones it answered, the one it died running (if it
/// was running one) and the ones it never took. Together they are the window's
/// `[read, sent)`, each request in exactly one; only [`exit_split`] makes one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExitSplit {
    answered: SeqRange,
    running: Option<Seq>,
    handed: SeqRange,
}

impl ExitSplit {
    /// The requests whose answers the peer published before it died, `[read, published)`.
    #[inline]
    pub const fn answered(&self) -> SeqRange {
        self.answered
    }

    /// The request the peer began and did not answer; it is never handed to another peer.
    #[inline]
    pub const fn running(&self) -> Option<Seq> {
        self.running
    }

    /// The requests the peer never took, up to `sent`. They are given up like the running one, never handed to
    /// another peer: their bytes were in memory the dead peer could write.
    #[inline]
    pub const fn handed(&self) -> SeqRange {
        self.handed
    }
}

/// A counter the peer writes holds a value the ring's protocol cannot produce, measured against the host's own
/// window. Each variant carries every value it was compared with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PeerCounterError {
    #[error("the peer says it answered up to {published}, but the host sent up to {sent} and has read up to {read}")]
    AnswersPastSent { read: Seq, sent: Seq, published: Seq },
    /// `taken` read after `published` lies between `published` and `sent`; a peer that stands still (a dead one)
    /// has it at `published` (it answered all it took) or one past it (it died running `published`).
    #[error(
        "the peer says it took up to {taken} and answered up to {published}, but the host sent up to {sent} and \
         has read up to {read}"
    )]
    TakenOutOfRange {
        read: Seq,
        sent: Seq,
        published: Seq,
        taken: Seq,
    },
}

/// Returns how many answers past `read` the peer published, given the `published` counter it wrote; a count past
/// `sent` is a fault, since no answer comes before its request.
#[inline]
pub fn count_ready_answers(w: &Window, published: Seq) -> Result<u32, PeerCounterError> {
    let ready = published.since(w.read);
    if ready > w.in_flight() {
        return Err(PeerCounterError::AnswersPastSent {
            read: w.read,
            sent: w.sent,
            published,
        });
    }
    Ok(ready)
}

/// Returns the oldest request the peer took and had not answered when `published` was read: none when `taken`,
/// read after `published`, is at it, else request `published`. A live peer may have answered it since and moved on,
/// so `taken` may run up to `sent`; past it, or behind `published`, is a fault. This is the one place `taken` is
/// read.
#[inline]
pub fn running(w: &Window, published: Seq, taken: Seq) -> Result<Option<Seq>, PeerCounterError> {
    count_ready_answers(w, published)?;
    if taken.since(published) > w.sent.since(published) {
        return Err(PeerCounterError::TakenOutOfRange {
            read: w.read,
            sent: w.sent,
            published,
            taken,
        });
    }
    Ok((taken != published).then_some(published))
}

/// Splits the window by how a dead peer left it, from the two counters it wrote last. A dead peer stands still, so
/// past [`running`]'s check it has taken at most one request it did not answer.
pub fn exit_split(w: &Window, published: Seq, taken: Seq) -> Result<ExitSplit, PeerCounterError> {
    let running = running(w, published, taken)?;
    if taken.since(published) > 1 {
        return Err(PeerCounterError::TakenOutOfRange {
            read: w.read,
            sent: w.sent,
            published,
            taken,
        });
    }
    let handed_from = if running.is_some() {
        published.after(1)
    } else {
        published
    };
    Ok(ExitSplit {
        answered: SeqRange {
            start: w.read,
            end: published,
        },
        running,
        handed: SeqRange {
            start: handed_from,
            end: w.sent,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(read: u32, sent: u32) -> Window {
        Window::new(Seq(read), Seq(sent))
    }

    fn depth(d: u32) -> RingDepth {
        RingDepth::new(d).expect("a test asks for a power-of-two depth")
    }

    #[test]
    fn a_ring_depth_is_a_power_of_two() {
        assert_eq!([0, 3, 6, u32::MAX].map(RingDepth::new), [None; 4]);
        assert_eq!(
            [1, 2, 8, 1 << 31].map(|d| RingDepth::new(d).map(RingDepth::get)),
            [Some(1), Some(2), Some(8), Some(1 << 31)]
        );
        assert_eq!(
            u32::MAX % 3,
            0,
            "depth 3 would put request u32::MAX in request 0's slot"
        );
        let eight = depth(8);
        assert_ne!(Seq(u32::MAX).slot_index(eight), Seq(0).slot_index(eight));
    }

    #[test]
    fn a_window_has_room_until_its_depth_is_in_flight() {
        assert!(w(10, 17).has_room(depth(8)));
        assert!(!w(10, 18).has_room(depth(8)));
        assert!(w(u32::MAX - 2, 4).has_room(depth(8)), "across the wrap");
        assert!(!w(u32::MAX - 2, 5).has_room(depth(8)), "across the wrap");
    }

    #[test]
    fn answers_up_to_what_was_sent_are_ready() {
        assert_eq!(count_ready_answers(&w(10, 15), Seq(10)), Ok(0));
        assert_eq!(count_ready_answers(&w(10, 15), Seq(11)), Ok(1));
        assert_eq!(
            count_ready_answers(&w(10, 15), Seq(15)),
            Ok(5),
            "every request sent is answered"
        );
    }

    #[test]
    fn an_answer_past_what_was_sent_is_a_fault() {
        let fault = |sent, published| PeerCounterError::AnswersPastSent {
            read: Seq(10),
            sent: Seq(sent),
            published: Seq(published),
        };
        assert_eq!(count_ready_answers(&w(10, 15), Seq(16)), Err(fault(15, 16)));
        assert_eq!(
            count_ready_answers(&w(10, 10), Seq(11)),
            Err(fault(10, 11)),
            "nothing was sent"
        );
    }

    /// A peer that steps its counter back looks, unchecked, like 2^32 - 1 answers to read.
    #[test]
    fn an_answer_counter_moved_backwards_is_a_fault() {
        assert_eq!(
            count_ready_answers(&w(10, 15), Seq(9)),
            Err(PeerCounterError::AnswersPastSent {
                read: Seq(10),
                sent: Seq(15),
                published: Seq(9)
            })
        );
        assert_eq!(
            count_ready_answers(&w(0, 3), Seq(u32::MAX)),
            Err(PeerCounterError::AnswersPastSent {
                read: Seq(0),
                sent: Seq(3),
                published: Seq(u32::MAX)
            })
        );
    }

    #[test]
    fn answers_are_counted_across_the_wrap() {
        let (read, sent) = (u32::MAX - 1, 2);
        let fault = |published| PeerCounterError::AnswersPastSent {
            read: Seq(read),
            sent: Seq(sent),
            published: Seq(published),
        };
        assert_eq!(count_ready_answers(&w(read, sent), Seq(u32::MAX)), Ok(1));
        assert_eq!(count_ready_answers(&w(read, sent), Seq(0)), Ok(2));
        assert_eq!(
            count_ready_answers(&w(read, sent), Seq(2)),
            Ok(4),
            "every request sent is answered"
        );
        assert_eq!(count_ready_answers(&w(read, sent), Seq(3)), Err(fault(3)));
        assert_eq!(count_ready_answers(&w(read, sent), Seq(read - 1)), Err(fault(read - 1)));
    }

    #[test]
    fn a_dead_peer_with_taken_at_published_answered_all_it_took() {
        let split = exit_split(&w(10, 15), Seq(10), Seq(10)).unwrap();
        assert_eq!(split.running(), None);
        assert!(split.answered().is_empty());
        assert_eq!((split.handed().start(), split.handed().end()), (Seq(10), Seq(15)));
        let none_sent = exit_split(&w(10, 10), Seq(10), Seq(10)).unwrap();
        assert_eq!(none_sent.running(), None, "nothing was sent");
        assert!(none_sent.handed().is_empty());
    }

    #[test]
    fn a_dead_peer_with_taken_one_past_published_died_running_it() {
        let split = exit_split(&w(10, 15), Seq(10), Seq(11)).unwrap();
        assert_eq!(split.running(), Some(Seq(10)));
        assert_eq!((split.handed().start(), split.handed().end()), (Seq(11), Seq(15)));
        assert_eq!(
            exit_split(&w(10, 11), Seq(10), Seq(11)).unwrap().running(),
            Some(Seq(10))
        );
        let wrapped = exit_split(&w(u32::MAX, 3), Seq(u32::MAX), Seq(0)).unwrap();
        assert_eq!(wrapped.running(), Some(Seq(u32::MAX)), "across the wrap");
    }

    #[test]
    fn a_dead_peers_answers_come_before_the_one_it_ran() {
        let split = exit_split(&w(10, 15), Seq(12), Seq(13)).unwrap();
        assert_eq!((split.answered().start(), split.answered().end()), (Seq(10), Seq(12)));
        assert_eq!(split.running(), Some(Seq(12)));
        assert_eq!((split.handed().start(), split.handed().end()), (Seq(13), Seq(15)));
    }

    #[test]
    fn a_dead_peers_taken_anywhere_else_is_a_fault() {
        let fault = |read, sent, published, taken| {
            Err(PeerCounterError::TakenOutOfRange {
                read: Seq(read),
                sent: Seq(sent),
                published: Seq(published),
                taken: Seq(taken),
            })
        };
        assert_eq!(
            exit_split(&w(10, 10), Seq(10), Seq(11)),
            fault(10, 10, 10, 11),
            "took a request never sent"
        );
        assert_eq!(
            exit_split(&w(10, 15), Seq(10), Seq(12)),
            fault(10, 15, 10, 12),
            "took two without answering one"
        );
        assert_eq!(
            exit_split(&w(10, 15), Seq(10), Seq(9)),
            fault(10, 15, 10, 9),
            "moved backwards"
        );
        assert_eq!(
            exit_split(&w(0, 3), Seq(0), Seq(u32::MAX)),
            fault(0, 3, 0, u32::MAX),
            "moved backwards across the wrap"
        );
        assert_eq!(
            exit_split(&w(10, 12), Seq(12), Seq(13)),
            fault(10, 12, 12, 13),
            "answered all, then took one never sent"
        );
    }

    /// Between the two reads a live peer may answer the request it ran and take more, up to what was sent.
    #[test]
    fn a_live_peer_that_took_up_to_what_was_sent_runs_the_oldest_unanswered() {
        assert_eq!(running(&w(10, 15), Seq(12), Seq(12)), Ok(None));
        assert_eq!(running(&w(10, 15), Seq(12), Seq(13)), Ok(Some(Seq(12))));
        assert_eq!(
            running(&w(10, 15), Seq(12), Seq(15)),
            Ok(Some(Seq(12))),
            "took every request sent"
        );
        assert_eq!(
            running(&w(u32::MAX - 1, 2), Seq(u32::MAX), Seq(1)),
            Ok(Some(Seq(u32::MAX))),
            "across the wrap"
        );
    }

    #[test]
    fn a_live_peers_taken_past_what_was_sent_or_behind_published_is_a_fault() {
        let fault = |published, taken| {
            Err(PeerCounterError::TakenOutOfRange {
                read: Seq(10),
                sent: Seq(15),
                published: Seq(published),
                taken: Seq(taken),
            })
        };
        assert_eq!(
            running(&w(10, 15), Seq(12), Seq(16)),
            fault(12, 16),
            "took a request never sent"
        );
        assert_eq!(
            running(&w(10, 15), Seq(12), Seq(11)),
            fault(12, 11),
            "behind what it answered"
        );
        assert_eq!(
            running(&w(10, 15), Seq(15), Seq(16)),
            fault(15, 16),
            "answered all, then took one never sent"
        );
    }

    #[test]
    fn a_dead_peers_answers_past_what_was_sent_are_a_fault() {
        assert_eq!(
            exit_split(&w(10, 15), Seq(16), Seq(16)),
            Err(PeerCounterError::AnswersPastSent {
                read: Seq(10),
                sent: Seq(15),
                published: Seq(16)
            })
        );
    }
}

#[cfg(kani)]
mod proofs {
    use super::*;

    fn any_depth() -> RingDepth {
        let log2: u32 = kani::any();
        kani::assume(log2 < u32::BITS);
        RingDepth(NonZeroU32::new(1 << log2).expect("1 << log2 is not 0"))
    }

    /// Any window that fits a ring of `depth`.
    fn any_window(depth: RingDepth) -> Window {
        let (read, sent): (u32, u32) = (kani::any(), kani::any());
        let w = Window::new(Seq(read), Seq(sent));
        kani::assume(w.in_flight() <= depth.get());
        w
    }

    /// Every answer the host reads is one it sent and has not read, and no two of them share a slot.
    #[kani::proof]
    fn every_answer_read_is_one_the_host_sent() {
        let depth = any_depth();
        let w = any_window(depth);
        let published = Seq(kani::any());
        if let Ok(ready) = count_ready_answers(&w, published) {
            let (i, j): (u32, u32) = (kani::any(), kani::any());
            kani::assume(i < j && j < ready);
            assert!(w.read.after(i).since(w.read) < w.in_flight());
            assert!(w.read.after(j).since(w.read) < w.in_flight());
            assert_ne!(w.read.after(i).slot_index(depth), w.read.after(j).slot_index(depth));
        }
    }

    /// A dead peer's split covers `[read, sent)` in order, each request once; it was running a request exactly when
    /// `taken` is one past `published`, and that request is the one after the answered ones.
    #[kani::proof]
    fn a_dead_peers_split_covers_what_was_sent() {
        let w = any_window(any_depth());
        let (published, taken) = (Seq(kani::any()), Seq(kani::any()));
        if let Ok(s) = exit_split(&w, published, taken) {
            assert_eq!(s.answered().start(), w.read);
            assert_eq!(s.handed().end(), w.sent);
            let ran = u32::from(s.running().is_some());
            assert_eq!(s.answered().end().after(ran), s.handed().start());
            assert_eq!(
                u64::from(s.answered().len()) + u64::from(ran) + u64::from(s.handed().len()),
                u64::from(w.in_flight())
            );
            assert_eq!(s.running().is_some(), taken == published.after(1));
            if let Some(seq) = s.running() {
                assert_eq!(seq, s.answered().end());
            }
        }
    }

    /// No counters a correct live peer leaves, read `published` first, are a fault, and the request running is the
    /// oldest unanswered or none.
    #[kani::proof]
    fn a_live_peer_is_never_a_fault() {
        let w = any_window(any_depth());
        let (answered, took): (u32, u32) = (kani::any(), kani::any());
        kani::assume(answered <= took && took <= w.in_flight());
        let (published, taken) = (w.read.after(answered), w.read.after(took));
        assert_eq!(
            running(&w, published, taken),
            Ok((took != answered).then_some(published))
        );
    }

    /// Sending one more request when the window has room keeps at most `depth` in flight.
    #[kani::proof]
    fn room_never_lets_the_ring_pass_its_depth() {
        let depth = any_depth();
        let w = any_window(depth);
        if w.has_room(depth) {
            assert!(Window::new(w.read, w.sent.after(1)).in_flight() <= depth.get());
        }
    }
}
