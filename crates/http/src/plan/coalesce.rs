//! Decides when the bytes an HTTP/1 connection writes are kept and when they go out; the writes
//! themselves are [`crate::io::coalesce`]'s.

use std::io::IoSlice;

/// The most bytes a connection keeps before writing them out; a write of this size or more is
/// never copied.
pub const COALESCE_LIMIT: usize = 64 * 1024;

/// An emptied buffer keeps its allocation up to this size, so a keep-alive connection does not
/// allocate per batch. It is as large as a batch can be, because a smaller cap frees and regrows
/// the buffer on every batch above it.
pub const KEEP_CAPACITY: usize = COALESCE_LIMIT;

/// The first allocation is enough for a batch of small responses.
pub const FIRST_CAPACITY: usize = 4096;

/// What an HTTP/2 connection sends first (RFC 9113 §3.4).
pub const H2_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Protocol {
    Sniffing(PartialPreface),
    Http1,
    Http2,
}

/// The count of bytes received so far that match the HTTP/2 preface. It is always shorter than
/// the preface, because only [`PartialPreface::EMPTY`] and [`sniffed`] make one.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PartialPreface(u8);

impl PartialPreface {
    pub const EMPTY: Self = PartialPreface(0);

    /// Returns the part of the preface still to come, which is at least one byte.
    #[inline]
    fn rest(self) -> &'static [u8] {
        &H2_PREFACE[usize::from(self.0)..]
    }
}

/// Says whether the connection future is being polled; inside a poll, the poll's end writes the
/// kept bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PollPhase {
    /// The IO is between polls or belongs to an upgraded stream, so a flush always writes.
    Outside,
    /// `blocked` says the inner IO returned `Pending` in this poll, so the task is already
    /// registered for writability and the poll's end does not try again.
    Inside { blocked: bool },
}

/// Says what a write does: when [`Self::drain_first`], the kept bytes go out first so order holds,
/// then [`Self::action`] runs. Only [`write()`] makes one, so a [`WriteAction::Keep`] is always under
/// [`COALESCE_LIMIT`]. `drain_first` sits beside the action rather than in its variants because
/// the call site then tests one register.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteDecision {
    drain_first: bool,
    action: WriteAction,
}

impl WriteDecision {
    #[inline]
    pub fn drain_first(self) -> bool {
        self.drain_first
    }

    #[inline]
    pub fn action(self) -> WriteAction {
        self.action
    }

    #[inline]
    const fn direct(drain_first: bool) -> Self {
        WriteDecision {
            drain_first,
            action: WriteAction::Direct,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteAction {
    /// HTTP/2 bytes go to the inner IO as they are, and nothing is kept or tracked.
    Pass,
    /// The bytes go to the inner IO uncopied, as one vectored write.
    Direct,
    /// `total` is under [`COALESCE_LIMIT`], so it fits a `u32` and the decision fits two registers.
    Keep { total: u32 },
}

/// Holds what the IO has learned about one connection that decides when its writes go out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionFacts {
    pub protocol: Protocol,
    pub poll: PollPhase,
    /// It is set while no read has returned `Pending` since the last one delivered bytes, so hyper
    /// may still hold unanswered requests.
    pub input_unread: bool,
    /// It is fixed for a stream; an inner IO that is not vectored writes one slice per call.
    pub vectored: bool,
}

/// Decides a write of `bufs` on a connection that keeps `kept` bytes, `draining` while their
/// write-out is under way.
///
/// A write is kept only when later responses may share its write: inside a poll while requests
/// may be unread, or behind bytes already kept. One that would take the kept bytes past
/// [`COALESCE_LIMIT`] drains them first. This is one function taking `conn` by reference and
/// reading it into locals, because splitting it by `kept` or passing `conn` by value measured
/// more instructions at the call site.
#[inline]
pub fn write(
    conn: &ConnectionFacts,
    kept: usize,
    draining: bool,
    bufs: &[IoSlice<'_>],
) -> WriteDecision {
    let ConnectionFacts { protocol, poll, input_unread, vectored } = *conn;
    if protocol == Protocol::Http2 {
        // Bytes kept while the preface was still being sniffed go out first.
        return WriteDecision {
            drain_first: draining || kept > 0,
            action: WriteAction::Pass,
        };
    }
    // Each branch below compares without a sum, so no input can overflow it.
    if draining || kept == 0 {
        // A write-out under way finishes before this write, and its written prefix is freed
        // before the buffer grows, so the buffer counts as empty.
        if !draining {
            let batching = poll != PollPhase::Outside && input_unread;
            if !batching && (vectored || bufs.len() == 1) {
                return WriteDecision::direct(false);
            }
        }
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        return if total >= COALESCE_LIMIT {
            WriteDecision::direct(draining)
        } else {
            keep(draining, total)
        };
    }
    let total: usize = bufs.iter().map(|b| b.len()).sum();
    if total >= COALESCE_LIMIT {
        return WriteDecision::direct(true);
    }
    // `total < COALESCE_LIMIT` here, so the subtraction cannot wrap.
    keep(kept > COALESCE_LIMIT - total, total)
}

/// Keeps a write of `total` bytes; callers have checked it is under [`COALESCE_LIMIT`], so it
/// fits a `u32`.
#[inline]
fn keep(drain_first: bool, total: usize) -> WriteDecision {
    WriteDecision {
        drain_first,
        action: WriteAction::Keep { total: total as u32 },
    }
}

/// Returns whether a flush keeps holding the kept bytes, which it does inside a poll while
/// requests may be unread, because the poll's end writes them at the latest.
#[inline]
pub fn flush_holds(conn: &ConnectionFacts, draining: bool) -> bool {
    !draining && conn.poll != PollPhase::Outside && conn.input_unread
}

/// Returns whether the end of a poll writes out what is `owed`; it does not after the poll met a
/// blocked inner IO, because another try is a certain `EAGAIN`.
#[inline]
pub fn poll_end_writes(phase: PollPhase, owed: bool) -> bool {
    phase != PollPhase::Inside { blocked: true } && owed
}

/// Returns the protocol once `got` follows `seen`; it is not inlined because only a connection's
/// first bytes are sniffed.
pub fn sniffed(seen: PartialPreface, got: &[u8]) -> Protocol {
    let want = seen.rest();
    let n = got.len().min(want.len());
    if got[..n] != want[..n] {
        Protocol::Http1
    } else if n == want.len() {
        Protocol::Http2
    } else {
        // `n < want.len()`, so `seen + n` is shorter than the preface and fits a `u8`.
        Protocol::Sniffing(PartialPreface(seen.0 + n as u8))
    }
}

#[inline]
pub fn keeps_allocation(capacity: usize) -> bool {
    capacity <= KEEP_CAPACITY
}

#[inline]
pub fn first_allocation(total: usize) -> usize {
    total.max(FIRST_CAPACITY)
}

#[cfg(test)]
mod tests {
    use super::*;

    const INSIDE: PollPhase = PollPhase::Inside { blocked: false };

    /// A connection inside a poll with requests unread is a pipelined batch, which is kept.
    const BATCHING: ConnectionFacts = ConnectionFacts {
        protocol: Protocol::Http1,
        poll: INSIDE,
        input_unread: true,
        vectored: true,
    };

    fn http1(kept: usize, draining: bool, n: usize) -> WriteDecision {
        let buf = vec![0; n];
        write(&BATCHING, kept, draining, &[IoSlice::new(&buf)])
    }

    fn alone() -> WriteDecision {
        direct(false)
    }

    fn keep(drain_first: bool, total: usize) -> WriteDecision {
        WriteDecision {
            drain_first,
            action: WriteAction::Keep { total: u32::try_from(total).unwrap() },
        }
    }

    fn direct(drain_first: bool) -> WriteDecision {
        WriteDecision {
            drain_first,
            action: WriteAction::Direct,
        }
    }

    /// Kept bytes plus a write up to the limit together are kept; one byte more writes out what
    /// is kept first and then keeps the write.
    #[test]
    fn a_write_that_fills_the_buffer_to_the_limit_is_kept_and_one_byte_more_drains_first() {
        assert_eq!(http1(100, false, COALESCE_LIMIT - 100), keep(false, COALESCE_LIMIT - 100));
        assert_eq!(http1(100, false, COALESCE_LIMIT - 99), keep(true, COALESCE_LIMIT - 99));
    }

    /// Past the limit, a write of the limit or more goes straight through after what is kept;
    /// one byte under it is kept.
    #[test]
    fn past_the_limit_a_write_of_the_limit_goes_through_and_one_byte_under_is_kept() {
        assert_eq!(http1(1, false, COALESCE_LIMIT), direct(true));
        assert_eq!(http1(2, false, COALESCE_LIMIT - 1), keep(true, COALESCE_LIMIT - 1));
    }

    /// A write of the limit is never copied, even into an empty buffer; one byte under it is kept.
    #[test]
    fn a_write_of_the_limit_goes_through_and_one_byte_under_is_kept() {
        assert_eq!(http1(0, false, COALESCE_LIMIT), direct(false));
        assert_eq!(http1(0, false, COALESCE_LIMIT - 1), keep(false, COALESCE_LIMIT - 1));
    }

    #[test]
    fn a_vectored_write_counts_all_its_slices() {
        let (a, b) = (vec![0; COALESCE_LIMIT / 2], vec![0; COALESCE_LIMIT / 2 + 1]);
        assert_eq!(
            write(&BATCHING, 0, false, &[IoSlice::new(&a), IoSlice::new(&b)]),
            direct(false)
        );
    }

    /// A kept count that would overflow with the write is past the limit, not wrapped under it.
    #[test]
    fn kept_bytes_and_a_write_past_usize_max_are_past_the_limit() {
        assert_eq!(http1(usize::MAX, false, 1), keep(true, 1));
    }

    /// A write-out under way goes first, and the edge is then judged on an empty buffer: what it
    /// was writing out does not count against the write.
    #[test]
    fn a_write_out_under_way_goes_first_and_leaves_the_buffer_empty() {
        assert_eq!(http1(COALESCE_LIMIT, true, COALESCE_LIMIT - 1), keep(true, COALESCE_LIMIT - 1));
        assert_eq!(http1(COALESCE_LIMIT, true, COALESCE_LIMIT), direct(true));
    }

    #[test]
    fn a_connection_still_sniffing_is_coalesced() {
        let buf = [0; 10];
        let sniffing = ConnectionFacts {
            protocol: Protocol::Sniffing(PartialPreface(3)),
            ..BATCHING
        };
        assert_eq!(write(&sniffing, 0, false, &[IoSlice::new(&buf)]), keep(false, 10));
    }

    /// HTTP/2 passes through, after what was kept while the preface was being sniffed.
    #[test]
    fn http2_passes_through_after_what_was_kept() {
        let buf = vec![0; COALESCE_LIMIT + 1];
        let pass = |drain_first| WriteDecision {
            drain_first,
            action: WriteAction::Pass,
        };
        let h2 = ConnectionFacts {
            protocol: Protocol::Http2,
            poll: PollPhase::Outside,
            input_unread: false,
            ..BATCHING
        };
        assert_eq!(write(&h2, 5, false, &[IoSlice::new(&buf[..10])]), pass(true));
        assert_eq!(write(&h2, 0, false, &[IoSlice::new(&buf)]), pass(false));
        assert_eq!(write(&h2, 5, true, &[IoSlice::new(&buf)]), pass(true));
    }

    /// A response with nothing kept before it and no request unread after it is the only one
    /// this write will carry: it goes straight through, uncopied, whatever its size.
    #[test]
    fn a_lone_response_goes_through_and_one_with_requests_unread_behind_it_is_kept() {
        let buf = [0; 100];
        let one = [IoSlice::new(&buf)];
        let read_pending = ConnectionFacts {
            input_unread: false,
            ..BATCHING
        };
        assert_eq!(write(&read_pending, 0, false, &one), alone());
        let outside = ConnectionFacts {
            poll: PollPhase::Outside,
            ..BATCHING
        };
        assert_eq!(
            write(&outside, 0, false, &one),
            alone(),
            "outside a poll a flush always writes anyway"
        );
        assert_eq!(write(&BATCHING, 0, false, &one), keep(false, 100), "a pipelined batch");
    }

    /// Bytes kept or under way go first, so a response behind them is kept to go with them.
    #[test]
    fn a_response_behind_kept_bytes_is_kept() {
        let buf = [0; 100];
        let one = [IoSlice::new(&buf)];
        let behind = ConnectionFacts {
            input_unread: false,
            ..BATCHING
        };
        assert_eq!(write(&behind, 1, false, &one), keep(false, 100));
        assert_eq!(write(&behind, 0, true, &one), keep(true, 100));
    }

    /// An inner IO that writes one slice per call would split a multi-slice response into a write
    /// per slice: such a response is kept and goes out as one; a single slice still goes through.
    #[test]
    fn several_slices_go_through_only_to_an_inner_io_that_takes_them_in_one_write() {
        let (head, body) = ([0; 10], [0; 20]);
        let scalar = ConnectionFacts {
            input_unread: false,
            vectored: false,
            ..BATCHING
        };
        let two = [IoSlice::new(&head), IoSlice::new(&body)];
        assert_eq!(write(&scalar, 0, false, &two), keep(false, 30));
        assert_eq!(write(&scalar, 0, false, &[IoSlice::new(&head)]), alone());
        let vectored = ConnectionFacts { vectored: true, ..scalar };
        assert_eq!(write(&vectored, 0, false, &two), alone());
    }

    #[test]
    fn the_preface_is_recognised_across_reads_and_anything_else_is_http1() {
        let seen = PartialPreface;
        assert_eq!(sniffed(PartialPreface::EMPTY, &H2_PREFACE[..10]), Protocol::Sniffing(seen(10)));
        assert_eq!(sniffed(seen(10), &H2_PREFACE[10..]), Protocol::Http2);
        assert_eq!(sniffed(seen(10), &H2_PREFACE[10..23]), Protocol::Sniffing(seen(23)));
        assert_eq!(sniffed(PartialPreface::EMPTY, b"PRI * HTTP/1.1"), Protocol::Http1);
        assert_eq!(sniffed(seen(23), b"\n"), Protocol::Http2, "the last byte");
        assert_eq!(sniffed(seen(23), b"x"), Protocol::Http1);
    }

    /// Inside a poll, with requests unread, a flush holds the kept bytes; once a read has come
    /// back `Pending`, outside a poll, or with a write-out under way, it writes.
    #[test]
    fn a_flush_holds_only_inside_a_poll_while_requests_are_unread() {
        let with = |poll, input_unread| ConnectionFacts {
            poll,
            input_unread,
            ..BATCHING
        };
        assert!(flush_holds(&BATCHING, false));
        assert!(flush_holds(&with(PollPhase::Inside { blocked: true }, true), false));
        assert!(!flush_holds(&with(INSIDE, false), false), "a read came back Pending");
        assert!(!flush_holds(&with(PollPhase::Outside, true), false), "outside a poll");
        assert!(!flush_holds(&BATCHING, true), "a write-out under way is carried on");
    }

    #[test]
    fn the_end_of_a_poll_writes_unless_the_poll_was_blocked_or_nothing_is_owed() {
        assert!(poll_end_writes(INSIDE, true));
        assert!(!poll_end_writes(INSIDE, false));
        assert!(!poll_end_writes(PollPhase::Inside { blocked: true }, true));
        assert!(poll_end_writes(PollPhase::Outside, true));
    }

    #[test]
    fn an_emptied_buffer_keeps_its_allocation_up_to_the_limit() {
        assert!(keeps_allocation(KEEP_CAPACITY));
        assert!(!keeps_allocation(KEEP_CAPACITY + 1));
        assert_eq!(KEEP_CAPACITY, 64 * 1024);
    }

    #[test]
    fn the_first_allocation_is_at_least_a_batch_of_small_responses() {
        assert_eq!(first_allocation(1), FIRST_CAPACITY);
        assert_eq!(first_allocation(FIRST_CAPACITY + 1), FIRST_CAPACITY + 1);
    }

    /// The decision must stay small enough for the compiler to keep in registers at the call site.
    #[test]
    fn a_write_decision_is_small() {
        assert!(size_of::<WriteDecision>() <= 16);
    }
}

/// Proves the edges above for every input, not only the cases the tests pick. Each slice is a
/// prefix of one buffer a byte past the limit, so their sum cannot overflow, as for real slices.
#[cfg(kani)]
mod proofs {
    use super::*;

    static BYTES: [u8; COALESCE_LIMIT + 1] = [0; COALESCE_LIMIT + 1];

    fn any_slices() -> ([IoSlice<'static>; 3], usize) {
        let slice = || IoSlice::new(&BYTES[..kani::any_where(|n: &usize| *n <= BYTES.len())]);
        let count = kani::any_where(|n: &usize| *n <= 3);
        ([slice(), slice(), slice()], count)
    }

    fn any_connection() -> ConnectionFacts {
        let protocol = match kani::any::<u8>() % 3 {
            0 => Protocol::Http1,
            1 => Protocol::Http2,
            _ => Protocol::Sniffing(PartialPreface(kani::any_where(|n: &u8| usize::from(*n) < H2_PREFACE.len()))),
        };
        let poll = if kani::any() { PollPhase::Outside } else { PollPhase::Inside { blocked: kani::any() } };
        ConnectionFacts { protocol, poll, input_unread: kani::any(), vectored: kani::any() }
    }

    /// Proves that `keep`'s `as u32` and the IO's `as usize` back lose nothing.
    #[kani::proof]
    #[kani::unwind(4)]
    fn a_kept_write_counts_every_slice_and_is_under_the_limit() {
        let (slices, count) = any_slices();
        let bufs = &slices[..count];
        let decision = write(&any_connection(), kani::any(), kani::any(), bufs);
        if let WriteAction::Keep { total } = decision.action() {
            let sum: usize = bufs.iter().map(|b| b.len()).sum();
            assert!(sum < COALESCE_LIMIT);
            assert_eq!(total as usize, sum);
        }
    }

    /// Proves the buffer never grows past the limit and the sum does not overflow.
    #[kani::proof]
    #[kani::unwind(4)]
    fn a_kept_write_that_drains_nothing_first_fits_beside_what_is_kept() {
        let (slices, count) = any_slices();
        let kept: usize = kani::any();
        let decision = write(&any_connection(), kept, kani::any(), &slices[..count]);
        if let WriteAction::Keep { total } = decision.action() {
            if !decision.drain_first() {
                let together = kept.checked_add(total as usize);
                assert!(together.is_some_and(|n| n <= COALESCE_LIMIT));
            }
        }
    }

    #[kani::proof]
    #[kani::unwind(4)]
    fn http2_and_only_http2_passes_through() {
        let (slices, count) = any_slices();
        let conn = any_connection();
        let (kept, draining): (usize, bool) = (kani::any(), kani::any());
        let decision = write(&conn, kept, draining, &slices[..count]);
        assert_eq!(decision.action() == WriteAction::Pass, conn.protocol == Protocol::Http2);
        if conn.protocol == Protocol::Http2 {
            assert_eq!(decision.drain_first(), draining || kept > 0);
        }
    }

    #[kani::proof]
    #[kani::unwind(26)]
    fn sniffing_any_bytes_never_panics_and_stays_short_of_the_preface() {
        let seen = PartialPreface(kani::any_where(|n: &u8| usize::from(*n) < H2_PREFACE.len()));
        let bytes: [u8; 25] = kani::any();
        let got = &bytes[..kani::any_where(|n: &usize| *n <= bytes.len())];
        if let Protocol::Sniffing(now) = sniffed(seen, got) {
            assert!(usize::from(now.0) < H2_PREFACE.len());
            assert_eq!(usize::from(now.0), usize::from(seen.0) + got.len());
        }
    }
}
