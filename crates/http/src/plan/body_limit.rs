//! Decides how large a request body may get as it arrives and how far its buffer grows.

/// The body is, or says it will be, larger than its limit, which is the client's fault (a 413).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TooLarge;

/// One request body's size limit and announced size; the reader keeps the count of bytes
/// received and hands it in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BodySizeLimit {
    max: usize,
    expected: Option<usize>,
}

impl BodySizeLimit {
    /// Starts a body of at most `max` bytes that announced at least `announced_lower` bytes, or
    /// exactly `announced_exact`.
    ///
    /// # Errors
    ///
    /// Returns [`TooLarge`] when an announced size is over `max` or does not fit `usize`, so the
    /// body is refused before any of it is read.
    #[inline]
    pub fn start(max: usize, announced_lower: u64, announced_exact: Option<u64>) -> Result<Self, TooLarge> {
        // Keeping `max` under `usize::MAX` makes `take`'s saturating sum exact: with a `max` of
        // `usize::MAX`, a saturated sum would let a body past it.
        let max = max.min(usize::MAX - 1);
        let over = |announced: u64| usize::try_from(announced).map_or(true, |n| n > max);
        if over(announced_lower) {
            return Err(TooLarge);
        }
        let expected = match announced_exact {
            Some(exact) if over(exact) => return Err(TooLarge),
            // The announced size only caps how far the buffer grows, and never sizes it ahead of
            // the bytes that came in.
            Some(exact) => Some(exact as usize),
            None => None,
        };
        Ok(BodySizeLimit { max, expected })
    }

    #[inline]
    pub fn expected(&self) -> Option<usize> {
        self.expected
    }

    /// Returns the bytes received once a frame of `len` bytes is added to `received`.
    ///
    /// # Errors
    ///
    /// Returns [`TooLarge`] when the frame takes the body past `max`; the frame must then not be
    /// taken.
    #[inline]
    pub fn take(&self, received: usize, len: usize) -> Result<usize, TooLarge> {
        let received = received.saturating_add(len);
        if received > self.max {
            return Err(TooLarge);
        }
        Ok(received)
    }
}

/// Returns the capacity a body buffer grows to when it must hold `needed` bytes.
///
/// Doubling keeps the copies linear in the body's size, and the announced size caps it, so an
/// honest `Content-Length` body ends in a buffer of exactly its size. The capacity never exceeds
/// twice what arrived, so a client cannot reserve memory by announcing a large size.
#[inline]
pub fn grown_capacity(needed: usize, expected: Option<usize>) -> usize {
    let doubled = needed.saturating_mul(2);
    match expected {
        Some(size) if size >= needed => doubled.min(size),
        _ => doubled,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_body_announced_at_the_limit_starts_and_one_byte_over_is_refused() {
        assert!(BodySizeLimit::start(1024, 1024, Some(1024)).is_ok());
        assert_eq!(BodySizeLimit::start(1024, 1025, Some(1025)), Err(TooLarge));
    }

    #[test]
    fn an_exact_size_over_the_limit_is_refused() {
        assert_eq!(BodySizeLimit::start(1024, 0, Some(1025)), Err(TooLarge));
        assert_eq!(BodySizeLimit::start(1024, 0, Some(1024)).map(|l| l.expected()), Ok(Some(1024)));
    }

    #[test]
    fn a_body_of_unknown_size_starts_and_counts_as_it_arrives() {
        let limit = BodySizeLimit::start(1024, 0, None);
        assert_eq!(limit.map(|l| l.expected()), Ok(None));
    }

    #[test]
    fn an_announced_size_past_the_limit_in_u64_is_refused() {
        assert_eq!(BodySizeLimit::start(usize::MAX - 1, u64::MAX, None), Err(TooLarge));
    }

    #[test]
    fn frames_up_to_the_limit_are_taken_and_the_byte_past_it_is_refused() {
        let limit = BodySizeLimit::start(20, 0, None).unwrap();
        assert_eq!(limit.take(0, 10), Ok(10));
        assert_eq!(limit.take(10, 10), Ok(20), "exactly the limit");
        assert_eq!(limit.take(20, 1), Err(TooLarge));
        assert_eq!(limit.take(20, 0), Ok(20));
    }

    #[test]
    fn frames_past_usize_max_are_refused() {
        let limit = BodySizeLimit::start(usize::MAX - 1, 0, None).unwrap();
        assert_eq!(limit.take(0, usize::MAX - 1), Ok(usize::MAX - 1));
        assert_eq!(limit.take(usize::MAX - 1, 2), Err(TooLarge));
        let unlimited = BodySizeLimit::start(usize::MAX, 0, None).unwrap();
        assert_eq!(unlimited.take(usize::MAX - 1, 0), Ok(usize::MAX - 1));
        assert_eq!(unlimited.take(usize::MAX - 1, 2), Err(TooLarge), "past what usize counts");
    }

    #[test]
    fn a_buffer_doubles_up_to_the_announced_size() {
        assert_eq!(grown_capacity(10, None), 20);
        assert_eq!(grown_capacity(10, Some(15)), 15);
        assert_eq!(grown_capacity(10, Some(20)), 20);
        assert_eq!(grown_capacity(10, Some(10)), 10, "exactly what was announced");
        assert_eq!(grown_capacity(11, Some(10)), 22);
        assert_eq!(grown_capacity(usize::MAX, None), usize::MAX);
    }
}

/// Proves the limit's arithmetic for every input with Kani.
#[cfg(kani)]
mod proofs {
    use super::*;

    #[kani::proof]
    fn an_accepted_body_never_passes_its_max() {
        let (asked, lower, exact): (usize, u64, Option<u64>) = (kani::any(), kani::any(), kani::any());
        let Ok(limit) = BodySizeLimit::start(asked, lower, exact) else {
            return;
        };
        let max = asked.min(usize::MAX - 1);
        assert!(u128::from(lower) <= max as u128);
        assert_eq!(limit.expected().map(|n| n as u64), exact);
        assert!(limit.expected().is_none_or(|n| n <= max));
        let (received, len): (usize, usize) = (kani::any(), kani::any());
        let sum = received as u128 + len as u128;
        match limit.take(received, len) {
            Ok(now) => {
                assert!(now as u128 == sum && now <= max);
            }
            Err(TooLarge) => {
                assert!(sum > max as u128);
            }
        }
    }

    #[kani::proof]
    fn a_grown_buffer_holds_what_is_needed_and_stops_at_the_announced_size() {
        let (needed, expected): (usize, Option<usize>) = (kani::any(), kani::any());
        let grown = grown_capacity(needed, expected);
        assert!(grown >= needed);
        assert!(grown as u128 <= 2 * needed as u128);
        if let Some(size) = expected {
            if size >= needed {
                assert!(grown <= size);
            }
        }
    }
}
