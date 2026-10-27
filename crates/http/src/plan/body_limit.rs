//! Decides how large a request body may get as it arrives.

/// The body is, or says it will be, larger than its limit, which is the client's fault (a 413).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TooLarge;

/// One request body's size limit; the reader keeps the count of bytes received and hands it in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BodySizeLimit {
    max: usize,
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
        let over = |announced: u64| usize::try_from(announced).map_or(true, |n| n > max);
        if over(announced_lower) {
            return Err(TooLarge);
        }
        if announced_exact.is_some_and(over) {
            return Err(TooLarge);
        }
        Ok(BodySizeLimit { max })
    }

    /// Returns the bytes received once a frame of `len` bytes is added to `received`.
    ///
    /// # Errors
    ///
    /// Returns [`TooLarge`] when the frame takes the body past `max`; the frame must then not be
    /// taken.
    #[inline]
    pub fn take(&self, received: usize, len: usize) -> Result<usize, TooLarge> {
        match received.checked_add(len) {
            Some(received) if received <= self.max => Ok(received),
            _ => Err(TooLarge),
        }
    }
}

#[cfg(test)]
mod fuzz;

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
        assert!(BodySizeLimit::start(1024, 0, Some(1024)).is_ok());
    }

    #[test]
    fn a_body_of_unknown_size_starts_and_counts_as_it_arrives() {
        assert!(BodySizeLimit::start(1024, 0, None).is_ok());
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
        assert_eq!(
            unlimited.take(usize::MAX - 1, 2),
            Err(TooLarge),
            "past what usize counts"
        );
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
        let max = asked;
        assert!(u128::from(lower) <= max as u128);
        assert!(exact.is_none_or(|n| u128::from(n) <= max as u128));
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
}
