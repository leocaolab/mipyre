//! Holds what is true of any byte stream, whichever transport carries it: whether an IO error
//! says the peer went away, the write timeout, and the counts of what happened to streams.

pub mod events;
pub mod stall;

use std::io;

/// Returns whether `err` says the peer closed, reset or aborted the stream, or ended it
/// mid-message.
pub fn peer_gone(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::UnexpectedEof
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_peer_closing_its_stream_is_the_peer_gone() {
        for kind in [
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::UnexpectedEof,
        ] {
            assert!(peer_gone(&io::Error::new(kind, "peer")), "{kind:?}");
        }
    }

    #[test]
    fn other_io_errors_are_not_the_peer_gone() {
        assert!(!peer_gone(&io::Error::new(io::ErrorKind::PermissionDenied, "nope")));
        assert!(!peer_gone(&io::Error::other("boom")));
    }
}
