//! Takes a request the front has routed to a Python handler through to its reply, the same way
//! whichever backend runs the handler: on the calling thread, timed by `InlineTimer`, or elsewhere
//! and waited for (`HandoffBackend`). It never sees a Python object, a thread, a pid or a ring.

#![forbid(unsafe_code)]

mod backend;
mod inline;
mod wait;

pub use backend::HandoffBackend;
pub use inline::InlineTimer;
pub use wait::{dispatch, ReplyWait};
