//! Does the IO under pyre's HTTP: the request body, write coalescing, request ids and heads, static
//! files, and the counts of what HTTP refused.
//!
//! No module here calls into the server above it. `os` is the one file with a platform `#[cfg]`.
#![expect(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "io/ is where the package touches sockets, files, clocks and threads; clippy.toml bans them for plan/"
)]

pub mod body;
pub mod clock;
pub mod coalesce;
pub mod events;
mod os;
pub mod request_head;
pub mod request_id;
pub mod static_files;
