//! Makes pyre's HTTP decisions as pure functions: no IO, no clock, no threads and no state shared
//! across threads.
//!
//! The package's `clippy.toml` and archcheck rule E-P1c refuse IO calls and tokio, hyper and
//! socket2 types outside `io/`. Nothing checks that these files' dependency closure is free of
//! those crates, because the package has one dependency list and `io/` needs them.
#![forbid(unsafe_code)]
#![deny(
    unreachable_pub,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

pub mod body_limit;
pub mod coalesce;
pub mod compression;
pub mod conditional;
pub mod cors;
pub mod header;
pub mod limits;
pub mod reject;
pub mod request;
pub mod response;
pub mod route;
pub mod site;
pub mod static_plan;
pub mod ws;
