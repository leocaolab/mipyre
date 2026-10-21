//! Holds what a Python script constructs directly: the app and its servers, `SharedState`, the
//! configuration values' Python classes and the extension module itself.

mod alloc;
mod app;
mod config;
mod module;
mod state;

pub(crate) use self::app::{App, RegistrationSealed, Server};
