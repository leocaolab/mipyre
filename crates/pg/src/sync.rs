//! The lock this crate's cross-thread state is made of (a handle's binding). Shipping code names a
//! lock only through this module.

pub(crate) use parking_lot::RwLock;
