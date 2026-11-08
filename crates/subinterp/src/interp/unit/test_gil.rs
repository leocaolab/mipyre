//! Stands in for CPython's GIL in the slot protocol's tests and its models.

use std::marker::PhantomData;
use std::sync::PoisonError;

use crate::sync::{Condvar, Mutex};

pub(super) struct Gil {
    held: Mutex<bool>,
    released: Condvar,
}

#[derive(Clone, Copy)]
pub(super) struct GilToken<'py>(PhantomData<&'py ()>);

impl Gil {
    pub(super) fn new() -> Self {
        Gil {
            held: Mutex::new(false),
            released: Condvar::new(),
        }
    }

    pub(super) fn take(&self) -> GilToken<'static> {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        while *held {
            held = self.released.wait(held).unwrap_or_else(PoisonError::into_inner);
        }
        *held = true;
        GilToken(PhantomData)
    }

    pub(super) fn give(&self) {
        *self.held.lock().unwrap_or_else(PoisonError::into_inner) = false;
        // Every waiter wakes and one takes the GIL; the others find it held and wait again.
        self.released.notify_all();
    }
}
