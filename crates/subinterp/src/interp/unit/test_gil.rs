//! Stands in for CPython's GIL in the slot protocol's tests and its models.

use std::marker::PhantomData;

use crate::sync::{Condvar, Mutex};

pub(super) struct Gil {
    held: Mutex<bool>,
    released: Condvar,
}

#[derive(Clone, Copy)]
pub(super) struct Held<'py>(PhantomData<&'py ()>);

impl Gil {
    pub(super) fn new() -> Self {
        Gil {
            held: Mutex::new(false),
            released: Condvar::new(),
        }
    }

    pub(super) fn take(&self) -> Held<'static> {
        let mut held = self.held.lock();
        while *held {
            self.released.wait(&mut held);
        }
        *held = true;
        Held(PhantomData)
    }

    pub(super) fn give(&self) {
        *self.held.lock() = false;
        // Every waiter wakes and one takes the GIL; the others find it held and wait again.
        self.released.notify_all();
    }
}
