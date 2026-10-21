//! The Loom model of a [`Budget`] (`cache.rs`): reservations, commits and releases from two
//! threads against one cap, over every interleaving, running the budget's own code (its counter
//! is Loom's here, `crate::sync`). Run with
//! `RUSTFLAGS="--cfg sync_model" cargo test -p mipyre_http --lib --release --target-dir target/loom loom`.
//!
//! It proves: what the budget holds never passes its cap, at any step either thread can see;
//! of two reservations that each fit alone but not together, exactly one is had (a refused one
//! held nothing the other was refused for); two that fit together are both had; and what is
//! held once both threads are done is exactly what was kept.

use std::sync::Arc;

use loom::thread;

use super::Budget;

const CAP: u64 = 10;

/// Reserves `len` and keeps it (commits), or reports the refusal; checks the cap on the way.
fn keep(budget: &Budget, len: u64) -> bool {
    let Some(reservation) = budget.reserve(len) else {
        assert!(budget.held() <= CAP, "a refused reservation saw the budget past its cap");
        return false;
    };
    assert!(budget.held() <= CAP, "a reservation took the budget past its cap");
    reservation.commit(None);
    true
}

/// Both threads keep `(one, two)` bytes; returns which were had, and what is held after both.
fn race(one: u64, two: u64) -> (bool, bool, u64) {
    let budget = Arc::new(Budget::new(CAP));
    let other = Arc::clone(&budget);
    let second = thread::spawn(move || keep(&other, two));
    let first = keep(&budget, one);
    let second = second.join().unwrap();
    (first, second, budget.held())
}

#[test]
fn two_reservations_that_fit_together_are_both_had() {
    loom::model(|| {
        let (first, second, held) = race(4, 6);
        assert!(first && second, "4 + 6 fits a cap of 10, and a reservation was refused");
        assert_eq!(held, 10);
    });
}

#[test]
fn of_two_reservations_that_fit_only_alone_exactly_one_is_had() {
    loom::model(|| {
        let (first, second, held) = race(6, 6);
        assert!(first != second, "6 and 6 against 10: had {first} and {second}");
        assert_eq!(held, 6, "what is held is the one kept");
    });
}

/// A release of a kept entry racing a reservation: the reservation is had or refused by what is
/// held when it is taken, and what is held after is exactly what was kept.
#[test]
fn a_release_racing_a_reservation_leaves_what_was_kept() {
    loom::model(|| {
        let budget = Arc::new(Budget::new(CAP));
        budget.reserve(8).expect("8 of 10").commit(None);
        let other = Arc::clone(&budget);
        let releasing = thread::spawn(move || {
            other.release(8);
            assert!(other.held() <= CAP);
        });
        let had = keep(&budget, 4);
        releasing.join().unwrap();
        assert_eq!(budget.held(), if had { 4 } else { 0 });
    });
}

/// Reservations dropped uncommitted, from both threads: every byte comes back.
#[test]
fn reservations_dropped_uncommitted_hand_every_byte_back() {
    loom::model(|| {
        let budget = Arc::new(Budget::new(CAP));
        let other = Arc::clone(&budget);
        let second = thread::spawn(move || drop(other.reserve(7)));
        drop(budget.reserve(7));
        second.join().unwrap();
        assert_eq!(budget.held(), 0);
    });
}
