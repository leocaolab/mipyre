//! The Loom model of a kind's live workers ([`WorkerLife::died`]): workers that die at once, over
//! every interleaving, running the shipping code (the count and the first cause are
//! `crate::sync`'s, Loom's here, and so are the run's counters). The losses told are kept behind
//! Loom's lock, so it sees every order they can arrive in. Run with
//! `RUSTFLAGS="--cfg sync_model" cargo test -p mipyre_pyext --lib --profile model --target-dir target/loom loom`.
//!
//! It proves: of a kind's workers dying at once, exactly the last one tells the kind's loss, once,
//! and hands back the kind's first cause, which every caller reads the same; of two kinds dying at
//! once, each kind's last tells its own loss with its own cause. That a kind is taken down at most
//! once per worker is the type's: a [`WorkerLife`] is taken by its death, and there is one
//! per worker.

use std::sync::Arc;

use loom::sync::Mutex;
use loom::thread;

use super::{LiveWorkers, WorkerCounters, WorkerKind, WorkerKindLoss, WorkerLife};

/// The losses the kinds told, in the order they arrived.
#[derive(Clone, Default)]
struct Told(Arc<Mutex<Vec<WorkerKindLoss>>>);

impl Told {
    fn losses(&self) -> Vec<WorkerKindLoss> {
        self.0.lock().unwrap().clone()
    }
}

/// Returns `workers` lives of `kind`, whose loss is told to `told`.
fn kind(kind: WorkerKind, workers: usize, told: &Told) -> (LiveWorkers, Vec<WorkerLife>) {
    let told = told.clone();
    let tell = Arc::new(move |loss| told.0.lock().unwrap().push(loss));
    LiveWorkers::new(kind, workers, tell, &Arc::new(WorkerCounters::new()))
}

fn die(life: WorkerLife, cause: &'static str) -> thread::JoinHandle<Option<Arc<str>>> {
    thread::spawn(move || life.died(cause.to_string()))
}

#[test]
fn of_two_workers_dying_at_once_the_last_tells_the_loss_once_with_the_first_cause() {
    loom::model(|| {
        let told = Told::default();
        let (kind, lives) = kind(WorkerKind::TpcThread, 2, &told);
        let mut lives = lives.into_iter();
        let (Some(one), Some(two)) = (lives.next(), lives.next()) else {
            panic!("two workers, two lives");
        };
        let other = die(two, "two");
        let mine = one.died("one".to_string());
        let theirs = other.join().unwrap();

        assert!(
            mine.is_some() != theirs.is_some(),
            "one death is the last: {mine:?}, {theirs:?}"
        );
        let first = kind.first_death().expect("a worker died");
        assert_eq!(
            mine.or(theirs).as_deref(),
            Some(&*first),
            "the last hands back the first cause"
        );
        assert_eq!(
            told.losses(),
            [WorkerKindLoss {
                kind: WorkerKind::TpcThread,
                first,
            }],
            "the loss is told once"
        );
    });
}

#[test]
fn of_two_kinds_whose_last_workers_die_at_once_each_tells_its_own_loss() {
    loom::model(|| {
        let told = Told::default();
        let (_, sync) = kind(WorkerKind::PoolSync, 1, &told);
        let (_, r#async) = kind(WorkerKind::PoolAsync, 1, &told);
        let (Some(sync), Some(r#async)) = (sync.into_iter().next(), r#async.into_iter().next()) else {
            panic!("one worker of each kind, one life each");
        };
        let other = die(r#async, "the engine stopped");
        let mine = sync.died("gc.collect() raised".to_string());
        let theirs = other.join().unwrap();

        assert_eq!(mine.as_deref(), Some("gc.collect() raised"));
        assert_eq!(theirs.as_deref(), Some("the engine stopped"));
        let mut losses = told.losses();
        losses.sort_by_key(|loss| loss.kind != WorkerKind::PoolSync);
        assert_eq!(
            losses,
            [
                WorkerKindLoss {
                    kind: WorkerKind::PoolSync,
                    first: "gc.collect() raised".into(),
                },
                WorkerKindLoss {
                    kind: WorkerKind::PoolAsync,
                    first: "the engine stopped".into(),
                },
            ]
        );
    });
}
