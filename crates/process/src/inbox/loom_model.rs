//! The Loom model of an inbox's close racing a send, over every interleaving and every value a
//! relaxed load of the closed flag may read. The flag and the fences are `crate::sync`'s, Loom's
//! here; the queue is crossbeam's, which Loom does not see, so each push and pop is one step. Run with
//! `RUSTFLAGS="--cfg sync_model" cargo test -p mipyre_process --lib --profile model --target-dir target/loom loom`.
//!
//! It proves: once a send and a close have both returned, the inbox holds nothing and the item was
//! dropped, whether the send saw the close before it pushed, after it pushed, or not at all.

use std::num::NonZeroUsize;
use std::sync::Arc;

use loom::thread;

use super::Inbox;

#[test]
fn a_send_racing_the_close_leaves_nothing_in_the_inbox() {
    loom::model(|| {
        let inbox = Arc::new(Inbox::new(NonZeroUsize::MIN));
        let item = Arc::new(());
        let sender = thread::spawn({
            let inbox = Arc::clone(&inbox);
            let item = Arc::clone(&item);
            move || drop(inbox.send(item))
        });
        inbox.close();
        sender.join().unwrap();

        assert!(inbox.take().is_none(), "an item was left in a closed inbox");
        assert_eq!(Arc::strong_count(&item), 1, "the item was neither taken nor dropped");
    });
}
