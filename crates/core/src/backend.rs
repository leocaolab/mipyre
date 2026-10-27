use std::future::Future;

/// A backend that runs the handler elsewhere: the async pool, the main interpreter's bridge, a
/// process lane, a blocking thread. What the ask and the reply hold, how the ask travels and how a
/// run is stopped are the backend's; the core only waits for the run.
pub trait HandoffBackend {
    type Ask;
    /// What the run produced, a backend's failure included (a worker that died, a lane that timed
    /// out). A worker's death is still reported by the backend's own lifecycle: nobody may be
    /// waiting for this reply.
    type Reply;
    /// Why the backend would not take an ask: full, retired, shutting down. It carries the reason,
    /// not the ask. A refusal the backend only learns after taking the ask (another worker it
    /// forwarded the ask to was full, its run stopped) is in the `Reply`.
    type Refusal;
    /// One run of a handler. Dropped before it is ready, it tells the backend that nobody waits
    /// for it any more; whether the run is then dropped, interrupted or left to finish is the
    /// backend's, in this type's `Drop`.
    type Run: Future<Output = Self::Reply>;

    /// Takes the ask or refuses it, never waiting for room.
    ///
    /// # Errors
    ///
    /// Returns the backend's reason when it cannot take the ask now.
    fn submit(&self, ask: Self::Ask) -> Result<Self::Run, Self::Refusal>;
}
