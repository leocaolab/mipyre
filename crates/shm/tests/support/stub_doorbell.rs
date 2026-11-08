use std::convert::Infallible;

use mipyre_shm::protocol::doorbell::{AtomicU32, Doorbell};

/// A peer's doorbell to a caller that never sleeps.
pub struct StubDoorbell;

impl Doorbell for StubDoorbell {
    type Outcome = ();
    type Error = Infallible;
    fn ring(&self, _: &AtomicU32) -> Result<(), Infallible> {
        Ok(())
    }
}
