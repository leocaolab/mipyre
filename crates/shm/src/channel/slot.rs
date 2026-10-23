//! Hands out the channel's slots as the protocol's states: a request the host writes and then publishes, the
//! answers the peer published, taken one by one into the host's own bytes, and a request the peer reads and answers
//! in one call. Each handle borrows its end, so no slot outlives its turn and no two are held at once.

use bytes::BytesMut;

use super::ends::PeerFault;
use super::memory::{MessageView, SlotRef};
use super::{Channel, Memory};
use crate::plan::ring::{Seq, Window};
use crate::plan::slot_len::SlotLengthError;
use crate::protocol::doorbell::{publish, Doorbell};
use crate::sync::Ordering::Relaxed;

/// A message does not fit the room of the slot it is written into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{needed} bytes do not fit a channel slot of {room} bytes")]
pub struct SlotRoomError {
    pub room: u32,
    /// It is a `u64` because what was asked for may pass any slot.
    pub needed: u64,
}

/// Writes a message into a slot from its start; it never passes the slot's room.
pub struct SlotWriter<'s> {
    slot: SlotRef<'s>,
    len: u32,
}

impl SlotWriter<'_> {
    /// Appends `bytes`; one that does not fit is refused and the length stays.
    #[inline]
    pub fn put(&mut self, bytes: &[u8]) -> Result<(), SlotRoomError> {
        self.len = self.slot.copy_in(self.len, bytes)?;
        Ok(())
    }

    /// Returns how many bytes a message here may take in all.
    #[inline]
    pub fn room(&self) -> u32 {
        self.slot.room()
    }

    #[inline]
    pub fn len(&self) -> u32 {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Drops what was written, so a writer that ran out of room can write a shorter message instead.
    #[inline]
    pub fn restart(&mut self) {
        self.len = 0;
    }
}

/// The free slot of the host's next request. Dropping it sends nothing; [`RequestSlot::publish`] sends what was
/// written.
///
/// It borrows its end, so two are never held at once:
///
/// ```compile_fail,E0499
/// # use std::sync::Arc;
/// # use mipyre_shm::adapter::doorbell::FutexDoorbell;
/// # use mipyre_shm::channel::{Channel, HeapMemory, HostEnd};
/// # use mipyre_shm::plan::{geometry::{Geometry, SlotBytes}, ring::RingDepth};
/// # let g = Geometry::new(RingDepth::new(2).unwrap(), SlotBytes::new(64).unwrap()).unwrap();
/// let mut end = HostEnd::new(Arc::new(Channel::new(HeapMemory::new(&g))), FutexDoorbell);
/// let first = end.request_slot();
/// let second = end.request_slot();
/// drop(first);
/// ```
///
/// and none outlives it:
///
/// ```compile_fail,E0505
/// # use std::sync::Arc;
/// # use mipyre_shm::adapter::doorbell::FutexDoorbell;
/// # use mipyre_shm::channel::{Channel, HeapMemory, HostEnd};
/// # use mipyre_shm::plan::{geometry::{Geometry, SlotBytes}, ring::RingDepth};
/// # let g = Geometry::new(RingDepth::new(2).unwrap(), SlotBytes::new(64).unwrap()).unwrap();
/// let mut end = HostEnd::new(Arc::new(Channel::new(HeapMemory::new(&g))), FutexDoorbell);
/// let slot = end.request_slot();
/// drop(end);
/// drop(slot);
/// ```
#[must_use = "a request slot that is not published leaves the request unsent"]
pub struct RequestSlot<'e, M: Memory, B: Doorbell> {
    channel: &'e Channel<M>,
    bell: &'e B,
    window: &'e mut Window,
    len: u32,
}

impl<'e, M: Memory, B: Doorbell> RequestSlot<'e, M, B> {
    #[inline]
    pub(super) fn new(channel: &'e Channel<M>, bell: &'e B, window: &'e mut Window) -> Self {
        RequestSlot {
            channel,
            bell,
            window,
            len: 0,
        }
    }

    #[inline]
    pub fn seq(&self) -> Seq {
        self.window.sent()
    }

    /// Returns the slot's index in its ring, which the request's answer shares.
    #[inline]
    pub fn index(&self) -> usize {
        self.seq().slot_index(self.channel.memory.geometry().depth())
    }

    /// Lets `fill` write the message; a later call goes on from where this one ended.
    #[inline]
    pub fn write<R>(&mut self, fill: impl FnOnce(&mut SlotWriter<'_>) -> R) -> R {
        let slot = self
            .channel
            .memory
            .slot(self.channel.memory.geometry().request_slot(self.seq()));
        let mut writer = SlotWriter { slot, len: self.len };
        let out = fill(&mut writer);
        self.len = writer.len;
        out
    }

    /// Records the written length in the slot (0 when nothing was written, so the peer never reads an earlier
    /// request's length), publishes the request and rings the peer when it said it sleeps; `Ok(None)` rang nothing.
    ///
    /// # Errors
    ///
    /// The doorbell's error, after the request is published.
    #[inline]
    pub fn publish(self) -> Result<Option<B::Outcome>, B::Error> {
        let seq = self.seq();
        let memory = &self.channel.memory;
        memory
            .slot(memory.geometry().request_slot(seq))
            .len_word()
            .store(self.len, Relaxed);
        *self.window = Window::new(self.window.read(), seq.after(1));
        let h = memory.header();
        publish(&h.req_head, &h.peer_sleeping, seq.after(1).get(), self.bell)
    }
}

/// The answers the peer published and the host has not taken, oldest first. Dropping it leaves the rest for the
/// next [`HostEnd::answers`](super::HostEnd::answers).
pub struct ReadyAnswers<'e, M: Memory> {
    channel: &'e Channel<M>,
    window: &'e mut Window,
    published: Seq,
    fault: &'e mut Option<PeerFault>,
}

impl<'e, M: Memory> ReadyAnswers<'e, M> {
    #[inline]
    pub(super) fn new(
        channel: &'e Channel<M>,
        window: &'e mut Window,
        published: Seq,
        fault: &'e mut Option<PeerFault>,
    ) -> Self {
        ReadyAnswers {
            channel,
            window,
            published,
            fault,
        }
    }

    /// Copies the oldest ready answer, whole, onto the end of `dst` and frees its slot; `Ok(None)` when none is left.
    /// Its length is read once: past the slot it is the peer's fault, which the end keeps, and `dst` is left as it
    /// was.
    ///
    /// # Errors
    ///
    /// The peer's fault, this one or one kept from before.
    #[inline]
    pub fn take_next(&mut self, dst: &mut BytesMut) -> Result<Option<Seq>, PeerFault> {
        if let Some(fault) = *self.fault {
            return Err(fault);
        }
        let seq = self.window.read();
        if seq == self.published {
            return Ok(None);
        }
        let memory = &self.channel.memory;
        if let Err(e) = memory.slot(memory.geometry().answer_slot(seq)).take_message(dst) {
            return Err(*self.fault.insert(PeerFault::Length(e)));
        }
        *self.window = Window::new(seq.after(1), self.window.sent());
        Ok(Some(seq))
    }
}

/// A request the peer took. It answers it with [`PeerRequest::answer`]; dropping it unanswered leaves the request to
/// be taken again.
#[must_use = "a request the peer does not answer is taken again"]
pub struct PeerRequest<'p, M: Memory, B: Doorbell> {
    channel: &'p Channel<M>,
    bell: &'p B,
    cursor: &'p mut Seq,
}

impl<'p, M: Memory, B: Doorbell> PeerRequest<'p, M, B> {
    #[inline]
    pub(super) fn new(channel: &'p Channel<M>, bell: &'p B, cursor: &'p mut Seq) -> Self {
        PeerRequest { channel, bell, cursor }
    }

    #[inline]
    pub fn seq(&self) -> Seq {
        *self.cursor
    }

    /// Borrows the request's bytes. The host wrote them and does not touch the slot until it reads the answer, so
    /// they are borrowed, not copied; the borrow ends before the request is answered.
    ///
    /// # Errors
    ///
    /// The slot's length passes the slot.
    #[inline]
    pub fn bytes(&self) -> Result<MessageView<'_>, SlotLengthError> {
        let memory = &self.channel.memory;
        memory.slot(memory.geometry().request_slot(*self.cursor)).view_message()
    }

    /// Lets `fill` write the answer and publishes it, ringing the host when it said it sleeps (`Ok(None)` rang
    /// nothing). An answer that does not fit is for `fill` to replace with a shorter one.
    ///
    /// # Errors
    ///
    /// The doorbell's error, after the answer is published.
    #[inline]
    pub fn answer(self, fill: impl FnOnce(&mut SlotWriter<'_>)) -> Result<Option<B::Outcome>, B::Error> {
        let seq = *self.cursor;
        let memory = &self.channel.memory;
        let slot = memory.slot(memory.geometry().answer_slot(seq));
        let len_word = slot.len_word();
        let mut writer = SlotWriter { slot, len: 0 };
        fill(&mut writer);
        len_word.store(writer.len, Relaxed);
        *self.cursor = seq.after(1);
        let h = memory.header();
        let rung = publish(&h.resp_head, &h.host_sleeping, seq.after(1).get(), self.bell)?;
        if rung.is_some() {
            h.peer_waits.wakes.add(1);
        }
        Ok(rung)
    }
}
