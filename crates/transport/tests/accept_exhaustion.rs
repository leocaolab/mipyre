//! Tests `EndpointSource::accept` with the process out of file descriptors, in a binary of its own
//! because lowering `RLIMIT_NOFILE` affects the whole process.
//!
//! Linux reserves the descriptor before it dequeues a connection, so a queued connection stays
//! queued while no descriptor is free; macOS dequeues it first and drops it. The test asserts the
//! first on Linux and only what both kernels promise elsewhere.
//!
//! Every client writes a byte right after it connects, because on Linux the listener's
//! `TCP_DEFER_ACCEPT` reports a connection only once its first byte arrives or after 10 s.
use std::io::Write as _;
use std::net::{SocketAddr, TcpStream};
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mipyre_transport::endpoint::{EndpointSource, Endpoints, ListenerSet, ListenerSpec};
use mipyre_transport::tcp::events::{self, TcpEvent};
use mipyre_transport::tcp::listener::{BindOptions, DEFAULT_BACKLOG, EXHAUSTION_BACKOFF};
use tracing_subscriber::fmt::MakeWriter;

/// Captures the subscriber's log lines.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Captured {
    type Writer = Captured;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl Captured {
    fn count(&self, needle: &str) -> usize {
        String::from_utf8_lossy(&self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner))
            .matches(needle)
            .count()
    }
}

/// Lowers the soft fd limit to a few above the lowest free number and takes every fd still free
/// below it, returning them so that dropping them frees the descriptors.
fn exhaust_fds(fd: &OwnedFd) -> Vec<OwnedFd> {
    // SAFETY: These are plain syscalls on a valid fd and a valid out-struct.
    unsafe {
        let lowest_free = libc::dup(fd.as_raw_fd());
        assert!(lowest_free >= 0, "dup failed before the limit was lowered");
        libc::close(lowest_free);
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        assert_eq!(libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit), 0);
        limit.rlim_cur = (lowest_free as libc::rlim_t + 16).min(limit.rlim_max);
        assert_eq!(libc::setrlimit(libc::RLIMIT_NOFILE, &limit), 0);
    }
    let mut hogs = Vec::new();
    loop {
        // SAFETY: `fd` is a valid descriptor to dup.
        let dup = unsafe { libc::dup(fd.as_raw_fd()) };
        if dup < 0 {
            let e = std::io::Error::last_os_error();
            assert_eq!(e.raw_os_error(), Some(libc::EMFILE), "{e}");
            return hogs;
        }
        // SAFETY: `dup` returned a new fd that nothing else owns.
        hogs.push(unsafe { OwnedFd::from_raw_fd(dup) });
    }
}

#[test]
fn accept_under_fd_exhaustion_backs_off_logs_and_recovers() {
    let logs = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .finish();
    let _subscriber = tracing::subscriber::set_default(subscriber);

    // The clock starts paused: the backoffs while out of descriptors take no time, and how
    // many accepts fit in a window is set by the backoff alone, not by how busy the machine is.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from(([127, 0, 0, 1], 0));
        let options = BindOptions {
            backlog: DEFAULT_BACKLOG,
            defer_accept: Duration::from_secs(10),
        };
        let bound = Endpoints::bind(
            &ListenerSet::one(ListenerSpec {
                addr: loopback,
                tls: None,
            }),
            NonZeroUsize::new(1).unwrap(),
            options,
        )
        .unwrap();
        let addr = bound.primary().addr;
        let mut source = EndpointSource::new(bound).unwrap();

        // The client connects before the descriptors run out; on Linux its connection then waits in
        // the listener's queue until accept can take it.
        let mut client = TcpStream::connect(addr).unwrap();
        client.write_all(b"G").unwrap();
        let client_addr = client.local_addr().unwrap();
        // The probe is held until the end, because a descriptor freed here would let accept succeed.
        let probe = OwnedFd::from(client.try_clone().unwrap());
        let hogs = exhaust_fds(&probe);

        // For three and a half backoffs accept() must fail and back off without returning, spinning
        // or ending; each failure is counted, and the log line, throttled to one a minute, goes out
        // once.
        let failed_before = events::total(TcpEvent::AcceptExhausted);
        let mut accept = Box::pin(source.accept());
        // The first attempt comes when the runtime hears from the kernel, which a paused clock
        // does not wait for, and an idle runtime would move the clock on first. So the runtime is
        // kept busy until that attempt has failed, and the window is counted from it.
        while events::total(TcpEvent::AcceptExhausted) == failed_before {
            tokio::select! {
                biased;
                accepted = &mut accept => panic!("accept returned while out of descriptors: {:?}", accepted.map(|c| c.tcp.remote)),
                () = tokio::task::yield_now() => {}
            }
        }
        let window = EXHAUSTION_BACKOFF * 3 + EXHAUSTION_BACKOFF / 2;
        let blocked = tokio::time::timeout(window, &mut accept).await;
        assert!(blocked.is_err(), "accept returned while out of descriptors");
        let failed = events::total(TcpEvent::AcceptExhausted) - failed_before;
        // Linux keeps the connection queued, so accept() is retried after 1, 2 and 3 backoffs.
        // macOS drops the connection with the first failure, and accept() then waits for the next.
        let retried = if cfg!(target_os = "linux") { 4 } else { 1 };
        assert_eq!(failed, retried, "exhausted accepts within {window:?}");
        let logged = logs.count("accept() resource exhaustion");
        assert_eq!(
            logged,
            1,
            "{}",
            String::from_utf8_lossy(&logs.0.lock().expect("log buffer"))
        );
        drop(accept);
        // What follows waits on the kernel, which a paused clock would not wait for.
        tokio::time::resume();

        // With descriptors back, Linux accepts the queued connection before any new one arrives, so
        // only the retry after the backoff can have woken accept up.
        drop(hogs);
        if cfg!(target_os = "linux") {
            let queued = tokio::time::timeout(Duration::from_secs(2), source.accept())
                .await
                .expect("the queued connection is accepted once descriptors are free")
                .expect("the listener is usable");
            assert_eq!(queued.tcp.remote, client_addr);
        }

        let mut second = TcpStream::connect(addr).unwrap();
        second.write_all(b"G").unwrap();
        let second_addr = second.local_addr().unwrap();
        let accepted = tokio::time::timeout(Duration::from_secs(2), source.accept())
            .await
            .expect("a new connection is accepted once descriptors are free")
            .expect("the listener is usable");
        assert_eq!(accepted.tcp.remote, second_addr);
    });
}
