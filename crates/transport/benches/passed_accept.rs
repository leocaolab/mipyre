//! Measures what one connection costs the accept loops that share a listener, by how they share it.
//!
//! `passed` duplicates one handed-in socket into every loop, so they share one accept queue.
//! `passed-reuseport` hands each loop its own `SO_REUSEPORT` socket, as systemd does with one
//! `.socket` unit per loop and `ReusePort=yes`. `reuseport` binds one socket per loop. Each loop runs
//! on its own thread and current-thread runtime, and the bench prints the process's context
//! switches and CPU time per connection.
//!
//!   BENCH_N=20000 cargo bench -p mipyre_transport --bench passed_accept -- passed-reuseport 8
//!
//! On Linux, `strace -f -c -e trace=accept4,epoll_wait <bench binary> passed 8` counts the accepts
//! that failed with `EAGAIN`, which are the loops a connection woke for nothing.
use std::net::{SocketAddr, TcpStream};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mipyre_microbench::{calls, Must};
use mipyre_transport::driver::TransportDriver;
use mipyre_transport::endpoint::{EndpointSource, Endpoints, ListenerSet, ListenerSpec};
use mipyre_transport::tcp::listener::{create_reuseport_listener, BindOptions, BoundListeners, DEFAULT_BACKLOG};

const OPTIONS: BindOptions = BindOptions {
    backlog: DEFAULT_BACKLOG,
    defer_accept: Duration::from_secs(10),
};

/// Returns the process's context switches, voluntary and involuntary, and its user and system CPU seconds.
fn usage() -> (i64, f64) {
    // SAFETY: `rusage` holds only integers, so all zeros is a valid value.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: `RUSAGE_SELF` is a valid `who` and `ru` is a live `rusage`.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    if rc != 0 {
        eprintln!("getrusage: {}", std::io::Error::last_os_error());
        std::process::exit(1);
    }
    let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    (
        (ru.ru_nvcsw + ru.ru_nivcsw) as i64,
        secs(ru.ru_utime) + secs(ru.ru_stime),
    )
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).filter(|a| a != "--bench").collect();
    let mode = args.first().map(String::as_str).unwrap_or("passed");
    let loops: usize = args.get(1).and_then(|n| n.parse().ok()).unwrap_or(8);
    let n = usize::try_from(calls(20_000)).must("BENCH_N connections fit in a usize");
    let copies = NonZeroUsize::new(loops).must("the loop count must be at least 1");
    let loopback = SocketAddr::from(([127, 0, 0, 1], 0));
    let bound = match mode {
        "passed" => {
            let handed = std::net::TcpListener::bind(loopback).must("bind the handed socket");
            let handed = BoundListeners::passed(vec![handed], copies).must("serve the handed socket");
            Endpoints::sharing_tls(handed, None)
        }
        "passed-reuseport" => {
            let first = create_reuseport_listener(loopback, OPTIONS).must("bind the first handed socket");
            let addr = first.local_addr().must("its address");
            let mut handed = vec![first];
            for _ in 1..loops {
                handed.push(create_reuseport_listener(addr, OPTIONS).must("bind a handed socket"));
            }
            Endpoints::sharing_tls(
                BoundListeners::passed(handed, copies).must("serve the handed sockets"),
                None,
            )
        }
        "reuseport" => Endpoints::bind(
            &ListenerSet::one(ListenerSpec {
                addr: loopback,
                tls: None,
            }),
            copies,
            OPTIONS,
        )
        .must("bind the reuseport set"),
        other => {
            eprintln!("mode is `passed`, `passed-reuseport` or `reuseport`, not {other:?}");
            std::process::exit(2)
        }
    };
    let addr = bound.primary().addr;
    let accepted = Arc::new(AtomicUsize::new(0));
    for one_loop in bound.into_loops() {
        let accepted = Arc::clone(&accepted);
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .must("build the loop's runtime");
            rt.block_on(async move {
                let mut source = EndpointSource::new(one_loop).must("register the listeners");
                loop {
                    let connection = source.accept().await.must("accept");
                    drop(connection);
                    accepted.fetch_add(1, Ordering::Release);
                }
            });
        });
    }
    // Gives every loop time to park in its accept before the clock starts.
    std::thread::sleep(Duration::from_millis(200));

    let (switches_before, cpu_before) = usage();
    let started = Instant::now();
    for i in 0..n {
        let client = TcpStream::connect(addr).must("connect");
        drop(client);
        // Waits for each accept so that a connection wakes the loops alone, not batched with the next.
        while accepted.load(Ordering::Acquire) <= i {
            std::hint::spin_loop();
        }
    }
    let elapsed = started.elapsed();
    let (switches_after, cpu_after) = usage();
    let per = |x: f64| x / n as f64;
    println!(
        "passed_accept: {mode} {loops} loops, {n} connections: {:.1} us/conn wall, \
         {:.1} us/conn CPU, {:.2} context switches/conn",
        per(elapsed.as_secs_f64() * 1e6),
        per((cpu_after - cpu_before) * 1e6),
        per((switches_after - switches_before) as f64),
    );
    std::process::exit(0);
}
