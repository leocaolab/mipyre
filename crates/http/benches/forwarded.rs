//! Measures how a request's client address is decided, which every request a handler gets pays.
use std::alloc::System;
use std::hint::black_box;
use std::net::IpAddr;

use hyper::header::{HeaderMap, HeaderValue};
use mipyre_http::plan::forwarded::Proxies;
use mipyre_microbench::{measure, run, AllocationCounter, Case, Must};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

const PROXY: &str = "10.0.0.1";

/// Decides the client of a request from `peer` carrying `forwarded_for`, trusting `proxies`.
fn client(name: &str, proxies: &[&str], peer: &str, forwarded_for: Option<&'static str>, n: u64) {
    let entries: Vec<String> = proxies.iter().map(|p| (*p).to_owned()).collect();
    let proxies = Proxies::parse(&entries).must("valid proxies");
    let peer: IpAddr = peer.parse().must("an IP address");
    let mut headers = HeaderMap::new();
    headers.insert("host", HeaderValue::from_static("127.0.0.1:8000"));
    if let Some(line) = forwarded_for {
        headers.insert("x-forwarded-for", HeaderValue::from_static(line));
    }
    measure(name, n, |_| {
        black_box(black_box(&proxies).client(black_box(peer), black_box(&headers)));
    });
}

/// Decides with no proxy trusted, the default: no header is read.
fn off(n: u64) {
    client("off", &[], PROXY, Some("203.0.113.9"), n);
}

/// Decides for a peer that is no trusted proxy and sent the header itself.
fn untrusted_peer(n: u64) {
    client(
        "untrusted_peer",
        &["10.0.0.0/8"],
        "203.0.113.7",
        Some("198.51.100.1"),
        n,
    );
}

/// Decides behind one proxy that appended the client.
fn one_hop(n: u64) {
    client("one_hop", &[PROXY], PROXY, Some("203.0.113.9"), n);
}

/// Decides behind two proxies, past the inner one, with an address the client forged on the left.
fn two_hops(n: u64) {
    client(
        "two_hops",
        &["10.0.0.0/8"],
        PROXY,
        Some("1.2.3.4, 203.0.113.9, 10.0.0.2"),
        n,
    );
}

/// Decides for a trusted proxy whose header holds an address with a port.
fn unreadable(n: u64) {
    client("unreadable", &[PROXY], PROXY, Some("203.0.113.9:4711"), n);
}

fn main() {
    let cases: [Case; 5] = [
        ("off", off),
        ("untrusted_peer", untrusted_peer),
        ("one_hop", one_hop),
        ("two_hops", two_hops),
        ("unreadable", unreadable),
    ];
    run(&cases);
}
