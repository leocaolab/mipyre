//! Decides a request's client address when it arrives through a reverse proxy: only a peer the
//! deployment lists as a proxy may name the client, in `X-Forwarded-For`, and the header is read
//! from the right, past every listed proxy, to the first address that is not one.
//!
//! `Forwarded` (RFC 7239) is not read: a proxy maintains only the header it writes and passes
//! the other through as the client sent it, so reading both would trust a forged value, and the
//! common proxies write `X-Forwarded-For`.

// `core::net`: the address is a value; `std::net` is the socket half the plan never names.
use core::net::IpAddr;
use std::str::FromStr;

use http::header::{HeaderMap, HeaderValue};
use http::HeaderName;

const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

/// The proxies a deployment trusts to say who the client is. With none, which is the default,
/// the peer of the connection is the client and no header is read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Proxies(Vec<IpNetwork>);

/// One address block, `addr/prefix`; a bare address is a block of one. The address is canonical
/// (an IPv4-mapped IPv6 address is the IPv4 one) and has no bits set past the prefix. Only
/// [`Proxies::parse`] makes one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct IpNetwork {
    addr: IpAddr,
    prefix: u8,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "trusted proxy {entry:?} is not an IP address or block: expected e.g. \"10.0.0.7\", \"10.0.0.0/8\" or \"fd00::/8\""
)]
pub struct ProxyEntryError {
    pub entry: String,
}

/// Who sent a request, as [`Proxies::client`] reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientAddress<'h> {
    /// The peer is the client: it is no trusted proxy, or it is one that named no client.
    Peer,
    Forwarded(IpAddr),
    /// The walk reached a field line whose entry is no IP address; the header is ignored, so the
    /// peer is the client.
    Unreadable(&'h HeaderValue),
}

impl IpNetwork {
    fn parse(entry: &str) -> Option<IpNetwork> {
        let (addr, prefix) = match entry.split_once('/') {
            Some((addr, prefix)) => (addr, Some(prefix)),
            None => (entry, None),
        };
        let addr = IpAddr::from_str(addr).ok()?.to_canonical();
        let width = bit_width(addr);
        let prefix = match prefix {
            Some(p) => u8::from_str(p).ok().filter(|&p| p <= width)?,
            None => width,
        };
        Some(IpNetwork {
            addr: masked(addr, prefix),
            prefix,
        })
    }

    fn contains(&self, addr: IpAddr) -> bool {
        let addr = addr.to_canonical();
        bit_width(addr) == bit_width(self.addr) && masked(addr, self.prefix) == self.addr
    }
}

fn bit_width(addr: IpAddr) -> u8 {
    match addr {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    }
}

/// Returns `addr` with every bit past the first `prefix` cleared; `prefix` is at most its width.
fn masked(addr: IpAddr, prefix: u8) -> IpAddr {
    match addr {
        IpAddr::V4(a) => {
            let mask = u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0);
            IpAddr::V4((u32::from(a) & mask).into())
        }
        IpAddr::V6(a) => {
            let mask = u128::MAX.checked_shl(128 - u32::from(prefix)).unwrap_or(0);
            IpAddr::V6((u128::from(a) & mask).into())
        }
    }
}

impl Proxies {
    /// Parses the deployment's proxies: IP addresses and CIDR blocks.
    ///
    /// # Errors
    ///
    /// Returns [`ProxyEntryError`] naming the first entry that is neither (a host name, a
    /// port, a prefix longer than the address).
    pub fn parse(entries: &[String]) -> Result<Proxies, ProxyEntryError> {
        entries
            .iter()
            .map(|entry| IpNetwork::parse(entry.trim()).ok_or_else(|| ProxyEntryError { entry: entry.clone() }))
            .collect::<Result<_, _>>()
            .map(Proxies)
    }

    /// Returns whether no proxy is trusted, the default, where every request's client is its peer.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn trusts(&self, addr: IpAddr) -> bool {
        self.0.iter().any(|net| net.contains(addr))
    }

    /// Returns who sent a request that came from `peer` with `headers`.
    ///
    /// Only a trusted peer's `X-Forwarded-For` is read, from its last entry back, each one
    /// written by the hop before, up to the first address that is no trusted proxy. An entry left
    /// of that one was written by someone untrusted and is never read, so a client cannot claim
    /// an address by sending the header itself. When every entry is a trusted proxy the leftmost
    /// one is the client.
    pub fn client<'h>(&self, peer: IpAddr, headers: &'h HeaderMap) -> ClientAddress<'h> {
        if !self.trusts(peer) {
            return ClientAddress::Peer;
        }
        let mut client = ClientAddress::Peer;
        // Field lines of one name read as one list in their order (RFC 9110 §5.3).
        for line in headers.get_all(X_FORWARDED_FOR).iter().rev() {
            let Ok(text) = line.to_str() else {
                return ClientAddress::Unreadable(line);
            };
            for entry in text.rsplit(',') {
                let Ok(addr) = IpAddr::from_str(entry.trim()) else {
                    return ClientAddress::Unreadable(line);
                };
                client = ClientAddress::Forwarded(addr);
                if !self.trusts(addr) {
                    return client;
                }
            }
        }
        client
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proxies(entries: &[&str]) -> Proxies {
        let entries: Vec<String> = entries.iter().map(|e| (*e).to_owned()).collect();
        Proxies::parse(&entries).expect("the test's entries parse")
    }

    fn forwarded_for(lines: &[&str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for line in lines {
            headers.append(
                X_FORWARDED_FOR,
                HeaderValue::from_str(line).expect("the test's line is a header value"),
            );
        }
        headers
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().expect("the test's address parses")
    }

    #[test]
    fn with_no_proxies_the_peer_is_the_client_whatever_the_header_says() {
        let headers = forwarded_for(&["203.0.113.9"]);
        assert_eq!(Proxies::default().client(ip("10.0.0.1"), &headers), ClientAddress::Peer);
    }

    #[test]
    fn a_peer_that_is_no_trusted_proxy_cannot_name_a_client() {
        let headers = forwarded_for(&["203.0.113.9"]);
        assert_eq!(
            proxies(&["10.0.0.1"]).client(ip("10.0.0.2"), &headers),
            ClientAddress::Peer
        );
    }

    #[test]
    fn a_trusted_proxy_names_the_client_it_appended() {
        let headers = forwarded_for(&["203.0.113.9"]);
        assert_eq!(
            proxies(&["10.0.0.0/8"]).client(ip("10.0.0.1"), &headers),
            ClientAddress::Forwarded(ip("203.0.113.9"))
        );
    }

    #[test]
    fn what_the_client_sent_left_of_its_own_address_is_not_read() {
        let headers = forwarded_for(&["1.2.3.4, not-an-address", "203.0.113.9"]);
        assert_eq!(
            proxies(&["10.0.0.1"]).client(ip("10.0.0.1"), &headers),
            ClientAddress::Forwarded(ip("203.0.113.9"))
        );
    }

    #[test]
    fn trusted_hops_are_walked_past_from_the_right() {
        let headers = forwarded_for(&["1.2.3.4, 203.0.113.9, 10.0.0.5", "10.0.0.6"]);
        assert_eq!(
            proxies(&["10.0.0.0/8"]).client(ip("10.0.0.1"), &headers),
            ClientAddress::Forwarded(ip("203.0.113.9"))
        );
    }

    #[test]
    fn when_every_hop_is_trusted_the_leftmost_is_the_client() {
        let headers = forwarded_for(&["10.0.0.7, 10.0.0.5"]);
        assert_eq!(
            proxies(&["10.0.0.0/8"]).client(ip("10.0.0.1"), &headers),
            ClientAddress::Forwarded(ip("10.0.0.7"))
        );
    }

    #[test]
    fn a_trusted_proxy_without_the_header_is_the_client() {
        assert_eq!(
            proxies(&["10.0.0.1"]).client(ip("10.0.0.1"), &HeaderMap::new()),
            ClientAddress::Peer
        );
    }

    #[test]
    fn an_entry_the_walk_reaches_that_is_no_address_makes_the_header_unreadable() {
        for line in [
            "unknown",
            "203.0.113.9:4711",
            "",
            "203.0.113.9,,10.0.0.5",
            "[2001:db8::1]",
        ] {
            let headers = forwarded_for(&[line]);
            assert_eq!(
                proxies(&["10.0.0.0/8"]).client(ip("10.0.0.1"), &headers),
                ClientAddress::Unreadable(&HeaderValue::from_str(line).expect("a header value")),
                "{line:?}"
            );
        }
    }

    #[test]
    fn a_line_that_is_not_text_is_unreadable() {
        let mut headers = HeaderMap::new();
        let line = HeaderValue::from_bytes(b"203.0.113.9\xff").expect("obs-text is a header value");
        headers.append(X_FORWARDED_FOR, line.clone());
        assert_eq!(
            proxies(&["10.0.0.1"]).client(ip("10.0.0.1"), &headers),
            ClientAddress::Unreadable(&line)
        );
    }

    #[test]
    fn a_block_holds_exactly_the_addresses_under_its_prefix() {
        let block = proxies(&["10.1.2.0/23"]);
        assert!(block.trusts(ip("10.1.2.0")));
        assert!(block.trusts(ip("10.1.3.255")));
        assert!(!block.trusts(ip("10.1.4.0")));
        assert!(!block.trusts(ip("10.1.1.255")));
        let v6 = proxies(&["fd00::/8"]);
        assert!(v6.trusts(ip("fdff::1")));
        assert!(!v6.trusts(ip("fe00::1")));
        assert!(!v6.trusts(ip("10.0.0.1")));
        assert!(proxies(&["0.0.0.0/0"]).trusts(ip("203.0.113.9")));
    }

    #[test]
    fn an_ipv4_mapped_peer_matches_its_ipv4_block() {
        assert!(proxies(&["10.0.0.0/8"]).trusts(ip("::ffff:10.0.0.1")));
        assert!(proxies(&["::ffff:10.0.0.1"]).trusts(ip("10.0.0.1")));
    }

    #[test]
    fn entries_that_are_no_address_or_block_are_refused_by_name() {
        for bad in [
            "proxy.local",
            "10.0.0.1:80",
            "10.0.0.0/33",
            "fd00::/129",
            "10.0.0.0/",
            "",
            "*",
        ] {
            let err = Proxies::parse(&[bad.to_owned()]).expect_err(bad);
            assert_eq!(err.entry, bad);
        }
    }

    #[test]
    fn host_bits_past_the_prefix_are_ignored() {
        assert_eq!(proxies(&["10.1.2.3/8"]), proxies(&["10.0.0.0/8"]));
    }
}
