//! Sorts an answered request into the closed sets its count is kept by: the kind of its method, the
//! class of its status and the bucket of its duration. A client cannot add a member to any of them,
//! so the counts stay a fixed size whatever it sends.

use http::{Method, StatusCode};

/// The method of a request, one kind per standard method and one for every other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MethodKind {
    Get,
    Head,
    Post,
    Put,
    Delete,
    Patch,
    Options,
    Connect,
    Trace,
    Other,
}

impl MethodKind {
    pub const ALL: [MethodKind; 10] = [
        MethodKind::Get,
        MethodKind::Head,
        MethodKind::Post,
        MethodKind::Put,
        MethodKind::Delete,
        MethodKind::Patch,
        MethodKind::Options,
        MethodKind::Connect,
        MethodKind::Trace,
        MethodKind::Other,
    ];

    #[inline]
    pub fn of(method: &Method) -> MethodKind {
        match *method {
            Method::GET => MethodKind::Get,
            Method::HEAD => MethodKind::Head,
            Method::POST => MethodKind::Post,
            Method::PUT => MethodKind::Put,
            Method::DELETE => MethodKind::Delete,
            Method::PATCH => MethodKind::Patch,
            Method::OPTIONS => MethodKind::Options,
            Method::CONNECT => MethodKind::Connect,
            Method::TRACE => MethodKind::Trace,
            _ => MethodKind::Other,
        }
    }

    /// Returns its place in [`MethodKind::ALL`].
    #[inline]
    pub fn index(self) -> usize {
        self as usize
    }

    /// Returns the method's name, or `other` for every method no kind names.
    pub fn label(self) -> &'static str {
        match self {
            MethodKind::Get => "GET",
            MethodKind::Head => "HEAD",
            MethodKind::Post => "POST",
            MethodKind::Put => "PUT",
            MethodKind::Delete => "DELETE",
            MethodKind::Patch => "PATCH",
            MethodKind::Options => "OPTIONS",
            MethodKind::Connect => "CONNECT",
            MethodKind::Trace => "TRACE",
            MethodKind::Other => "other",
        }
    }
}

/// The hundreds digit of a status: `1` for 1xx up to `9` for 9xx, the codes a response may carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatusClass(u8);

impl StatusClass {
    pub const ALL: [StatusClass; 9] = [
        StatusClass(1),
        StatusClass(2),
        StatusClass(3),
        StatusClass(4),
        StatusClass(5),
        StatusClass(6),
        StatusClass(7),
        StatusClass(8),
        StatusClass(9),
    ];

    #[inline]
    pub fn of(status: StatusCode) -> StatusClass {
        // A `StatusCode` is 100..=999, so the quotient is 1..=9 and fits a `u8`.
        StatusClass((status.as_u16() / 100) as u8)
    }

    /// Returns its place in [`StatusClass::ALL`].
    #[inline]
    pub fn index(self) -> usize {
        usize::from(self.0 - 1)
    }

    /// Returns `1xx` up to `9xx`.
    pub fn label(self) -> &'static str {
        ["1xx", "2xx", "3xx", "4xx", "5xx", "6xx", "7xx", "8xx", "9xx"][self.index()]
    }
}

/// The upper bounds, in microseconds and inclusive, of the duration buckets but the last, which
/// takes every longer duration. They start at 100 µs, well below the common 5 ms, because most
/// requests here are answered in tens to hundreds of microseconds.
pub const DURATION_BOUNDS_US: [u64; 16] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000,
    5_000_000, 10_000_000,
];

/// The number of duration buckets: one per bound and the last, unbounded one.
pub const DURATION_BUCKETS: usize = DURATION_BOUNDS_US.len() + 1;

/// Returns the bucket a duration of `elapsed_us` falls in: the first whose bound it does not
/// exceed, or the last when it exceeds them all.
#[inline]
pub fn duration_bucket(elapsed_us: u64) -> usize {
    DURATION_BOUNDS_US.partition_point(|&bound| bound < elapsed_us)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class(code: u16) -> StatusClass {
        StatusClass::of(StatusCode::from_u16(code).unwrap())
    }

    #[test]
    fn a_status_falls_in_its_hundreds() {
        assert_eq!(class(100), StatusClass(1));
        assert_eq!(class(199), StatusClass(1));
        assert_eq!(class(200), StatusClass(2));
        assert_eq!(class(599), StatusClass(5));
        assert_eq!(class(600), StatusClass(6));
        assert_eq!(class(999), StatusClass(9));
        assert_eq!(class(100).index(), 0);
        assert_eq!(class(999).index(), StatusClass::ALL.len() - 1);
        assert_eq!((class(404).label(), class(999).label()), ("4xx", "9xx"));
    }

    #[test]
    fn each_standard_method_has_its_kind_and_others_share_one() {
        let standard = [
            Method::GET,
            Method::HEAD,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::PATCH,
            Method::OPTIONS,
            Method::CONNECT,
            Method::TRACE,
        ];
        for (method, kind) in standard.iter().zip(MethodKind::ALL) {
            assert_eq!(MethodKind::of(method), kind);
        }
        for (method, kind) in standard.iter().zip(MethodKind::ALL) {
            assert_eq!(kind.label(), method.as_str());
        }
        let propfind = Method::from_bytes(b"PROPFIND").unwrap();
        assert_eq!(MethodKind::of(&propfind), MethodKind::Other);
        for (i, kind) in MethodKind::ALL.into_iter().enumerate() {
            assert_eq!(kind.index(), i);
        }
    }

    #[test]
    fn a_duration_falls_in_the_first_bucket_it_does_not_exceed() {
        assert_eq!(duration_bucket(0), 0);
        assert_eq!(duration_bucket(100), 0);
        assert_eq!(duration_bucket(101), 1);
        assert_eq!(duration_bucket(10_000_000), 15);
        assert_eq!(duration_bucket(10_000_001), 16);
        assert_eq!(duration_bucket(u64::MAX), DURATION_BUCKETS - 1);
    }
}
