//! Builds the header map a fuzz target decides on from the fuzzer's fields: each field picks one of the target's
//! header names and carries the value's bytes. A value no header can hold is left out, because the connection
//! refuses such a request before the plan sees it.

use http::header::{HeaderMap, HeaderName, HeaderValue};

pub(crate) fn header_map(names: &[HeaderName], fields: &[(u8, Vec<u8>)]) -> HeaderMap {
    fields
        .iter()
        .filter_map(|(at, value)| {
            let name = &names[usize::from(*at) % names.len()];
            HeaderValue::from_bytes(value).ok().map(|value| (name.clone(), value))
        })
        .fold(HeaderMap::new(), |mut map, (name, value)| {
            map.append(name, value);
            map
        })
}
