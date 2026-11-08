//! The test oracle of NUMERIC text: what `decode` answers for a number once it has gone through the wire. The
//! codec's property tests and mipyre_pg_harness's fuzz target both check against it.

/// Strips leading integer zeros and the sign of a zero, as `decode` writes a wire in
/// Postgres's own normal form.
pub(crate) fn normal_form(text: &str) -> String {
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let integer = match integer.trim_start_matches('0') {
        "" => "0",
        trimmed => trimmed,
    };
    let is_zero = integer == "0" && fraction.bytes().all(|b| b == b'0');
    let sign = if negative && !is_zero { "-" } else { "" };
    if fraction.is_empty() {
        format!("{sign}{integer}")
    } else {
        format!("{sign}{integer}.{fraction}")
    }
}
