//! Decides which HTTP responses are compressed and with which codec, and encodes them.
//!
//! The engine owns the buffer pool the output goes into, runs a large body on its blocking pool,
//! and logs a failed encoding before sending the body uncompressed.

use bytes::Bytes;
use http::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, VARY};

use crate::plan::response::HandlerResponse;

/// A content coding this server compresses with (RFC 9110 §8.4.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Coding {
    Gzip,
    Brotli,
}

impl Coding {
    /// The server's preference, most wanted first.
    pub const PREFERENCE: [Coding; 2] = [Coding::Brotli, Coding::Gzip];

    pub fn name(self) -> &'static str {
        match self {
            Coding::Gzip => "gzip",
            Coding::Brotli => "br",
        }
    }

    pub fn content_encoding(self) -> HeaderValue {
        HeaderValue::from_static(self.name())
    }

    /// Returns what a pre-compressed copy's file name adds to the original's.
    pub fn file_suffix(self) -> &'static str {
        match self {
            Coding::Gzip => ".gz",
            Coding::Brotli => ".br",
        }
    }
}

/// A gzip compression level, 1..=9.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GzipLevel(u8);

impl GzipLevel {
    /// # Errors
    ///
    /// Returns an error when `level` is outside 1..=9.
    pub fn new(level: i64) -> Result<Self, CompressionSettingsError> {
        match u8::try_from(level) {
            Ok(level @ 1..=9) => Ok(Self(level)),
            _ => Err(CompressionSettingsError::GzipLevelOutOfRange(level)),
        }
    }

    pub fn get(self) -> u8 {
        self.0
    }
}

/// A brotli quality, 0..=11.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BrotliQuality(u8);

impl BrotliQuality {
    /// At or above this quality brotli is slow enough (about 1 MB/s at 11) that even a small body
    /// is compressed off the serving thread.
    const SLOW: u8 = 10;

    /// # Errors
    ///
    /// Returns an error when `quality` is outside 0..=11.
    pub fn new(quality: i64) -> Result<Self, CompressionSettingsError> {
        match u8::try_from(quality) {
            Ok(quality @ 0..=11) => Ok(Self(quality)),
            _ => Err(CompressionSettingsError::BrotliQualityOutOfRange(quality)),
        }
    }

    pub fn get(self) -> u8 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CompressionSettingsError {
    #[error("min_size must be 0..={max} bytes, got {0}", max = u32::MAX)]
    MinSizeOutOfRange(i64),
    #[error("gzip_level must be 1..=9, got {0}")]
    GzipLevelOutOfRange(i64),
    #[error("brotli_quality must be 0..=11, got {0}")]
    BrotliQualityOutOfRange(i64),
    #[error(
        "gzip=False and brotli=False enables no algorithm; use disable_compression() to turn \
         compression off"
    )]
    NoCodec,
}

/// A coding at the level it runs at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    Gzip(GzipLevel),
    Brotli(BrotliQuality),
}

impl Codec {
    pub fn coding(self) -> Coding {
        match self {
            Codec::Gzip(_) => Coding::Gzip,
            Codec::Brotli(_) => Coding::Brotli,
        }
    }
}

/// The codecs an app compresses with, each at its level; it never holds none.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnabledCodecs {
    Gzip(GzipLevel),
    Brotli(BrotliQuality),
    Both(GzipLevel, BrotliQuality),
}

impl EnabledCodecs {
    pub fn coding_set(self) -> CodingSet {
        CodingSet::of(|c| self.codec(c).is_some())
    }

    /// Returns `coding` at this app's level for it, or `None` when the app does not use it.
    pub fn codec(self, coding: Coding) -> Option<Codec> {
        match (self, coding) {
            (EnabledCodecs::Gzip(level) | EnabledCodecs::Both(level, _), Coding::Gzip) => Some(Codec::Gzip(level)),
            (EnabledCodecs::Brotli(quality) | EnabledCodecs::Both(_, quality), Coding::Brotli) => {
                Some(Codec::Brotli(quality))
            }
            _ => None,
        }
    }
}

/// An app's compression settings; the fields are public because each field's type holds its own
/// bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompressionSettings {
    /// The smallest body that is compressed, in bytes.
    pub min_size: u32,
    pub codecs: EnabledCodecs,
}

impl CompressionSettings {
    /// Parses the arguments of `Compression(...)`, checking the level of an unused codec too.
    ///
    /// # Errors
    ///
    /// Returns an error when a number is out of range or neither codec is enabled.
    pub fn parse(
        min_size: i64,
        gzip: bool,
        brotli: bool,
        gzip_level: i64,
        brotli_quality: i64,
    ) -> Result<Self, CompressionSettingsError> {
        let min_size = u32::try_from(min_size).map_err(|_| CompressionSettingsError::MinSizeOutOfRange(min_size))?;
        let level = GzipLevel::new(gzip_level)?;
        let quality = BrotliQuality::new(brotli_quality)?;
        let codecs = match (gzip, brotli) {
            (true, true) => EnabledCodecs::Both(level, quality),
            (true, false) => EnabledCodecs::Gzip(level),
            (false, true) => EnabledCodecs::Brotli(quality),
            (false, false) => return Err(CompressionSettingsError::NoCodec),
        };
        Ok(Self { min_size, codecs })
    }
}

/// A set of codings, such as the ones an app compresses with or the ones a file has a
/// pre-compressed copy of.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CodingSet {
    gzip: bool,
    brotli: bool,
}

impl CodingSet {
    pub const ALL: CodingSet = CodingSet {
        gzip: true,
        brotli: true,
    };

    pub fn only(coding: Coding) -> CodingSet {
        CodingSet::of(|c| c == coding)
    }

    /// Returns the set of the codings `has` admits.
    pub fn of(has: impl Fn(Coding) -> bool) -> CodingSet {
        CodingSet {
            gzip: has(Coding::Gzip),
            brotli: has(Coding::Brotli),
        }
    }

    pub fn contains(self, coding: Coding) -> bool {
        match coding {
            Coding::Gzip => self.gzip,
            Coding::Brotli => self.brotli,
        }
    }
}

/// Returns the coding the server prefers among the `allowed` ones that `accept_encoding` accepts.
pub fn negotiate(accept_encoding: &str, allowed: CodingSet) -> Option<Coding> {
    accepted(accept_encoding).first(|coding| allowed.contains(coding).then_some(coding))
}

/// Holds the q-value a client's `Accept-Encoding` gives each coding; a coding neither named nor
/// covered by `*` holds [`UNNAMED`].
#[derive(Clone, Copy, Debug)]
struct CodingWeights {
    br_q: f32,
    gz_q: f32,
}

impl CodingWeights {
    /// Returns what `pick` gives for the first coding, in server preference, that the client
    /// accepts and `pick` takes.
    fn first<T>(self, pick: impl Fn(Coding) -> Option<T>) -> Option<T> {
        Coding::PREFERENCE.into_iter().find_map(|coding| {
            let q = match coding {
                Coding::Brotli => self.br_q,
                Coding::Gzip => self.gz_q,
            };
            if q > 0.0 {
                pick(coding)
            } else {
                None
            }
        })
    }
}

/// The weight of a coding the header does not name. No client can send it, because [`q_value`]
/// returns only weights from 0 to 1.
const UNNAMED: f32 = -1.0;

fn accepted(accept_encoding: &str) -> CodingWeights {
    // A coding named twice keeps its highest q-value. `identity` is not negotiated, because
    // with no shared coding the body goes out uncompressed anyway.
    let mut br_q = UNNAMED;
    let mut gz_q = UNNAMED;
    let mut star_q = UNNAMED;

    for raw in accept_encoding.split(',') {
        let part = raw.trim();
        if part.is_empty() {
            continue;
        }
        let (name, q) = match part.split_once(';') {
            Some((n, rest)) => {
                let mut q = 1.0f32;
                for param in rest.split(';') {
                    let p = param.trim();
                    if let Some(value) = strip_prefix_ignore_ascii_case(p, "q=") {
                        q = q_value(value);
                        // The first `q=` wins, so a later duplicate cannot flip the preference.
                        break;
                    }
                }
                (n.trim(), q)
            }
            None => (part, 1.0),
        };
        if name.eq_ignore_ascii_case("br") {
            br_q = br_q.max(q);
        } else if name.eq_ignore_ascii_case("gzip") || name.eq_ignore_ascii_case("x-gzip") {
            gz_q = gz_q.max(q);
        } else if name == "*" {
            star_q = star_q.max(q);
        }
    }

    if br_q < 0.0 {
        br_q = star_q;
    }
    if gz_q < 0.0 {
        gz_q = star_q;
    }
    CodingWeights { br_q, gz_q }
}

/// Returns the weight a `q=` parameter gives, from 0 to 1 (RFC 9110 section 12.4.2); a value
/// that is not a number in that range disables the coding rather than preferring it.
// Out of line, it keeps the loop of a header with no `q=` as short as it was before the range check.
#[inline(never)]
fn q_value(text: &str) -> f32 {
    match text.trim().parse::<f32>() {
        Ok(q) if (0.0..=1.0).contains(&q) => q,
        Ok(_) | Err(_) => 0.0,
    }
}

/// Strips an ASCII `prefix` case-insensitively; it compares bytes, so a multi-byte character
/// across the prefix length is a mismatch, never a panic.
fn strip_prefix_ignore_ascii_case<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let head = s.as_bytes().get(..prefix.len())?;
    // The ASCII prefix matched, so `prefix.len()` is a char boundary of `s`.
    head.eq_ignore_ascii_case(prefix.as_bytes()).then(|| &s[prefix.len()..])
}

fn is_compressible(content_type: &str) -> bool {
    let ct = content_type.split(';').next().unwrap_or(content_type).trim();
    if strip_prefix_ignore_ascii_case(ct, "text/").is_some_and(|subtype| !subtype.is_empty()) {
        return true;
    }
    // The framework always emits these lowercase; a mixed-case one from a handler only goes
    // out uncompressed.
    matches!(
        ct,
        "application/json"
            | "application/javascript"
            | "application/xml"
            | "application/xhtml+xml"
            | "application/rss+xml"
            | "application/atom+xml"
            | "application/x-javascript"
            | "application/ld+json"
            | "application/manifest+json"
            | "image/svg+xml"
    )
}

/// Keeps one gzip stream from body to body and resets it for each, instead of building one per
/// response (eight allocations and fresh tables). It holds zlib's window and hash chains, about
/// 260 KiB at level 6; a new level rebuilds it.
#[derive(Default)]
pub struct GzipStream {
    kept: Option<(GzipLevel, flate2::Compress)>,
}

impl GzipStream {
    /// Appends `data` gzipped at `level` to `out`.
    ///
    /// # Errors
    ///
    /// Returns an error when zlib fails; `out` may then hold partial output after what it held.
    pub fn encode(&mut self, data: &[u8], level: GzipLevel, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        gzip_compress(self.reset_at(level), data, out).map_err(|source| EncodeError {
            codec: Codec::Gzip(level),
            source,
        })
    }

    fn reset_at(&mut self, level: GzipLevel) -> &mut flate2::Compress {
        let stream = match self.kept.take() {
            Some((kept, mut stream)) if kept == level => {
                stream.reset();
                stream
            }
            _ => flate2::Compress::new_gzip(flate2::Compression::new(level.0.into()), GZIP_WINDOW_BITS),
        };
        &mut self.kept.insert((level, stream)).1
    }
}

/// Appends `data` brotli-encoded at `quality` to `out`.
///
/// # Errors
///
/// Returns an error when the encoder fails; `out` may then hold partial output after what it held.
pub fn brotli(data: &[u8], quality: BrotliQuality, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    brotli_compress(data, quality, out).map_err(|source| EncodeError {
        codec: Codec::Brotli(quality),
        source,
    })
}

/// Gzip's largest window, 32 KiB, which `GzEncoder` used too.
const GZIP_WINDOW_BITS: u8 = 15;

/// Returns zlib's looser `deflateBound` (the one that assumes no stream parameters) plus the gzip
/// header and trailer.
fn gzip_bound(len: usize) -> usize {
    len.saturating_add(len.div_ceil(8))
        .saturating_add(len.div_ceil(64))
        .saturating_add(5 + 18)
}

/// Reserves the bound first: with that much room, zlib's `deflate` contract is to finish the
/// stream in one call.
fn gzip_compress(stream: &mut flate2::Compress, data: &[u8], out: &mut Vec<u8>) -> Result<(), CodecError> {
    out.reserve(gzip_bound(data.len()));
    let status = stream
        .compress_vec(data, out, flate2::FlushCompress::Finish)
        .map_err(CodecError::GzipRefused)?;
    if status == flate2::Status::StreamEnd {
        Ok(())
    } else {
        Err(CodecError::GzipNotFinished {
            status,
            len: data.len(),
        })
    }
}

/// Hands the encoder the whole body in one finishing call, so it sizes its hash table for the
/// body alone. Streamed through `BrotliCompress`'s 4 KiB reads, it cleared the whole table for
/// every response, nearly half of a small JSON body's encode; the output bytes are the same.
fn brotli_compress(data: &[u8], quality: BrotliQuality, out: &mut Vec<u8>) -> Result<(), CodecError> {
    use brotli::enc::encode::{
        BrotliEncoderDestroyInstance, BrotliEncoderMaxCompressedSize, BrotliEncoderOperation, BrotliEncoderParameter,
        BrotliEncoderStateStruct,
    };
    let mut encoder = BrotliEncoderStateStruct::new(brotli::enc::StandardAlloc::default());
    // The encoder caps the size hint at 1 GiB, so a saturated one loses nothing.
    let size_hint = u32::try_from(data.len()).unwrap_or(u32::MAX);
    // A new encoder accepts any parameter; it refuses one only once it has started.
    let configured = encoder.set_parameter(BrotliEncoderParameter::BROTLI_PARAM_QUALITY, quality.0.into())
        && encoder.set_parameter(BrotliEncoderParameter::BROTLI_PARAM_SIZE_HINT, size_hint);
    let start = out.len();
    out.resize(start + BrotliEncoderMaxCompressedSize(data.len()), 0);
    let (mut available_in, mut taken, mut written) = (data.len(), 0, start);
    let finished = configured
        && loop {
            let mut available_out = out.len() - written;
            if !encoder.compress_stream(
                BrotliEncoderOperation::BROTLI_OPERATION_FINISH,
                &mut available_in,
                data,
                &mut taken,
                &mut available_out,
                out,
                &mut written,
                &mut None,
                &mut |_, _, _, _| (),
            ) {
                break false;
            }
            if encoder.is_finished() {
                break true;
            }
            // An encoder that returns unfinished with room left would make no progress.
            if available_out != 0 {
                break false;
            }
            // The bound is the body stored uncompressed, which the encoder's own try can pass.
            out.resize(out.len() * 2, 0);
        };
    BrotliEncoderDestroyInstance(&mut encoder);
    out.truncate(written);
    if finished {
        Ok(())
    } else {
        Err(CodecError::BrotliNotFinished { taken, len: data.len() })
    }
}

#[derive(Debug, thiserror::Error)]
enum CodecError {
    #[error(transparent)]
    GzipRefused(flate2::CompressError),
    #[error("the gzip stream of a {len}-byte body stopped unfinished ({status:?}) with room for its bound")]
    GzipNotFinished { status: flate2::Status, len: usize },
    #[error("the brotli encoder stopped unfinished, having taken {taken} of the body's {len} bytes")]
    BrotliNotFinished { taken: usize, len: usize },
}

#[derive(Debug, thiserror::Error)]
#[error("{source}")]
pub struct EncodeError {
    pub codec: Codec,
    source: CodecError,
}

/// Marks `headers` as carrying a body compressed with `coding`; it drops a handler's
/// `Content-Length`, which counted the uncompressed body.
fn set_compression_headers(headers: &mut HeaderMap, coding: Coding) {
    headers.insert(CONTENT_ENCODING, coding.content_encoding());
    headers.remove(CONTENT_LENGTH);
    if headers.get_all(VARY).iter().any(names_accept_encoding) {
        return;
    }
    // A cache reads a separate `Vary` line the same as one merged into a lone line.
    let mut lines = headers.get_all(VARY).iter();
    let merged = match (lines.next(), lines.next()) {
        (Some(only), None) => only.to_str().ok().and_then(|v| {
            let v = v.trim_end().trim_end_matches(',').trim_end();
            HeaderValue::from_str(&format!("{v}, Accept-Encoding")).ok()
        }),
        _ => None,
    };
    if let Some(vary) = merged {
        headers.insert(VARY, vary);
    } else {
        headers.append(VARY, HeaderValue::from_static("Accept-Encoding"));
    }
}

fn names_accept_encoding(vary: &HeaderValue) -> bool {
    vary.to_str().is_ok_and(|v| {
        v.split(',')
            .any(|token| token.trim().eq_ignore_ascii_case("accept-encoding"))
    })
}

/// A body at or above this size is compressed off the serving thread, because gzip-6 and brotli-4
/// take about 100 µs per 16 KiB.
pub const INLINE_MAX_BYTES: usize = 16 * 1024;

/// Says with which codec, and on which thread, a response's body is compressed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompressPlan {
    codec: Codec,
    inline: bool,
}

impl CompressPlan {
    /// Returns whether the serving thread compresses the body; otherwise the blocking pool does.
    pub fn runs_inline(self) -> bool {
        self.inline
    }

    pub fn codec(self) -> Codec {
        self.codec
    }
}

/// Returns how to compress `data`, or `None` when it goes out as is; a handler-set
/// `Content-Encoding` is never compressed again. The cheap checks come first.
pub fn plan(data: &HandlerResponse, accept_encoding: &str, settings: &CompressionSettings) -> Option<CompressPlan> {
    let len = data.body.len();
    if accept_encoding.is_empty() || len < settings.min_size as usize {
        return None;
    }
    if data.headers().contains_key(CONTENT_ENCODING) {
        return None;
    }
    if !data.content_type().to_str().is_ok_and(is_compressible) {
        return None;
    }
    let codec = accepted(accept_encoding).first(|coding| settings.codecs.codec(coding))?;
    let slow = matches!(codec, Codec::Brotli(q) if q.0 >= BrotliQuality::SLOW);
    Some(CompressPlan {
        codec,
        inline: len < INLINE_MAX_BYTES && !slow,
    })
}

/// Returns the request's `Accept-Encoding` with the app's `settings`, or `None` when either is
/// missing; with compression off the headers are not read.
#[inline]
pub fn accept_encoding<'h, 's>(
    headers: &'h HeaderMap,
    settings: Option<&'s CompressionSettings>,
) -> Option<(&'h HeaderValue, &'s CompressionSettings)> {
    let settings = settings?;
    headers.get(ACCEPT_ENCODING).map(|value| (value, settings))
}

/// Replaces the body of `data` with `compressed`, the output of `codec`, and sets its headers to
/// say so.
pub fn apply(data: &mut HandlerResponse, compressed: Bytes, codec: Codec) {
    data.body = compressed;
    set_compression_headers(data.headers_mut(), codec.coding());
}

#[cfg(test)]
mod fuzz;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::response::HeaderLines;

    fn settings(min_size: i64, gzip: bool, brotli: bool) -> CompressionSettings {
        CompressionSettings::parse(min_size, gzip, brotli, 6, 4).unwrap()
    }

    fn json(body: impl Into<Bytes>) -> HandlerResponse {
        HandlerResponse::new(
            http::StatusCode::OK,
            body.into(),
            HeaderValue::from_static("application/json"),
            HeaderLines::new(),
        )
    }

    fn decode(encoded: &[u8], codec: Codec) -> Vec<u8> {
        use std::io::Read;
        let mut decoded = Vec::new();
        match codec {
            Codec::Gzip(_) => flate2::read::GzDecoder::new(encoded).read_to_end(&mut decoded),
            Codec::Brotli(_) => brotli::Decompressor::new(encoded, 4096).read_to_end(&mut decoded),
        }
        .unwrap();
        decoded
    }

    /// Encodes through the entry `codec` takes; gzip gets a new stream each call.
    fn encode(data: &[u8], codec: Codec, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        match codec {
            Codec::Gzip(level) => GzipStream::default().encode(data, level, out),
            Codec::Brotli(quality) => brotli(data, quality, out),
        }
    }

    fn codecs() -> [Codec; 2] {
        [
            Codec::Gzip(GzipLevel::new(6).unwrap()),
            Codec::Brotli(BrotliQuality::new(4).unwrap()),
        ]
    }

    /// Builds the JSON body `benches/compression.rs` measures.
    fn json_items(items: u32) -> Vec<u8> {
        let records: Vec<String> = (0..items)
            .map(|id| format!(r#"{{"id":{id},"name":"mipyre","tags":["a","b","c"],"price":12.5,"ok":true}}"#))
            .collect();
        format!(r#"{{"items":[{}]}}"#, records.join(",")).into_bytes()
    }

    #[test]
    fn encoded_json_decodes_to_the_body() {
        for codec in codecs() {
            for items in [0, 1, 13, 64, 211, 843] {
                let body = json_items(items);
                let mut out = Vec::new();
                encode(&body, codec, &mut out).unwrap();
                // From 1 KiB on, a JSON body shrinks; the empty list does not.
                if items >= 13 {
                    assert!(
                        out.len() < body.len(),
                        "{codec:?}, {items} items: {} → {}",
                        body.len(),
                        out.len()
                    );
                }
                assert_eq!(decode(&out, codec), body, "{codec:?}, {items} items");
            }
        }
    }

    #[test]
    fn brotli_bytes_are_the_streaming_encoders() {
        let quality = BrotliQuality::new(4).unwrap();
        for items in [1, 13, 64, 211, 843] {
            let body = json_items(items);
            let mut streamed = Vec::new();
            let params = brotli::enc::BrotliEncoderParams {
                quality: 4,
                ..Default::default()
            };
            brotli::BrotliCompress(&mut &body[..], &mut streamed, &params).unwrap();
            let mut out = Vec::new();
            encode(&body, Codec::Brotli(quality), &mut out).unwrap();
            assert_eq!(out, streamed, "{items} items");
        }
    }

    /// A kept gzip stream, reset between bodies and rebuilt for another level, writes what a
    /// new one writes for each body.
    #[test]
    fn kept_gzip_stream_writes_what_a_new_one_writes() {
        let mut kept = GzipStream::default();
        for (level, items) in [(6, 64), (6, 13), (1, 211), (6, 64), (9, 0)] {
            let level = GzipLevel::new(level).unwrap();
            let body = json_items(items);
            let (mut reused, mut fresh) = (Vec::new(), Vec::new());
            kept.encode(&body, level, &mut reused).unwrap();
            GzipStream::default().encode(&body, level, &mut fresh).unwrap();
            assert_eq!(reused, fresh, "{level:?}, {items} items");
            assert_eq!(decode(&reused, Codec::Gzip(level)), body, "{level:?}, {items} items");
        }
    }

    #[test]
    fn encode_appends_after_what_the_buffer_holds() {
        let body = json_items(64);
        for codec in codecs() {
            let mut out = b"held".to_vec();
            encode(&body, codec, &mut out).unwrap();
            assert_eq!(&out[..4], b"held");
            assert_eq!(decode(&out[4..], codec), body, "{codec:?}");
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(32))]

        /// The bodies include incompressible ones, for which brotli's output is larger than the body.
        #[test]
        fn encoded_bytes_decode_to_the_body(body in proptest::collection::vec(proptest::num::u8::ANY, 0..20_000)) {
            for codec in codecs() {
                let mut out = Vec::new();
                encode(&body, codec, &mut out).unwrap();
                proptest::prop_assert_eq!(decode(&out, codec), body.clone());
            }
        }
    }

    #[test]
    fn negotiate_picks_brotli_over_gzip() {
        assert_eq!(negotiate("gzip, deflate, br", CodingSet::ALL), Some(Coding::Brotli));
    }

    #[test]
    fn negotiate_gzip_only_when_brotli_disabled() {
        assert_eq!(negotiate("gzip, br", CodingSet::only(Coding::Gzip)), Some(Coding::Gzip));
    }

    #[test]
    fn negotiate_respects_q_zero() {
        assert_eq!(negotiate("br;q=0, gzip", CodingSet::ALL), Some(Coding::Gzip));
    }

    #[test]
    fn a_q_value_from_zero_to_one_is_kept_and_one_past_either_end_is_zero() {
        assert_eq!(q_value("0"), 0.0);
        assert_eq!(q_value("1"), 1.0);
        assert_eq!(q_value("1.001"), 0.0);
        assert_eq!(q_value("-0.001"), 0.0);
        assert_eq!(q_value("NaN"), 0.0);
    }

    #[test]
    fn a_coding_named_with_an_out_of_range_q_is_refused_not_left_to_the_wildcard() {
        assert_eq!(negotiate("br;q=1, gzip;q=0.5", CodingSet::ALL), Some(Coding::Brotli));
        assert_eq!(negotiate("br;q=1.001, gzip;q=0.5", CodingSet::ALL), Some(Coding::Gzip));
        assert_eq!(negotiate("br;q=-0.001, gzip;q=0.5", CodingSet::ALL), Some(Coding::Gzip));
        assert_eq!(negotiate("br;q=NaN, gzip;q=0.5", CodingSet::ALL), Some(Coding::Gzip));
        assert_eq!(negotiate("br;q=-1, *", CodingSet::ALL), Some(Coding::Gzip));
    }

    #[test]
    fn negotiate_wildcard() {
        assert_eq!(negotiate("*", CodingSet::ALL), Some(Coding::Brotli));
    }

    #[test]
    fn negotiate_wildcard_q_zero_excludes() {
        assert_eq!(negotiate("*;q=0, gzip", CodingSet::ALL), Some(Coding::Gzip));
    }

    #[test]
    fn negotiate_none_when_unsupported() {
        assert_eq!(negotiate("deflate, compress", CodingSet::ALL), None);
    }

    #[test]
    fn negotiate_uppercase_q_param() {
        assert_eq!(negotiate("br;Q=0, gzip", CodingSet::ALL), Some(Coding::Gzip));
    }

    #[test]
    fn negotiate_uppercase_algo_names() {
        assert_eq!(negotiate("BR, GZIP", CodingSet::ALL), Some(Coding::Brotli));
    }

    #[test]
    fn negotiate_malformed_q_disables() {
        assert_eq!(negotiate("br;q=bad, gzip", CodingSet::ALL), Some(Coding::Gzip));
    }

    #[test]
    fn is_compressible_mixed_case_text() {
        assert!(is_compressible("Text/HTML"));
        assert!(is_compressible("TEXT/plain; charset=utf-8"));
    }

    #[test]
    fn is_compressible_exact_application_types() {
        assert!(is_compressible("application/json"));
        assert!(is_compressible("application/ld+json"));
        assert!(!is_compressible("application/octet-stream"));
    }

    #[test]
    fn compressible_allowlist() {
        assert!(is_compressible("text/html"));
        assert!(is_compressible("text/html; charset=utf-8"));
        assert!(is_compressible("application/json"));
        assert!(is_compressible("image/svg+xml"));
        assert!(!is_compressible("image/png"));
        assert!(!is_compressible("application/octet-stream"));
        assert!(!is_compressible("video/mp4"));
    }

    #[test]
    fn slow_brotli_quality_never_runs_inline() {
        let slow = CompressionSettings::parse(0, false, true, 6, 11).unwrap();
        let job = plan(&json(vec![b'z'; 1024]), "br", &slow).unwrap();
        assert!(!job.runs_inline());
        let fast = plan(&json(vec![b'z'; 1024]), "br", &settings(0, false, true)).unwrap();
        assert!(fast.runs_inline());
    }

    #[test]
    fn a_body_just_under_the_inline_limit_runs_inline_and_one_at_it_does_not() {
        let gzip = settings(0, true, false);
        let under = plan(&json(vec![b'z'; INLINE_MAX_BYTES - 1]), "gzip", &gzip).unwrap();
        assert!(under.runs_inline());
        let at = plan(&json(vec![b'z'; INLINE_MAX_BYTES]), "gzip", &gzip).unwrap();
        assert!(!at.runs_inline());
        assert_eq!(at.codec(), Codec::Gzip(GzipLevel(6)));
    }

    #[test]
    fn a_body_just_under_min_size_is_skipped_and_one_at_it_is_planned() {
        let s = settings(100, true, true);
        assert!(plan(&json(vec![b'x'; 99]), "gzip", &s).is_none());
        assert!(plan(&json(vec![b'x'; 100]), "gzip", &s).is_some());
    }

    #[test]
    fn the_planned_codec_is_the_preferred_one_the_app_enables_at_its_level() {
        let gzip_only = settings(0, true, false);
        let job = plan(&json(vec![b'x'; 2048]), "br, gzip", &gzip_only).unwrap();
        assert_eq!(job.codec(), Codec::Gzip(GzipLevel(6)));
        let both = settings(0, true, true);
        let job = plan(&json(vec![b'x'; 2048]), "br, gzip", &both).unwrap();
        assert_eq!(job.codec(), Codec::Brotli(BrotliQuality(4)));
        assert!(plan(&json(vec![b'x'; 2048]), "br", &gzip_only).is_none());
    }

    #[test]
    fn with_compression_off_the_accept_encoding_is_not_looked_at() {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));
        assert!(accept_encoding(&headers, None).is_none());
        let on = settings(0, true, true);
        assert_eq!(
            accept_encoding(&headers, Some(&on)).unwrap(),
            (&HeaderValue::from_static("gzip"), &on)
        );
        assert!(accept_encoding(&HeaderMap::new(), Some(&on)).is_none());
    }

    #[test]
    fn small_body_skipped() {
        let data = json("small");
        assert!(plan(&data, "gzip, br", &settings(512, true, true)).is_none());
    }

    #[test]
    fn binary_content_type_skipped() {
        let data = HandlerResponse::new(
            http::StatusCode::OK,
            vec![0u8; 2048].into(),
            HeaderValue::from_static("image/png"),
            HeaderLines::new(),
        );
        assert!(plan(&data, "gzip, br", &settings(100, true, true)).is_none());
    }

    #[test]
    fn empty_accept_encoding_noop() {
        assert!(plan(&json(vec![b'x'; 2048]), "", &settings(100, true, true)).is_none());
    }

    #[test]
    fn is_compressible_non_ascii_does_not_panic() {
        // Byte 5 falls inside the 3-byte '€': a `str[..5]` slice panics here.
        assert!(!is_compressible("tex\u{20ac}/plain"));
        assert!(!is_compressible("\u{20ac}\u{20ac}/x"));
        assert!(is_compressible("text/\u{20ac}"));
        assert!(!is_compressible("text/"));
    }

    #[test]
    fn negotiate_non_ascii_param_does_not_panic() {
        // Byte 2 falls inside the 3-byte '€' of the parameter.
        assert_eq!(negotiate("br;\u{20ac}, gzip", CodingSet::ALL), Some(Coding::Brotli));
        assert_eq!(
            negotiate("br;q\u{20ac}", CodingSet::only(Coding::Brotli)),
            Some(Coding::Brotli)
        );
    }

    #[test]
    fn levels_out_of_range_are_errors_not_clamped() {
        assert_eq!(
            CompressionSettings::parse(100, true, true, 42, 4),
            Err(CompressionSettingsError::GzipLevelOutOfRange(42))
        );
        assert_eq!(
            CompressionSettings::parse(100, true, true, 0, 4),
            Err(CompressionSettingsError::GzipLevelOutOfRange(0))
        );
        assert_eq!(
            CompressionSettings::parse(100, true, true, 6, 12),
            Err(CompressionSettingsError::BrotliQualityOutOfRange(12))
        );
        assert_eq!(
            CompressionSettings::parse(100, true, true, 6, -1),
            Err(CompressionSettingsError::BrotliQualityOutOfRange(-1))
        );
        assert_eq!(
            CompressionSettings::parse(-1, true, true, 6, 4),
            Err(CompressionSettingsError::MinSizeOutOfRange(-1))
        );
    }

    #[test]
    fn no_algorithm_is_an_error() {
        assert_eq!(
            CompressionSettings::parse(100, false, false, 6, 4),
            Err(CompressionSettingsError::NoCodec)
        );
    }

    #[test]
    fn range_edges_are_accepted() {
        let s = CompressionSettings::parse(0, true, false, 1, 0).unwrap();
        assert_eq!(s.codecs, EnabledCodecs::Gzip(GzipLevel(1)));
        let s = CompressionSettings::parse(0, false, true, 9, 11).unwrap();
        assert_eq!(s.codecs, EnabledCodecs::Brotli(BrotliQuality(11)));
        let s = CompressionSettings::parse(0, true, true, 1, 11).unwrap();
        assert_eq!(s.codecs, EnabledCodecs::Both(GzipLevel(1), BrotliQuality(11)));
    }

    fn headers_with(pairs: &[(http::header::HeaderName, &'static str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (name, value) in pairs {
            h.append(name.clone(), HeaderValue::from_static(value));
        }
        h
    }

    fn vary_lines(h: &HeaderMap) -> Vec<&str> {
        h.get_all(VARY).iter().map(|v| v.to_str().unwrap()).collect()
    }

    #[test]
    fn compression_headers_drop_the_handlers_content_length() {
        for coding in [Coding::Gzip, Coding::Brotli] {
            let mut h = headers_with(&[(CONTENT_LENGTH, "4096")]);
            set_compression_headers(&mut h, coding);
            assert!(h.get(CONTENT_LENGTH).is_none(), "{coding:?}");
            assert_eq!(h.get(CONTENT_ENCODING).unwrap(), coding.name());
            assert_eq!(vary_lines(&h), ["Accept-Encoding"]);
        }
    }

    #[test]
    fn compression_headers_merge_into_a_lone_vary_line() {
        let mut h = headers_with(&[(VARY, "Origin, ")]);
        set_compression_headers(&mut h, Coding::Gzip);
        assert_eq!(vary_lines(&h), ["Origin, Accept-Encoding"]);
    }

    #[test]
    fn compression_headers_leave_a_vary_that_names_accept_encoding() {
        let mut h = headers_with(&[(VARY, "Origin"), (VARY, "accept-encoding")]);
        set_compression_headers(&mut h, Coding::Brotli);
        assert_eq!(vary_lines(&h), ["Origin", "accept-encoding"]);
    }

    #[test]
    fn compression_headers_add_a_vary_line_beside_several() {
        let mut h = headers_with(&[(VARY, "Origin"), (VARY, "Cookie")]);
        set_compression_headers(&mut h, Coding::Gzip);
        assert_eq!(vary_lines(&h), ["Origin", "Cookie", "Accept-Encoding"]);
    }

    #[test]
    fn compress_helpers_report_success_as_a_result() {
        let data = vec![b'a'; 4096];
        let mut gz = Vec::new();
        gzip_compress(GzipStream::default().reset_at(GzipLevel(6)), &data, &mut gz).unwrap();
        assert!(!gz.is_empty() && gz.len() < data.len());
        let mut br = Vec::new();
        brotli_compress(&data, BrotliQuality(4), &mut br).unwrap();
        assert!(!br.is_empty() && br.len() < data.len());
    }
}
