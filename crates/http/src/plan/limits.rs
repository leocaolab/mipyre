//! Holds an app's limits and checks their values; the engine enforces them.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

/// Bounds the wait for a request head. The engine also hands it to the listener as the
/// `TCP_DEFER_ACCEPT` hold, which comes before it: Linux holds a silent connection for the SYN-ACK
/// retransmissions that cover the time asked (10 s rounds up to 15 s), and that hold costs only the
/// kernel's request socket, not an fd or a task.
pub const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Bounds the gap between a client's bytes while the engine reads off what the client still sends
/// after a connection's last answer: a client that has finished sending and is reading the answer
/// sends nothing more, so the close waits this long after its last bytes.
pub const LINGER_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

const DEFAULT_MAX_BODY_BYTES: u64 = 10 * 1024 * 1024;
/// The default matches the `websockets` library's 1 MiB rather than tungstenite's 64 MiB.
const DEFAULT_MAX_MESSAGE_BYTES: MessageByteLimit = MessageByteLimit(1024 * 1024);
const DEFAULT_REQUEST_BUDGET: RequestBudget = RequestBudget(Duration::from_secs(30));
const DEFAULT_BODY_MIN_RATE: BodyMinRate = BodyMinRate(NonZeroU64::new(1024).unwrap());
const DEFAULT_BODY_TIME_LIMIT: BodyTimeLimit = BodyTimeLimit(Duration::from_secs(600));
const DEFAULT_INTERRUPT_GRACE: InterruptGrace = InterruptGrace(Duration::from_secs(2));
const DEFAULT_SHUTDOWN_TIMEOUT: ShutdownTimeout = ShutdownTimeout(Duration::from_secs(5));
/// Each WebSocket connection holds one OS thread for its Python handler.
const DEFAULT_MAX_CONNECTIONS: NonZeroUsize = NonZeroUsize::new(1024).unwrap();
/// Bytes charged per queued WebSocket message on top of its payload, so that a flood of empty
/// messages is bounded too.
pub const MESSAGE_OVERHEAD_BYTES: u32 = 64;
/// A message budget plus [`MESSAGE_OVERHEAD_BYTES`] must fit the `u32` permit count a semaphore
/// acquire takes.
pub const MAX_MESSAGE_BYTES_LIMIT: u32 = u32::MAX - MESSAGE_OVERHEAD_BYTES;

/// The largest WebSocket message and frame, in bytes, always in 1..=[`MAX_MESSAGE_BYTES_LIMIT`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageByteLimit(u32);

impl MessageByteLimit {
    /// Parses a message size limit from Python's `int`.
    ///
    /// # Errors
    ///
    /// Returns [`WsLimitError::MessageSizeOutOfRange`] when `bytes` is outside
    /// 1..=[`MAX_MESSAGE_BYTES_LIMIT`].
    pub fn new(bytes: i64) -> Result<Self, WsLimitError> {
        u32::try_from(bytes)
            .ok()
            .filter(|b| (1..=MAX_MESSAGE_BYTES_LIMIT).contains(b))
            .map(Self)
            .ok_or(WsLimitError::MessageSizeOutOfRange(bytes))
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }

    #[inline]
    pub fn is_exceeded_by(self, len: usize) -> bool {
        len > self.as_usize()
    }

    /// Returns the permits one queue direction holds: one message of this size and its overhead.
    pub const fn budget_permits(self) -> u32 {
        self.0 + MESSAGE_OVERHEAD_BYTES
    }
}

impl std::fmt::Display for MessageByteLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A number of bytes a body may not go past; one type for every such cap, so a cap is never
/// compared against a count of something else.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteLimit(pub u64);

impl ByteLimit {
    /// Returns the limit as a buffer length, saturating at the address space.
    pub fn as_usize(self) -> usize {
        usize::try_from(self.0).unwrap_or(usize::MAX)
    }

    #[inline]
    pub fn is_exceeded_by(self, bytes: u64) -> bool {
        bytes > self.0
    }
}

/// How long the server waits for one step of a request, its whole body arriving (else 408) or
/// its handler responding (else 504). It is always above zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RequestBudget(Duration);

impl RequestBudget {
    /// Parses the setting from Python's seconds.
    ///
    /// # Errors
    ///
    /// Returns [`SecondsError`] unless `seconds` is finite, above zero and at most
    /// [`MAX_SETTING_SECS`].
    pub fn from_secs(seconds: f64) -> Result<Self, SecondsError> {
        positive_secs("request_timeout", seconds).map(Self)
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}

/// The longest any time setting may be, a year in seconds. No configured wait needs longer, and
/// the bound keeps a deadline that adds the setting to the clock from overflowing.
const MAX_SETTING_SECS: u64 = 365 * 24 * 60 * 60;

#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("{setting} must be a finite number of seconds above 0 and at most {MAX_SETTING_SECS} (365 days), got {got}")]
pub struct SecondsError {
    pub setting: &'static str,
    pub got: f64,
}

fn positive_secs(setting: &'static str, seconds: f64) -> Result<Duration, SecondsError> {
    Duration::try_from_secs_f64(seconds)
        .ok()
        .filter(|d| !d.is_zero() && *d <= Duration::from_secs(MAX_SETTING_SECS))
        .ok_or(SecondsError { setting, got: seconds })
}

/// The slowest a request body may arrive and still earn time past the request budget, in bytes
/// per second; it is always above zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BodyMinRate(NonZeroU64);

impl BodyMinRate {
    /// Parses the rate from Python's `int`.
    ///
    /// # Errors
    ///
    /// Returns [`BodyMinRateError`] unless `bytes_per_second` is above zero.
    pub fn new(bytes_per_second: i64) -> Result<Self, BodyMinRateError> {
        u64::try_from(bytes_per_second)
            .ok()
            .and_then(NonZeroU64::new)
            .map(Self)
            .ok_or(BodyMinRateError(bytes_per_second))
    }

    pub const fn get(self) -> NonZeroU64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("body_min_rate must be a number of bytes per second above 0, got {0}")]
pub struct BodyMinRateError(pub i64);

/// The most time a request body may take, whatever it earns by arriving; it is always above zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BodyTimeLimit(Duration);

impl BodyTimeLimit {
    /// Parses the setting from Python's seconds.
    ///
    /// # Errors
    ///
    /// Returns [`SecondsError`] unless `seconds` is finite, above zero and at most
    /// [`MAX_SETTING_SECS`].
    pub fn from_secs(seconds: f64) -> Result<Self, SecondsError> {
        positive_secs("body_time_limit", seconds).map(Self)
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}

/// How long an interrupted sync request's worker has to end it before it is abandoned; it is
/// always above zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct InterruptGrace(Duration);

impl InterruptGrace {
    /// Parses the setting from Python's seconds.
    ///
    /// # Errors
    ///
    /// Returns [`SecondsError`] unless `seconds` is finite, above zero and at most
    /// [`MAX_SETTING_SECS`].
    pub fn from_secs(seconds: f64) -> Result<Self, SecondsError> {
        positive_secs("interrupt_grace", seconds).map(Self)
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}

/// How long a shutdown waits for each of the app's threads to end; it is always above zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShutdownTimeout(Duration);

impl ShutdownTimeout {
    /// Parses the setting from Python's seconds.
    ///
    /// # Errors
    ///
    /// Returns [`SecondsError`] unless `seconds` is finite, above zero and at most
    /// [`MAX_SETTING_SECS`].
    pub fn from_secs(seconds: f64) -> Result<Self, SecondsError> {
        positive_secs("shutdown_timeout", seconds).map(Self)
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}

/// How long a request body may take to arrive: `budget`, plus one second for every `min_rate`
/// bytes received, and never more than `cap`.
///
/// A client sending at `min_rate` or faster is never cut off for being slow, while holding a
/// connection for `cap` costs a client at least `cap × min_rate` bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BodyTimeAllowance {
    pub budget: RequestBudget,
    pub min_rate: BodyMinRate,
    pub cap: BodyTimeLimit,
}

impl BodyTimeAllowance {
    /// Allows `budget` and no more, whatever arrives.
    pub const fn absolute(budget: RequestBudget) -> Self {
        BodyTimeAllowance {
            budget,
            min_rate: BodyMinRate(NonZeroU64::MAX),
            cap: BodyTimeLimit(budget.get()),
        }
    }

    /// Returns the time a body has in total once `received` bytes of it have come in.
    pub fn allowed(&self, received: u64) -> Duration {
        let earned = Duration::from_secs(received / self.min_rate.get().get());
        self.budget.get().saturating_add(earned).min(self.cap.get())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub max_body: ByteLimit,
    pub request_budget: RequestBudget,
    pub body_min_rate: BodyMinRate,
    pub body_time_limit: BodyTimeLimit,
    pub interrupt_grace: InterruptGrace,
    pub shutdown_timeout: ShutdownTimeout,
    pub ws: WsLimits,
}

impl Limits {
    pub const DEFAULT: Self = Self {
        max_body: ByteLimit(DEFAULT_MAX_BODY_BYTES),
        request_budget: DEFAULT_REQUEST_BUDGET,
        body_min_rate: DEFAULT_BODY_MIN_RATE,
        body_time_limit: DEFAULT_BODY_TIME_LIMIT,
        interrupt_grace: DEFAULT_INTERRUPT_GRACE,
        shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        ws: WsLimits::DEFAULT,
    };

    pub fn body_time(&self) -> BodyTimeAllowance {
        BodyTimeAllowance {
            budget: self.request_budget,
            min_rate: self.body_min_rate,
            cap: self.body_time_limit,
        }
    }

    /// Returns one line for each limit a worker's script set differently from the served app.
    ///
    /// A worker's app is never served, so such a setting has no effect and the worker says so.
    pub fn ignored_in_worker(&self, in_worker: &Limits) -> Vec<String> {
        let mut ignored = Vec::new();
        let mut differ = |call: &str, worker: u64, served: u64| {
            if worker != served {
                ignored.push(format!(
                    "{call}({worker}) in a worker is ignored: limits are per app, and the app \
                     served is the main interpreter's, which has {served}"
                ));
            }
        };
        differ("set_max_body_size", in_worker.max_body.0, self.max_body.0);
        differ(
            "set_max_websocket_message_size",
            u64::from(in_worker.ws.max_message_bytes.get()),
            u64::from(self.ws.max_message_bytes.get()),
        );
        differ(
            "set_body_min_rate",
            in_worker.body_min_rate.get().get(),
            self.body_min_rate.get().get(),
        );
        differ(
            "set_max_websocket_connections",
            in_worker.ws.max_connections.get() as u64,
            self.ws.max_connections.get() as u64,
        );
        if in_worker.request_budget != self.request_budget {
            ignored.push(format!(
                "set_request_timeout({:?}) in a worker is ignored: limits are per app, and the \
                 app served is the main interpreter's, which has {:?}",
                in_worker.request_budget.get(),
                self.request_budget.get(),
            ));
        }
        if in_worker.body_time_limit != self.body_time_limit {
            ignored.push(format!(
                "set_body_time_limit({:?}) in a worker is ignored: limits are per app, and the \
                 app served is the main interpreter's, which has {:?}",
                in_worker.body_time_limit.get(),
                self.body_time_limit.get(),
            ));
        }
        if in_worker.shutdown_timeout != self.shutdown_timeout {
            ignored.push(format!(
                "set_shutdown_timeout({:?}) in a worker is ignored: limits are per app, and the \
                 app served is the main interpreter's, which has {:?}",
                in_worker.shutdown_timeout.get(),
                self.shutdown_timeout.get(),
            ));
        }
        if in_worker.interrupt_grace != self.interrupt_grace {
            ignored.push(format!(
                "set_interrupt_grace({:?}) in a worker is ignored: limits are per app, and the \
                 app served is the main interpreter's, which has {:?}",
                in_worker.interrupt_grace.get(),
                self.interrupt_grace.get(),
            ));
        }
        ignored
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WsLimits {
    pub max_message_bytes: MessageByteLimit,
    pub max_connections: NonZeroUsize,
}

impl WsLimits {
    pub const DEFAULT: Self = Self {
        max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
        max_connections: DEFAULT_MAX_CONNECTIONS,
    };
}

#[derive(Debug, thiserror::Error)]
pub enum WsLimitError {
    #[error("max_websocket_message_size must be between 1 and {MAX_MESSAGE_BYTES_LIMIT} bytes, got {0}")]
    MessageSizeOutOfRange(i64),
    #[error("max_websocket_connections must be at least 1, got {0}")]
    ConnectionLimitOutOfRange(i64),
}

/// Parses a WebSocket connection limit from Python's `int`.
///
/// # Errors
///
/// Returns [`WsLimitError::ConnectionLimitOutOfRange`] when `connections` is below 1.
pub fn connection_limit(connections: i64) -> Result<NonZeroUsize, WsLimitError> {
    usize::try_from(connections)
        .ok()
        .and_then(NonZeroUsize::new)
        .ok_or(WsLimitError::ConnectionLimitOutOfRange(connections))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn thirty_seconds_a_kib_up_to_100() -> BodyTimeAllowance {
        BodyTimeAllowance {
            budget: RequestBudget::from_secs(30.0).unwrap(),
            min_rate: BodyMinRate::new(1024).unwrap(),
            cap: BodyTimeLimit::from_secs(100.0).unwrap(),
        }
    }

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn a_body_earns_a_second_per_min_rate_bytes() {
        let time = thirty_seconds_a_kib_up_to_100();
        assert_eq!(time.allowed(0), secs(30));
        assert_eq!(time.allowed(1023), secs(30), "one byte short of a second");
        assert_eq!(time.allowed(1024), secs(31), "exactly a second's worth");
        assert_eq!(time.allowed(10 * 1024), secs(40));
    }

    #[test]
    fn a_body_earns_no_more_than_the_cap() {
        let time = thirty_seconds_a_kib_up_to_100();
        assert_eq!(time.allowed(69 * 1024 + 1023), secs(99), "a byte short of the cap");
        assert_eq!(time.allowed(70 * 1024), secs(100), "at the cap");
        assert_eq!(time.allowed(71 * 1024), secs(100), "past what the cap allows");
        assert_eq!(time.allowed(u64::MAX), secs(100));
    }

    #[test]
    fn an_apps_body_time_is_its_budget_rate_and_limit() {
        let limits = Limits {
            request_budget: RequestBudget::from_secs(1.5).unwrap(),
            body_min_rate: BodyMinRate::new(4096).unwrap(),
            body_time_limit: BodyTimeLimit::from_secs(2.0).unwrap(),
            ..Limits::DEFAULT
        };
        let time = limits.body_time();
        assert_eq!(
            (time.budget.get(), time.min_rate.get().get(), time.cap.get()),
            (Duration::from_millis(1500), 4096, secs(2))
        );
        assert_eq!(time.allowed(0), Duration::from_millis(1500));
        assert_eq!(time.allowed(4096), secs(2));
        assert_eq!(time.allowed(15 * 4096), secs(2));
    }

    proptest::proptest! {
        #[test]
        fn the_allowance_is_the_budget_plus_whole_seconds_earned_up_to_the_cap(
            budget_ms in 1u64..100_000,
            min_rate in 1u64..1 << 20,
            over_cap_ms in 0u64..1_000_000,
            received in 0u64..1 << 40,
        ) {
            let budget = Duration::from_millis(budget_ms);
            let cap = budget + Duration::from_millis(over_cap_ms);
            let time = BodyTimeAllowance {
                budget: RequestBudget::from_secs(budget.as_secs_f64()).unwrap(),
                min_rate: BodyMinRate::new(min_rate as i64).unwrap(),
                cap: BodyTimeLimit::from_secs(cap.as_secs_f64()).unwrap(),
            };
            let allowed = time.allowed(received);
            proptest::prop_assert_eq!(allowed, (budget + secs(received / min_rate)).min(cap));
            proptest::prop_assert!(time.allowed(received + 1) >= allowed);
            let next = time.allowed(received + min_rate);
            proptest::prop_assert!(next - allowed <= secs(1), "{:?} → {:?}", allowed, next);
            proptest::prop_assert!(allowed <= cap);
        }
    }

    #[test]
    fn an_absolute_budget_earns_nothing() {
        let budget = RequestBudget::from_secs(30.0).unwrap();
        assert_eq!(BodyTimeAllowance::absolute(budget).allowed(u64::MAX), secs(30));
    }

    #[test]
    fn limits_reject_out_of_range_values() {
        assert!(MessageByteLimit::new(0).is_err());
        assert!(MessageByteLimit::new(-1).is_err());
        assert!(MessageByteLimit::new(MAX_MESSAGE_BYTES_LIMIT as i64 + 1).is_err());
        assert_eq!(MessageByteLimit::new(4096).unwrap().get(), 4096);
        assert!(connection_limit(0).is_err());
        assert!(connection_limit(-1).is_err());
        assert_eq!(connection_limit(3).unwrap().get(), 3);
        for seconds in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e30] {
            assert_eq!(
                RequestBudget::from_secs(seconds).map_err(|e| e.to_string()),
                Err(SecondsError {
                    setting: "request_timeout",
                    got: seconds
                }
                .to_string()),
            );
        }
        assert_eq!(RequestBudget::from_secs(0.5).unwrap().get(), Duration::from_millis(500));
    }

    #[test]
    fn the_default_interrupt_grace_is_2_s_and_out_of_range_values_are_refused() {
        assert_eq!(Limits::DEFAULT.interrupt_grace.get(), Duration::from_secs(2));
        for seconds in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e30] {
            assert!(InterruptGrace::from_secs(seconds).is_err());
        }
        assert_eq!(
            InterruptGrace::from_secs(0.25).unwrap().get(),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn the_default_shutdown_timeout_is_5_s_and_out_of_range_values_are_refused() {
        assert_eq!(Limits::DEFAULT.shutdown_timeout.get(), Duration::from_secs(5));
        for seconds in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e30] {
            assert!(ShutdownTimeout::from_secs(seconds).is_err());
        }
        assert_eq!(
            ShutdownTimeout::from_secs(0.2).unwrap().get(),
            Duration::from_millis(200)
        );
    }

    /// Every time setting takes a year exactly and refuses the next value up, naming itself
    /// and the value it was given: a deadline that adds the setting to the clock cannot overflow.
    #[test]
    fn every_time_setting_takes_a_year_and_refuses_anything_longer() {
        let year = MAX_SETTING_SECS as f64;
        type FromSecs = fn(f64) -> Result<Duration, SecondsError>;
        let settings: [(&str, FromSecs); 4] = [
            ("request_timeout", |s| {
                RequestBudget::from_secs(s).map(RequestBudget::get)
            }),
            ("body_time_limit", |s| {
                BodyTimeLimit::from_secs(s).map(BodyTimeLimit::get)
            }),
            ("interrupt_grace", |s| {
                InterruptGrace::from_secs(s).map(InterruptGrace::get)
            }),
            ("shutdown_timeout", |s| {
                ShutdownTimeout::from_secs(s).map(ShutdownTimeout::get)
            }),
        ];
        for (setting, from_secs) in settings {
            assert_eq!(from_secs(year), Ok(Duration::from_secs(MAX_SETTING_SECS)), "{setting}");
            for over in [year.next_up(), year + 1.0] {
                assert_eq!(from_secs(over), Err(SecondsError { setting, got: over }), "{setting}");
            }
        }
    }

    /// The refusal says what the bound is, after the words a Python caller already matches on.
    #[test]
    fn a_setting_past_a_year_is_refused_with_the_bound_in_its_words() {
        let refused = ShutdownTimeout::from_secs(1e12).expect_err("past a year");
        assert_eq!(
            refused.to_string(),
            "shutdown_timeout must be a finite number of seconds above 0 and at most 31536000 \
             (365 days), got 1000000000000"
        );
    }

    #[test]
    fn the_default_request_budget_is_30_s() {
        assert_eq!(Limits::DEFAULT.request_budget.get(), Duration::from_secs(30));
    }

    #[test]
    fn the_body_time_settings_have_defaults_and_refuse_out_of_range_values() {
        assert_eq!(Limits::DEFAULT.body_min_rate.get().get(), 1024);
        assert_eq!(Limits::DEFAULT.body_time_limit.get(), Duration::from_secs(600));
        for rate in [0, -1] {
            assert_eq!(BodyMinRate::new(rate), Err(BodyMinRateError(rate)));
        }
        assert_eq!(BodyMinRate::new(100).unwrap().get().get(), 100);
        for seconds in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e30] {
            assert!(BodyTimeLimit::from_secs(seconds).is_err());
        }
        assert_eq!(
            BodyTimeLimit::from_secs(1.5).unwrap().get(),
            Duration::from_millis(1500)
        );
    }
}

/// Proves a body's time for every allowance and byte count with Kani.
#[cfg(kani)]
mod proofs {
    use super::*;

    fn any_duration() -> Duration {
        let duration = Duration::new(kani::any(), kani::any_where(|n: &u32| *n < 1_000_000_000));
        kani::assume(!duration.is_zero());
        duration
    }

    #[kani::proof]
    fn a_body_is_allowed_its_budget_and_never_past_its_cap() {
        let time = BodyTimeAllowance {
            budget: RequestBudget(any_duration()),
            min_rate: BodyMinRate(kani::any()),
            cap: BodyTimeLimit(any_duration()),
        };
        let allowed = time.allowed(kani::any());
        assert!(allowed <= time.cap.get());
        assert!(allowed >= time.budget.get().min(time.cap.get()));
    }
}
