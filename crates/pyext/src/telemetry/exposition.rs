//! Renders the metrics page in the Prometheus text format: one run's answers and counts, and the
//! counts the process keeps. Every family's name, help and type is written here and nowhere else,
//! and both inputs are taken apart field by field, so a counter added to either does not compile
//! until this page shows it or says why not.

use std::fmt::Display;

use mipyre_http::plan::answer::{MethodKind, StatusClass, DURATION_BOUNDS_US};

use super::run_counters::{AnswerCounts, RunCounts};
use super::Counters;

pub(crate) const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[derive(Clone, Copy)]
enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

impl MetricType {
    fn word(self) -> &'static str {
        match self {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
            MetricType::Histogram => "histogram",
        }
    }
}

fn header(out: &mut String, name: &str, kind: MetricType, help: &str) {
    out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {}\n", kind.word()));
}

fn sample(out: &mut String, name: &str, labels: &str, value: impl Display) {
    out.push_str(&format!("{name}{labels} {value}\n"));
}

/// Writes a family of one sample, or of one sample per `(labels, value)`.
fn family<V: Display>(
    out: &mut String,
    name: &str,
    kind: MetricType,
    help: &str,
    samples: impl IntoIterator<Item = (&'static str, V)>,
) {
    header(out, name, kind, help);
    for (labels, value) in samples {
        sample(out, name, labels, value);
    }
}

/// Returns `value` as a label value of the text format: backslash, double quote and newline
/// escaped.
fn label_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

/// Returns microseconds as seconds, exactly: the text format's unit for a duration.
fn seconds(us: u64) -> String {
    format!("{}.{:06}", us / 1_000_000, us % 1_000_000)
}

fn answers(out: &mut String, counts: &AnswerCounts) {
    let AnswerCounts {
        answers,
        duration_buckets,
        duration_sum_us,
    } = counts;

    let name = "mipyre_http_requests_total";
    header(
        out,
        name,
        MetricType::Counter,
        "HTTP requests answered, by method and status class.",
    );
    for kind in MethodKind::ALL {
        for class in StatusClass::ALL {
            let count = answers[kind.index()][class.index()];
            if count != 0 {
                let labels = format!("{{method=\"{}\",status_class=\"{}\"}}", kind.label(), class.label());
                sample(out, name, &labels, count);
            }
        }
    }

    let name = "mipyre_http_request_duration_seconds";
    header(
        out,
        name,
        MetricType::Histogram,
        "Time from a request's arrival to its response's head; a streamed body's sending is not in it.",
    );
    let bucket = format!("{name}_bucket");
    let mut below = 0;
    for (bound, count) in DURATION_BOUNDS_US.iter().zip(duration_buckets) {
        below += count;
        let le = (*bound as f64) / 1e6;
        sample(out, &bucket, &format!("{{le=\"{le}\"}}"), below);
    }
    let all: u64 = duration_buckets.iter().sum();
    sample(out, &bucket, "{le=\"+Inf\"}", all);
    sample(out, &format!("{name}_sum"), "", seconds(*duration_sum_us));
    sample(out, &format!("{name}_count"), "", all);
}

fn run_counts(out: &mut String, run: &RunCounts) {
    let RunCounts {
        answers: answer_counts,
        refused,
        workers_serving,
        workers_died,
        process_workers_died,
        gc_failures,
        workers_retired,
        // The reason is a log line's text, not a number; the count is `workers_retired`.
        last_worker_retirement: _,
        // Why the threads run unpinned is words; the counts are `mipyre_cpu_degraded_total`.
        unpinned_because: _,
        cpu_pin_failures,
        cpu_qos_failures,
        cpu_affinity_unreadable,
        cpu_topology_unreadable,
        cpu_threads_beyond_cpus,
        drain_timeouts,
        drain_closed_connections,
        stop_watcher_failures,
        blocking_calls_left,
        validation_failures,
        cpu_sizing_fallback,
    } = run;

    // A run that does not count its answers has none to show; its refusals are still counted.
    if let Some(counts) = answer_counts {
        answers(out, counts);
    }
    family(
        out,
        "mipyre_http_requests_refused_total",
        MetricType::Counter,
        "Requests the server answered by itself instead of the handler: 503 overloaded or shutting down, 504 \
         past the request budget, 502 a process-lane worker died, 500 a worker dropped the request.",
        [("", refused)],
    );
    family(
        out,
        "mipyre_workers_serving",
        MetricType::Gauge,
        "Sub-interpreter workers serving now.",
        [("", workers_serving)],
    );
    family(
        out,
        "mipyre_workers_died_total",
        MetricType::Counter,
        "Workers that stopped serving on their own while the server ran: sub-interpreter workers, and \
         runtime=\"process\" worker processes that died (crashed, killed, timed out) and were replaced.",
        [
            ("{runtime=\"subinterp\"}", workers_died),
            ("{runtime=\"process\"}", process_workers_died),
        ],
    );
    family(
        out,
        "mipyre_gc_failures_total",
        MetricType::Counter,
        "gc.collect() calls that raised or panicked; each retired its worker.",
        [("", gc_failures)],
    );
    family(
        out,
        "mipyre_workers_retired_total",
        MetricType::Counter,
        "Workers retired because their gc.collect() failed: they take no more requests.",
        [("", workers_retired)],
    );
    family(
        out,
        "mipyre_cpu_degraded_total",
        MetricType::Counter,
        "CPU placement degradations: threads not pinned, QoS not raised, topology unreadable, more TPC \
         threads than CPUs.",
        [
            ("{kind=\"pin_failed\"}", cpu_pin_failures),
            ("{kind=\"qos_failed\"}", cpu_qos_failures),
            ("{kind=\"affinity_unreadable\"}", cpu_affinity_unreadable),
            ("{kind=\"topology_unreadable\"}", cpu_topology_unreadable),
            ("{kind=\"threads_beyond_cpus\"}", cpu_threads_beyond_cpus),
        ],
    );
    family(
        out,
        "mipyre_drain_timeouts_total",
        MetricType::Counter,
        "Drains of stopped accept loops that ran out of time.",
        [("", drain_timeouts)],
    );
    family(
        out,
        "mipyre_drain_closed_connections_total",
        MetricType::Counter,
        "Connections still open when such a drain gave up; they were closed.",
        [("", drain_closed_connections)],
    );
    family(
        out,
        "mipyre_stop_watcher_failures_total",
        MetricType::Counter,
        "Stop watchers (the thread that stops a server run on SIGINT or SIGTERM) that panicked or were still \
         running when their run ended.",
        [("", stop_watcher_failures)],
    );
    family(
        out,
        "mipyre_blocking_calls_left_total",
        MetricType::Counter,
        "Blocking calls (main-interpreter handlers) still running when their TPC thread's runtime was shut \
         down, and left running.",
        [("", blocking_calls_left)],
    );
    family(
        out,
        "mipyre_validation_failures_total",
        MetricType::Counter,
        "Requests answered 422 because their body= model or query= string failed validation.",
        [("", validation_failures)],
    );
    if let Some(reason) = cpu_sizing_fallback {
        let name = "mipyre_cpu_sizing_fallback_info";
        header(
            out,
            name,
            MetricType::Gauge,
            "1 when the server is sized from a fallback CPU count; the reason is the label.",
        );
        sample(out, name, &format!("{{reason=\"{}\"}}", label_value(reason)), 1);
    }
}

fn process_counts(out: &mut String, process: &Counters) {
    let Counters {
        // The GIL monitor's readings and the RSS are not on the page; they are read from
        // `process_metrics()`.
        gil_wait_last_us: _,
        gil_wait_peak_us: _,
        gil_wait_count: _,
        gil_wait_total_us: _,
        gil_queue_length: _,
        gil_hold_peak_us: _,
        rss_bytes: _,
        precompressed_fallbacks,
        static_unwatched_mounts,
        static_answers_cut,
        static_bytes_in_flight,
        db_results_undeliverable,
        gil_congested_waits,
        client_disconnects,
        accept_failures,
        accept_exhausted,
        tls_handshake_failures,
        malformed_requests,
        oversized_request_heads,
        first_request_timeouts,
        request_head_timeouts,
        forwarded_for_unreadable,
        quickack_failures,
        defer_accept_failures,
        nodelay_failures,
        // Each line is a component's reason in words; the counts above say how often.
        degraded: _,
    } = process;

    family(
        out,
        "mipyre_precompressed_fallbacks_total",
        MetricType::Counter,
        "Pre-compressed static files that could not be read; the original was served.",
        [("", precompressed_fallbacks)],
    );
    family(
        out,
        "mipyre_static_unwatched_mounts_total",
        MetricType::Counter,
        "Static mounts served uncached because their file watch failed or was lost.",
        [("", static_unwatched_mounts)],
    );
    family(
        out,
        "mipyre_static_answers_cut_total",
        MetricType::Counter,
        "Static answers sent from disk that were cut off because the file changed or failed while it was sent.",
        [("", static_answers_cut)],
    );
    family(
        out,
        "mipyre_static_bytes_in_flight",
        MetricType::Gauge,
        "Bytes static answers sent from disk hold now, read and not yet written out.",
        [("", static_bytes_in_flight)],
    );
    family(
        out,
        "mipyre_db_results_undeliverable_total",
        MetricType::Counter,
        "Async database results that could not reach a closed asyncio loop.",
        [("", db_results_undeliverable)],
    );
    family(
        out,
        "mipyre_gil_congested_waits_total",
        MetricType::Counter,
        "GIL acquisition waits past the congestion threshold (50 ms).",
        [("", gil_congested_waits)],
    );
    family(
        out,
        "mipyre_client_disconnects_total",
        MetricType::Counter,
        "Peer-gone events (reset, aborted, closed mid-IO): the client's doing.",
        [("", client_disconnects)],
    );
    family(
        out,
        "mipyre_accept_failures_total",
        MetricType::Counter,
        "accept() failures other than resource exhaustion or a peer going away.",
        [("", accept_failures)],
    );
    family(
        out,
        "mipyre_accept_exhausted_total",
        MetricType::Counter,
        "accept() failures for lack of file descriptors, sockets or kernel buffers.",
        [("", accept_exhausted)],
    );
    family(
        out,
        "mipyre_tls_handshake_failures_total",
        MetricType::Counter,
        "TLS handshakes that failed.",
        [("", tls_handshake_failures)],
    );
    family(
        out,
        "mipyre_bad_requests_total",
        MetricType::Counter,
        "Connections closed because the client sent no HTTP/1 request (400) or a head past the limits (414 / \
         431).",
        [
            ("{kind=\"malformed\"}", malformed_requests),
            ("{kind=\"oversized\"}", oversized_request_heads),
        ],
    );
    family(
        out,
        "mipyre_request_head_timeouts_total",
        MetricType::Counter,
        "Connections closed because the client sent no request head within the request-head timeout: no \
         first request after connecting, or no next one on a kept-alive connection.",
        [
            ("{kind=\"first_request\"}", first_request_timeouts),
            ("{kind=\"next_request\"}", request_head_timeouts),
        ],
    );
    family(
        out,
        "mipyre_forwarded_for_unreadable_total",
        MetricType::Counter,
        "X-Forwarded-For headers from a trusted proxy that were ignored because the entry where the client \
         was looked for is no IP address; the proxy was taken for the client.",
        [("", forwarded_for_unreadable)],
    );
    family(
        out,
        "mipyre_socket_option_failures_total",
        MetricType::Counter,
        "Socket options that could not be set (the option is off on that socket).",
        [
            ("{option=\"quickack\"}", quickack_failures),
            ("{option=\"defer_accept\"}", defer_accept_failures),
            ("{option=\"nodelay\"}", nodelay_failures),
        ],
    );
}

/// Returns the page for one run's counts and the process's.
pub(crate) fn render(run: &RunCounts, process: &Counters) -> String {
    let mut out = String::new();
    run_counts(&mut out, run);
    process_counts(&mut out, process);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn process() -> Counters {
        Counters {
            gil_wait_last_us: 0,
            gil_wait_peak_us: 0,
            gil_wait_count: 0,
            gil_wait_total_us: 0,
            gil_queue_length: 0,
            gil_hold_peak_us: 0,
            rss_bytes: None,
            precompressed_fallbacks: 0,
            static_unwatched_mounts: 0,
            static_answers_cut: 0,
            static_bytes_in_flight: 0,
            db_results_undeliverable: 0,
            gil_congested_waits: 0,
            client_disconnects: 0,
            accept_failures: 0,
            accept_exhausted: 0,
            tls_handshake_failures: 0,
            malformed_requests: 3,
            oversized_request_heads: 0,
            first_request_timeouts: 0,
            request_head_timeouts: 0,
            forwarded_for_unreadable: 0,
            quickack_failures: 0,
            defer_accept_failures: 0,
            nodelay_failures: 0,
            degraded: Vec::new(),
        }
    }

    fn run_with_answers() -> RunCounts {
        let mut counts = AnswerCounts::default();
        let get = MethodKind::Get.index();
        counts.answers[get][1] = 2;
        counts.answers[MethodKind::Other.index()][4] = 1;
        counts.duration_buckets[0] = 1;
        counts.duration_buckets[6] = 1;
        counts.duration_buckets[16] = 1;
        counts.duration_sum_us = 100 + 7_000 + 12_000_001;
        RunCounts {
            answers: Some(counts),
            refused: 1,
            ..RunCounts::default()
        }
    }

    fn lines_of<'a>(page: &'a str, name: &str) -> Vec<&'a str> {
        page.lines().filter(|l| l.starts_with(name)).collect()
    }

    #[test]
    fn only_the_answers_given_are_on_the_page() {
        let page = render(&run_with_answers(), &process());
        assert_eq!(
            lines_of(&page, "mipyre_http_requests_total"),
            [
                "mipyre_http_requests_total{method=\"GET\",status_class=\"2xx\"} 2",
                "mipyre_http_requests_total{method=\"other\",status_class=\"5xx\"} 1",
            ]
        );
        assert!(page.contains("\nmipyre_http_requests_refused_total 1\n"));
    }

    #[test]
    fn the_buckets_add_up_to_the_count_and_never_fall() {
        let page = render(&run_with_answers(), &process());
        let buckets = lines_of(&page, "mipyre_http_request_duration_seconds_bucket");
        assert_eq!(buckets.len(), DURATION_BOUNDS_US.len() + 1);
        assert_eq!(
            buckets[0],
            "mipyre_http_request_duration_seconds_bucket{le=\"0.0001\"} 1"
        );
        assert_eq!(
            buckets[5],
            "mipyre_http_request_duration_seconds_bucket{le=\"0.005\"} 1"
        );
        assert_eq!(buckets[6], "mipyre_http_request_duration_seconds_bucket{le=\"0.01\"} 2");
        assert_eq!(buckets[15], "mipyre_http_request_duration_seconds_bucket{le=\"10\"} 2");
        assert_eq!(
            buckets[16],
            "mipyre_http_request_duration_seconds_bucket{le=\"+Inf\"} 3"
        );
        assert!(page.contains("\nmipyre_http_request_duration_seconds_count 3\n"));
        assert!(page.contains("\nmipyre_http_request_duration_seconds_sum 12.007101\n"));
    }

    #[test]
    fn every_family_has_one_help_and_one_type_line() {
        let page = render(&run_with_answers(), &process());
        let names: Vec<&str> = page
            .lines()
            .filter_map(|l| l.strip_prefix("# TYPE "))
            .map(|l| l.split(' ').next().unwrap())
            .collect();
        for name in &names {
            assert_eq!(page.matches(&format!("# HELP {name} ")).count(), 1, "{name}");
            assert_eq!(page.matches(&format!("# TYPE {name} ")).count(), 1, "{name}");
        }
        assert!(names.contains(&"mipyre_http_request_duration_seconds"));
        assert!(names.contains(&"mipyre_socket_option_failures_total"));
        assert!(page.contains("# TYPE mipyre_http_request_duration_seconds histogram\n"));
    }

    #[test]
    fn a_label_value_is_escaped() {
        let run = RunCounts {
            cpu_sizing_fallback: Some("cgroup quota \"max\"\nunread\\".into()),
            ..RunCounts::default()
        };
        let page = render(&run, &process());
        assert!(page.contains("mipyre_cpu_sizing_fallback_info{reason=\"cgroup quota \\\"max\\\"\\nunread\\\\\"} 1\n"));
        assert!(page.contains("mipyre_bad_requests_total{kind=\"malformed\"} 3\n"));
    }

    #[test]
    fn a_run_that_does_not_count_its_answers_shows_none() {
        let page = render(&RunCounts::default(), &process());
        assert!(!page.contains("mipyre_http_requests_total"));
        assert!(!page.contains("mipyre_http_request_duration_seconds"));
        assert!(page.contains("\nmipyre_http_requests_refused_total 0\n"));
    }
}
