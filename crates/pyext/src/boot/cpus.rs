//! Reads the host's CPUs a server is sized from, and says when they are a fallback.

use std::num::NonZeroUsize;

use mipyre_cpu::{CpuReadError, Cpus, HostCpus, PinOrder, PinOrderError, Quota, QuotaReadError};

/// Holds the CPU counts a server is sized from, where its TPC threads would be pinned, and why
/// the counts are a fallback when they are.
pub(crate) struct ServerCpus {
    pub(crate) cpus: Cpus,
    pub(crate) pin_order: Result<PinOrder, PinOrderError>,
    /// It is `None` when the OS gave the counts; written by [`host_cpus`] as it reads them.
    pub(crate) fallback: Option<CpuCountFallback>,
}

/// Why the CPU counts a server is sized from are a fallback: which of the OS reads failed.
#[derive(Debug)]
pub(crate) enum CpuCountFallback {
    /// The CPU counts could not be read; the logical count, or one CPU, stands in.
    Cpus(CpuReadError),
    /// The cgroup CPU quota could not be read; the affinity mask alone sizes the server.
    Quota(QuotaReadError),
    Both(CpuReadError, QuotaReadError),
}

impl ServerCpus {
    /// Returns why the counts are a fallback, in words, or `None` when the OS gave them. Cores
    /// left unread by a failed pin-order read are said with that read's error, which only
    /// `pin_order` holds.
    pub(crate) fn fallback_reason(&self) -> Option<String> {
        let cpus = |e: &CpuReadError| match (e, self.pin_order.as_ref().err()) {
            (CpuReadError::Cores { .. }, Some(source)) => format!("{e}: {source}"),
            _ => e.to_string(),
        };
        self.fallback.as_ref().map(|fallback| match fallback {
            CpuCountFallback::Cpus(e) => cpus(e),
            CpuCountFallback::Quota(e) => e.to_string(),
            CpuCountFallback::Both(e, quota) => format!("{}; {quota}", cpus(e)),
        })
    }
}

/// Reads the host's CPUs; when the OS gives no count it logs why and sizes from a fallback,
/// because a count only sizes defaults that `threads` and `subinterpreters` can override.
pub(crate) fn host_cpus() -> ServerCpus {
    let (quota, quota_error) = cpu_quota();
    let HostCpus { cpus, pin_order } = HostCpus::read(quota);
    let (cpus, cpus_error) = sizing_cpus(cpus, &pin_order);
    let fallback = match (cpus_error, quota_error) {
        (None, None) => None,
        (Some(e), None) => Some(CpuCountFallback::Cpus(e)),
        (None, Some(e)) => Some(CpuCountFallback::Quota(e)),
        (Some(e), Some(quota)) => Some(CpuCountFallback::Both(e, quota)),
    };
    ServerCpus {
        cpus,
        pin_order,
        fallback,
    }
}

/// Reads this process's cgroup CPU quota; one that cannot be read is logged and treated as no
/// quota, and its error is handed back for the fallback reason.
fn cpu_quota() -> (Quota, Option<QuotaReadError>) {
    match mipyre_cpu::read_cpu_quota() {
        Ok(quota) => (quota, None),
        Err(e) => {
            tracing::warn!(target: "mipyre::server", error = %e,
                error_type = "QuotaReadError",
                "could not read the CPU quota; sizing from the affinity mask alone");
            (Quota::Unlimited, Some(e))
        }
    }
}

/// Returns the CPUs [`sizing`] picks and, when they are a fallback, the read failure, logged
/// here. Cores left unread by a failed pin-order read are logged with that read's error.
fn sizing_cpus(
    read: Result<Cpus, CpuReadError>,
    pin_order: &Result<PinOrder, PinOrderError>,
) -> (Cpus, Option<CpuReadError>) {
    let (cpus, fallback) = sizing(read);
    match &fallback {
        None => {}
        Some(e @ CpuReadError::Cores { .. }) => {
            let source = pin_order.as_ref().err();
            tracing::warn!(target: "mipyre::server", error = %e,
                source = source.map(tracing::field::display),
                error_type = "CpuReadError::Cores",
                "sizing the physical-core defaults from the logical CPU count");
        }
        Some(e @ CpuReadError::CoreCount { .. }) => {
            tracing::warn!(target: "mipyre::server", error = %e,
                error_type = "CpuReadError::CoreCount",
                "sizing the physical-core defaults from the logical CPU count");
        }
        Some(e @ CpuReadError::CoresNotCountedHere { .. }) => {
            tracing::info!(target: "mipyre::server", error = %e,
                error_type = "CpuReadError::CoresNotCountedHere",
                "sizing the physical-core defaults from the logical CPU count");
        }
        Some(e @ CpuReadError::Logical(_)) => {
            tracing::warn!(target: "mipyre::server", error = %e,
                error_type = "CpuReadError::Logical", "sizing for one CPU");
        }
    }
    (cpus, fallback)
}

/// Returns the CPUs to size from and, when they are a fallback, the read failure they stand in
/// for: the logical count replaces unread cores, and one CPU replaces an unread logical count.
fn sizing(read: Result<Cpus, CpuReadError>) -> (Cpus, Option<CpuReadError>) {
    match read {
        Ok(cpus) => (cpus, None),
        Err(
            e @ (CpuReadError::Cores { logical }
            | CpuReadError::CoreCount { logical, .. }
            | CpuReadError::CoresNotCountedHere { logical }),
        ) => (Cpus::new(logical, logical), Some(e)),
        Err(e @ CpuReadError::Logical(_)) => (Cpus::new(NonZeroUsize::MIN, NonZeroUsize::MIN), Some(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    #[test]
    fn counts_that_were_read_are_sized_from_as_they_are() {
        let read = Cpus::new(n(16), n(8));
        let (cpus, fallback) = sizing(Ok(read));
        assert_eq!(cpus, read);
        assert!(fallback.is_none());
    }

    #[test]
    fn cores_this_platform_does_not_count_size_from_the_logical_count_and_say_why() {
        let (cpus, fallback) = sizing(Err(CpuReadError::CoresNotCountedHere { logical: n(8) }));
        assert_eq!((cpus.logical(), cpus.physical()), (n(8), n(8)));
        let reason = fallback.expect("a fallback says why").to_string();
        assert!(reason.contains("does not count physical cores"), "{reason}");
    }

    #[test]
    fn unread_cores_size_from_the_logical_count_and_say_why() {
        let (cpus, fallback) = sizing(Err(CpuReadError::Cores { logical: n(8) }));
        assert_eq!((cpus.logical(), cpus.physical()), (n(8), n(8)));
        let reason = fallback.expect("a fallback says why").to_string();
        assert!(reason.contains("8 logical CPUs"), "{reason}");
        assert!(reason.contains("physical core"), "{reason}");
    }

    #[test]
    fn an_unread_logical_count_sizes_for_one_cpu_and_says_why() {
        let (cpus, fallback) = sizing(Err(CpuReadError::Logical(std::io::Error::other("no /proc here"))));
        assert_eq!((cpus.logical(), cpus.physical()), (n(1), n(1)));
        let reason = fallback.expect("a fallback says why").to_string();
        assert!(reason.contains("no /proc here"), "{reason}");
    }

    fn unread_topology() -> Result<PinOrder, PinOrderError> {
        Err(mipyre_cpu::PinOrderError::Topology(mipyre_cpu::CpuListFileError::Os(
            mipyre_cpu::FileError {
                path: "no siblings file".into(),
                source: std::io::Error::from_raw_os_error(libc::ENOENT),
            },
        )))
    }

    #[test]
    fn counts_the_os_gave_carry_no_fallback_reason() {
        let read = Cpus::new(n(16), n(8));
        let cpus = ServerCpus {
            cpus: read,
            pin_order: Ok(PinOrder::Unsupported),
            fallback: None,
        };
        assert_eq!(cpus.fallback_reason(), None);
    }

    #[test]
    fn a_fallback_says_which_read_failed_with_the_pin_order_read_behind_unread_cores() {
        let (sized, error) = sizing(Err(CpuReadError::Cores { logical: n(8) }));
        let cpus = ServerCpus {
            cpus: sized,
            pin_order: unread_topology(),
            fallback: error.map(CpuCountFallback::Cpus),
        };
        let reason = cpus.fallback_reason().expect("unread cores are a fallback");
        assert!(
            reason.contains("8 logical CPUs") && reason.contains("no siblings file"),
            "{reason}"
        );

        let both = ServerCpus {
            cpus: sized,
            pin_order: Ok(PinOrder::Unsupported),
            fallback: Some(CpuCountFallback::Both(
                CpuReadError::Logical(std::io::Error::other("no parallelism")),
                mipyre_cpu::QuotaReadError::NotCgroupV2 {
                    text: "1:name=systemd:/".into(),
                },
            )),
        };
        let reason = both.fallback_reason().expect("both reads failed");
        assert!(
            reason.contains("no parallelism") && reason.contains("1:name=systemd:/"),
            "{reason}"
        );
    }

    #[test]
    fn a_cpu_sizing_fallback_warns_with_its_error_type() {
        let logical = NonZeroUsize::new(6).unwrap();
        let cores = CpuReadError::Cores { logical };
        let unread = Err(mipyre_cpu::PinOrderError::Topology(mipyre_cpu::CpuListFileError::Os(
            mipyre_cpu::FileError {
                path: "no siblings file".into(),
                source: std::io::Error::from_raw_os_error(libc::ENOENT),
            },
        )));
        let mut sized = None;
        let log = mipyre_runtime::testkit::captured_log(|| sized = Some(sizing_cpus(Err(cores), &unread).0));
        let line = log.lines().find(|l| l.contains("no siblings file")).expect(&log);
        assert!(line.contains("\"error_type\":\"CpuReadError::Cores\""), "{line}");
        let sized = sized.unwrap();
        assert_eq!((sized.logical(), sized.physical()), (logical, logical));

        let counted = CpuReadError::CoreCount {
            logical,
            source: std::io::Error::other("no sysctl answer"),
        };
        let mut sized = None;
        let log = mipyre_runtime::testkit::captured_log(|| {
            sized = Some(sizing_cpus(Err(counted), &Ok(PinOrder::Unsupported)).0)
        });
        let line = log.lines().find(|l| l.contains("no sysctl answer")).expect(&log);
        assert!(line.contains("\"error_type\":\"CpuReadError::CoreCount\""), "{line}");
        let sized = sized.unwrap();
        assert_eq!((sized.logical(), sized.physical()), (logical, logical));

        let unknown = CpuReadError::Logical(std::io::Error::other("no parallelism"));
        let mut sized = None;
        let log = mipyre_runtime::testkit::captured_log(|| {
            sized = Some(sizing_cpus(Err(unknown), &Ok(PinOrder::Unsupported)).0)
        });
        let line = log.lines().find(|l| l.contains("no parallelism")).expect(&log);
        assert!(line.contains("\"error_type\":\"CpuReadError::Logical\""), "{line}");
        let sized = sized.unwrap();
        assert_eq!(
            (sized.logical(), sized.physical()),
            (NonZeroUsize::MIN, NonZeroUsize::MIN)
        );
    }
}
