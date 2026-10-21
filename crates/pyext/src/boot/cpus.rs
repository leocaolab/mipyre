//! Reads the host's CPUs a server is sized from, and says when they are a fallback.

use std::num::NonZeroUsize;

use mipyre_cpu::{CpuReadError, Cpus, HostCpus, PinOrder, PinOrderError, Quota};

/// Holds the CPU counts a server is sized from and where its TPC threads would be pinned.
pub(crate) struct ServerCpus {
    pub(crate) cpus: Cpus,
    pub(crate) pin_order: Result<PinOrder, PinOrderError>,
}

/// Reads the host's CPUs; when the OS gives no count it logs why and sizes from a fallback,
/// because a count only sizes defaults that `workers` and `io_workers` can override.
pub(crate) fn host_cpus() -> ServerCpus {
    let HostCpus { cpus, pin_order } = HostCpus::read(cpu_quota());
    ServerCpus {
        cpus: sizing_cpus(cpus, &pin_order),
        pin_order,
    }
}

/// Reads this process's cgroup CPU quota; one that cannot be read is logged, counted and treated
/// as no quota.
fn cpu_quota() -> Quota {
    match mipyre_cpu::read_cpu_quota() {
        Ok(quota) => quota,
        Err(e) => {
            tracing::warn!(target: "mipyre::server", error = %e,
                error_type = "QuotaReadError",
                "could not read the CPU quota; sizing from the affinity mask alone");
            crate::telemetry::record_cpu_sizing_fallback(|| e.to_string());
            Quota::Unlimited
        }
    }
}

/// Returns the CPUs [`sizing`] picks, logging and counting a fallback. Cores left unread by a
/// failed pin-order read are said with that read's error, which only `pin_order` holds.
fn sizing_cpus(
    read: Result<Cpus, CpuReadError>,
    pin_order: &Result<PinOrder, PinOrderError>,
) -> Cpus {
    let (cpus, fallback) = sizing(read);
    match &fallback {
        None => {}
        Some(e @ CpuReadError::Cores { .. }) => {
            let source = pin_order.as_ref().err();
            tracing::warn!(target: "mipyre::server", error = %e,
                source = source.map(tracing::field::display),
                error_type = "CpuReadError::Cores",
                "sizing the physical-core defaults from the logical CPU count");
            crate::telemetry::record_cpu_sizing_fallback(|| match source {
                Some(source) => format!("{e}: {source}"),
                None => e.to_string(),
            });
        }
        Some(e @ CpuReadError::CoreCount { .. }) => {
            tracing::warn!(target: "mipyre::server", error = %e,
                error_type = "CpuReadError::CoreCount",
                "sizing the physical-core defaults from the logical CPU count");
            crate::telemetry::record_cpu_sizing_fallback(|| e.to_string());
        }
        Some(e @ CpuReadError::Logical(_)) => {
            tracing::warn!(target: "mipyre::server", error = %e,
                error_type = "CpuReadError::Logical", "sizing for one CPU");
            crate::telemetry::record_cpu_sizing_fallback(|| e.to_string());
        }
    }
    cpus
}

/// Returns the CPUs to size from and, when they are a fallback, the read failure they stand in
/// for: the logical count replaces unread cores, and one CPU replaces an unread logical count.
fn sizing(read: Result<Cpus, CpuReadError>) -> (Cpus, Option<CpuReadError>) {
    match read {
        Ok(cpus) => (cpus, None),
        Err(e @ (CpuReadError::Cores { logical } | CpuReadError::CoreCount { logical, .. })) => {
            (Cpus::new(logical, logical), Some(e))
        }
        Err(e @ CpuReadError::Logical(_)) => {
            (Cpus::new(NonZeroUsize::MIN, NonZeroUsize::MIN), Some(e))
        }
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
    fn unread_cores_size_from_the_logical_count_and_say_why() {
        let (cpus, fallback) = sizing(Err(CpuReadError::Cores { logical: n(8) }));
        assert_eq!((cpus.logical(), cpus.physical()), (n(8), n(8)));
        let reason = fallback.expect("a fallback says why").to_string();
        assert!(reason.contains("8 logical CPUs"), "{reason}");
        assert!(reason.contains("physical core"), "{reason}");
    }

    #[test]
    fn an_unread_logical_count_sizes_for_one_cpu_and_says_why() {
        let (cpus, fallback) = sizing(Err(CpuReadError::Logical(std::io::Error::other(
            "no /proc here",
        ))));
        assert_eq!((cpus.logical(), cpus.physical()), (n(1), n(1)));
        let reason = fallback.expect("a fallback says why").to_string();
        assert!(reason.contains("no /proc here"), "{reason}");
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
        let log =
            crate::exchange::captured_log(|| sized = Some(sizing_cpus(Err(cores), &unread)));
        let line = log.lines().find(|l| l.contains("no siblings file")).expect(&log);
        assert!(line.contains("\"error_type\":\"CpuReadError::Cores\""), "{line}");
        let sized = sized.unwrap();
        assert_eq!((sized.logical(), sized.physical()), (logical, logical));

        let counted = CpuReadError::CoreCount {
            logical,
            source: std::io::Error::other("no sysctl answer"),
        };
        let mut sized = None;
        let log = crate::exchange::captured_log(|| {
            sized = Some(sizing_cpus(Err(counted), &Ok(PinOrder::Unsupported)))
        });
        let line = log.lines().find(|l| l.contains("no sysctl answer")).expect(&log);
        assert!(line.contains("\"error_type\":\"CpuReadError::CoreCount\""), "{line}");
        let sized = sized.unwrap();
        assert_eq!((sized.logical(), sized.physical()), (logical, logical));

        let unknown = CpuReadError::Logical(std::io::Error::other("no parallelism"));
        let mut sized = None;
        let log = crate::exchange::captured_log(|| sized = Some(sizing_cpus(Err(unknown), &Ok(PinOrder::Unsupported))));
        let line = log.lines().find(|l| l.contains("no parallelism")).expect(&log);
        assert!(line.contains("\"error_type\":\"CpuReadError::Logical\""), "{line}");
        let sized = sized.unwrap();
        assert_eq!((sized.logical(), sized.physical()), (NonZeroUsize::MIN, NonZeroUsize::MIN));
    }
}
