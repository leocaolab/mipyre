//! Runs the app's in-memory and loopback benchmarks in `bench` builds.

use std::num::NonZeroUsize;
use std::sync::Arc;

use pyo3::prelude::*;

use super::App;
use crate::boot::{thread_sites, Deployment, EntryPoint, ServerCpus, ServerFlags};
use crate::serve::Site;
use mipyre_cpu::CoreId;

/// Returns how many workers to run, one per physical core by default, and the CPUs to pin
/// them to; a pinning failure is counted in `counters`.
fn bench_workers(
    workers: Option<usize>,
    server_cpus: ServerCpus,
    counters: &crate::telemetry::RunCounters,
) -> PyResult<(NonZeroUsize, Vec<CoreId>)> {
    let ServerCpus { cpus, pin_order, .. } = server_cpus;
    let n = match workers {
        None => cpus.physical(),
        Some(n) => NonZeroUsize::new(n)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("bench workers must be at least 1"))?,
    };
    crate::serve::report_pinning(&pin_order, n.get(), counters);
    Ok((n, crate::serve::thread_cpus(&pin_order)))
}

impl App {
    pub(super) fn run_bench_inmem(
        &self,
        py: Python<'_>,
        duration_s: u64,
        workers: Option<usize>,
        conns_per_worker: usize,
    ) -> PyResult<(u64, f64)> {
        let env = crate::boot::ServerRecipe::of(
            Deployment::from_process(&[], EntryPoint::Embedded)?.over(&ServerFlags::default()),
        )?;
        let server_cpus = crate::boot::host_cpus();
        let counters = self.start_counters(env.tuning.metrics, &server_cpus);
        let (n, core_ids) = bench_workers(workers, server_cpus, &counters)?;
        let first = Arc::new(self.bench_site(py, Arc::clone(&counters))?);
        let program = self.source.program(
            py,
            crate::runtime::CommandLine::of_main(py)?,
            env.isolate_dir(crate::boot::user_cache_dir().as_deref()).as_deref(),
        )?;
        let recipe = Arc::new(crate::boot::worker_recipe(&program, &first, env.tuning.gc, &self.store));
        // Each worker gets its own copy of the site so no two cores share its refcount cache
        // line, while all copies share the run's counters.
        let sites: Vec<Arc<Site>> = (1..n.get()).map(|_| Arc::new(first.for_thread(py))).collect();
        let sites: Vec<Arc<Site>> = std::iter::once(first).chain(sites).collect();

        // The sites hold main-interpreter `Py<T>`s, so their last references must be dropped
        // here, attached, after the bench threads are joined.
        let sites_keepalive = sites.clone();
        let paired = crate::bench::BenchWorkers {
            recipe,
            sites,
            counters,
        };
        let duration = std::time::Duration::from_secs(duration_s);
        let measured = py
            .detach(move || {
                crate::bench::run_inmem_bench(
                    conns_per_worker,
                    duration,
                    paired,
                    &core_ids,
                    crate::bench::BENCH_REQUEST.into(),
                )
            })
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        drop(sites_keepalive);
        Ok((measured.requests, measured.elapsed.as_secs_f64()))
    }

    pub(super) fn run_bench_inmem_profile<'py>(
        &self,
        py: Python<'py>,
        request: Vec<u8>,
        duration_s: u64,
        conns: usize,
        stacks: bool,
    ) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let env = crate::boot::ServerRecipe::of(
            Deployment::from_process(&[], EntryPoint::Embedded)?.over(&ServerFlags::default()),
        )?;
        let server_cpus = crate::boot::host_cpus();
        let counters = self.start_counters(env.tuning.metrics, &server_cpus);
        let (_, core_ids) = bench_workers(Some(1), server_cpus, &counters)?;
        let site: Arc<Site> = Arc::new(self.bench_site(py, counters)?);
        let program = self.source.program(
            py,
            crate::runtime::CommandLine::of_main(py)?,
            env.isolate_dir(crate::boot::user_cache_dir().as_deref()).as_deref(),
        )?;
        let recipe = Arc::new(crate::boot::worker_recipe(&program, &site, env.tuning.gc, &self.store));
        let site_keepalive = Arc::clone(&site);
        let paired = crate::bench::BenchWorkers {
            recipe,
            sites: vec![Arc::clone(&site)],
            counters: Arc::clone(&site.counters),
        };
        drop(site);
        let duration = std::time::Duration::from_secs(duration_s);
        let depth = if stacks {
            crate::bench::ProfileDepth::WithStacks
        } else {
            crate::bench::ProfileDepth::Counts
        };
        let (measured, profiled) = py
            .detach(move || crate::bench::run_inmem_profile(conns, duration, paired, &core_ids, request.into(), depth))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        drop(site_keepalive);
        let out = pyo3::types::PyDict::new(py);
        let (requests, instructions) = match profiled.instructions {
            Some(window) => (window.requests, Some(window.counted)),
            None => (measured.requests, None),
        };
        out.set_item("requests", requests)?;
        out.set_item("instructions", instructions)?;
        out.set_item("alloc_requests", profiled.allocs.requests)?;
        out.set_item("allocs", profiled.allocs.counted.allocs)?;
        out.set_item("alloc_bytes", profiled.allocs.counted.bytes)?;
        let (stack_requests, stacks) = match profiled.stacks {
            Some(window) => (window.requests, window.counted.stacks),
            None => (0, Vec::new()),
        };
        let stacks: Vec<(u64, u64, Vec<String>)> = stacks
            .into_iter()
            .map(|site| (site.count, site.bytes, site.frames))
            .collect();
        out.set_item("stack_requests", stack_requests)?;
        out.set_item("stacks", stacks)?;
        Ok(out)
    }

    pub(super) fn run_bench_loopback(
        &self,
        py: Python<'_>,
        duration_s: u64,
        workers: Option<usize>,
        client_conns: usize,
    ) -> PyResult<(u64, f64, u16)> {
        let env = crate::boot::ServerRecipe::of(
            Deployment::from_process(&[], EntryPoint::Embedded)?.over(&ServerFlags::default()),
        )?;
        let server_cpus = crate::boot::host_cpus();
        let counters = self.start_counters(env.tuning.metrics, &server_cpus);
        let (n, core_ids) = bench_workers(workers, server_cpus, &counters)?;
        let site: Arc<Site> = Arc::new(self.bench_site(py, counters)?);
        let program = self.source.program(
            py,
            crate::runtime::CommandLine::of_main(py)?,
            env.isolate_dir(crate::boot::user_cache_dir().as_deref()).as_deref(),
        )?;
        let recipe = Arc::new(crate::boot::worker_recipe(&program, &site, env.tuning.gc, &self.store));

        // Each worker thread gets its own copy as in production TPC, and their last
        // references are dropped here, attached, as in `bench_inmem`.
        let sites = thread_sites(py, &site, n.get());
        let site_keepalive = (Arc::clone(&site), sites.clone());
        let workers = crate::bench::BenchWorkers {
            recipe,
            sites,
            counters: Arc::clone(&site.counters),
        };
        let duration = std::time::Duration::from_secs(duration_s);
        let (measured, port) = py
            .detach(move || crate::bench::run_loopback_bench(client_conns, duration, n, workers, &core_ids))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        drop(site_keepalive);
        Ok((measured.requests, measured.elapsed.as_secs_f64(), port))
    }

    /// Returns the sealed site a bench serves; every route must be sync, off the GIL and not
    /// streaming, because the benches run only the TPC inline path.
    fn bench_site(&self, py: Python<'_>, counters: Arc<crate::telemetry::RunCounters>) -> PyResult<Site> {
        self.seal_if_unsealed();
        let site = self.snapshot(py, counters)?;
        if !mipyre_http::plan::route::PlaceCounts::of(&site.rules.routes).all_inline_sync() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "the benches support only sync, non-GIL, non-stream routes",
            ));
        }
        Ok(site)
    }
}
