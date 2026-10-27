//! Reads thread stack sizes for the tests that check a thread running Python was given
//! `PYTHON_THREAD_STACK`.
//!
//! The OS rounds what it reports its own way, so a site's thread is compared with a reference thread
//! built with that stack in the same process, not with the number. On Linux every such test runs
//! through `with_exact_stacks`, because glibc hands a joined thread's cached stack to a new thread that
//! asks for at least a quarter of its size, and the new thread then reports the cached block's size.
#![expect(
    clippy::expect_used,
    reason = "test helpers: a failed expectation is how a test using them fails"
)]

/// Returns the calling thread's stack size as the OS reports it.
#[cfg(target_os = "macos")]
pub fn current() -> usize {
    // SAFETY: pthread_self() is the calling thread, alive for the call.
    unsafe { libc::pthread_get_stacksize_np(libc::pthread_self()) }
}

/// Returns the calling thread's stack size as the OS reports it.
#[cfg(target_os = "linux")]
pub fn current() -> usize {
    let mut attr = std::mem::MaybeUninit::<libc::pthread_attr_t>::uninit();
    let mut size = 0usize;
    // SAFETY: pthread_getattr_np initialises `attr` for the calling thread (checked by its return); the
    // attribute is read once and destroyed once.
    unsafe {
        assert_eq!(libc::pthread_getattr_np(libc::pthread_self(), attr.as_mut_ptr()), 0);
        assert_eq!(libc::pthread_attr_getstacksize(attr.as_ptr(), &mut size), 0);
        assert_eq!(libc::pthread_attr_destroy(attr.as_mut_ptr()), 0);
    }
    size
}

/// Returns what a thread built with `std::thread::Builder::stack_size(size)` reports from `current`.
pub fn of_a_thread_with(size: usize) -> usize {
    std::thread::Builder::new()
        .name("stack-reference".into())
        .stack_size(size)
        .spawn(current)
        .expect("the reference thread starts")
        .join()
        .expect("the reference thread reads its stack")
}

/// Runs `body` in a run of this test binary for the test at path `test` alone, with glibc's stack
/// cache off so every thread reports the stack it was built with; a failure there fails this test with
/// that run's own output.
#[cfg(target_os = "linux")]
pub fn with_exact_stacks(test: &str, body: impl FnOnce()) {
    const NO_STACK_CACHE: &str = "glibc.pthread.stack_cache_size=0";
    // An unset GLIBC_TUNABLES means no tunable is set.
    let tunables = std::env::var_os("GLIBC_TUNABLES").unwrap_or_default();
    if tunables.to_string_lossy().split(':').any(|t| t == NO_STACK_CACHE) {
        return body();
    }
    let mut without_cache = tunables;
    if !without_cache.is_empty() {
        without_cache.push(":");
    }
    without_cache.push(NO_STACK_CACHE);
    // libtest names a test by its path inside the crate.
    let name = test.split_once("::").map_or(test, |(_, path)| path);
    let binary = std::env::current_exe().expect("the test binary's path");
    let run = std::process::Command::new(&binary)
        .args([name, "--exact", "--test-threads=1", "--nocapture"])
        .env("GLIBC_TUNABLES", &without_cache)
        .output()
        .expect("the test binary runs again");
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    // Requiring "1 passed" keeps a misspelt name, which runs no test, from passing.
    assert!(
        run.status.success() && stdout.contains("test result: ok. 1 passed"),
        "{name} with glibc's stack cache off ({}): {}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        binary.display(),
        run.status,
    );
}

/// Runs `body` directly, because this OS does not hand a joined thread's stack to a new thread.
#[cfg(not(target_os = "linux"))]
pub fn with_exact_stacks(_test: &str, body: impl FnOnce()) {
    body()
}

/// Shows the comparison tells stacks apart: a thread built with tokio's default 2 MiB stack, started
/// after the reference as a site's thread is, reads less than it.
#[test]
fn a_thread_built_with_tokios_default_reads_less_than_a_python_thread() {
    with_exact_stacks(
        concat!(
            module_path!(),
            "::a_thread_built_with_tokios_default_reads_less_than_a_python_thread"
        ),
        || {
            let python = of_a_thread_with(mipyre_subinterp::PYTHON_THREAD_STACK);
            assert!(of_a_thread_with(2 << 20) < python, "{python}");
        },
    );
}
