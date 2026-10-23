"""The one rule mipyre limits library thread pools by, in process-lane workers and for
isolated sub-interpreters alike."""

from mipyre._thread_pools import BLAS, POLARS, describe, one_thread_each


def test_a_pool_nobody_sized_gets_one_thread_in_every_variable():
    assert one_thread_each({}) == {v: "1" for v in BLAS.variables + POLARS.variables}


def test_a_pool_the_user_sized_through_any_one_variable_is_left_whole():
    for variable in BLAS.variables:
        limits = one_thread_each({variable: "4"})
        assert limits == {"POLARS_MAX_THREADS": "1"}, variable


def test_the_users_omp_setting_is_not_overruled_by_openblas_or_mkl():
    limits = one_thread_each({"OMP_NUM_THREADS": "8"})
    assert "OPENBLAS_NUM_THREADS" not in limits and "MKL_NUM_THREADS" not in limits


def test_the_startup_log_says_per_pool_what_was_done():
    environ = {"OMP_NUM_THREADS": "8", "MKL_NUM_THREADS": "2"}
    said = describe(environ, one_thread_each(environ))
    assert said == (
        "BLAS (numpy, scipy): left as set by OMP_NUM_THREADS, MKL_NUM_THREADS; polars: 1 thread per worker"
    )
