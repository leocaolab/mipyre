//! Holds the check that `.cargo/config.toml` reaches the test processes cargo starts in this tree: a failing
//! proptest writes no proptest-regressions/ file beside the source. One crate's test stands for every crate, since
//! cargo hands the same `[env]` to each.

#[test]
fn a_proptest_in_this_tree_persists_no_failure() {
    assert!(
        proptest::test_runner::Config::default().failure_persistence.is_none(),
        "PROPTEST_DISABLE_FAILURE_PERSISTENCE (.cargo/config.toml) did not reach this test process: a failing proptest \
         would write proptest-regressions/ into the source tree"
    );
}
