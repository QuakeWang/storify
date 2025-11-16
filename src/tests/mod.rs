pub mod behavior;

#[test]
fn behavior_suite() {
    if let Err(err) = behavior::run_behavior_suite() {
        panic!("behavior suite failed: {err}");
    }
}
