use automations::Executor as _;
use data_plane_controller::job::executor::{DevExecutor, Executor};

/// `DevExecutor` exists only to register the same controller logic under a
/// second task type, so the two types differing is the entire feature. If a
/// refactor left `DevExecutor::TASK_TYPE` pointing at the primary type, both
/// deployments would serve every data-plane and nothing would fail loudly:
/// they would simply contend for the same tasks through the heartbeat lease.
#[test]
fn dev_executor_serves_a_distinct_task_type() {
    assert_eq!(
        Executor::TASK_TYPE,
        automations::task_types::DATA_PLANE_CONTROLLER,
    );
    assert_eq!(
        DevExecutor::TASK_TYPE,
        automations::task_types::DATA_PLANE_CONTROLLER_DEV,
    );
    assert_ne!(Executor::TASK_TYPE, DevExecutor::TASK_TYPE);
}
