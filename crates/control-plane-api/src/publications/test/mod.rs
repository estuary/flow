// Integration tests to move into this module:
// TODO: move locking_retries.rs
// TODO: move quotas.rs
// TODO: unknown_connectors.rs
use super::*;

#[test]
fn test_errors_result_in_test_failed_status() {
    let build = UncommittedBuild {
        publication_id: models::Id::zero(),
        build_id: models::Id::zero(),
        user_id: Uuid::new_v4(),
        detail: None,
        started_at: tokens::now(),
        output: Default::default(),
        test_errors: std::iter::once(tables::Error {
            scope: tables::synthetic_scope("test", "test/of/a/test"),
            error: anyhow::anyhow!("test error"),
        })
        .collect(),
        retry_count: 0,
    };
    let result = build.build_failed();
    assert_eq!(StatusType::TestFailed, result.status.r#type);
}
