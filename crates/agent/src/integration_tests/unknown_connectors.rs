use models::Id;
use uuid::Uuid;

use crate::integration_tests::harness::{draft_catalog, TestHarness};

#[tokio::test]
#[serial_test::serial]
async fn test_forbidden_connector() {
    let mut harness = TestHarness::init("test_forbidden_connector").await;
    let user_id = harness.setup_tenant("sheep").await;

    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "sheep/wool": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "color": { "type": "string" },
                    },
                    "required": ["id"]
                },
                "key": ["/id"]
            }
        },
        "captures": {
            "sheep/capture": {
                "endpoint": {
                    "connector": {
                        "image": "forbidden_connector:v99",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "target": "sheep/wool",
                        "resource": {
                            "pasture": "green hill"
                        }
                    }
                ]
            }
        }
    }));
    let pub_id = Id::new([0, 0, 0, 0, 0, 0, 0, 9]);
    let built = harness
        .publisher
        .build(
            user_id,
            pub_id,
            None,
            draft,
            Uuid::new_v4(),
            "ops/dp/public/test",
            true,
            0,
        )
        .await
        .expect("build failed");
    assert!(built.has_errors());

    let errors = built.errors().collect::<Vec<_>>();

    insta::assert_debug_snapshot!(errors, @r###"
    [
        Error {
            scope: flow://capture/sheep/capture,
            error: Forbidden connector image 'forbidden_connector',
        },
    ]
    "###);
}
