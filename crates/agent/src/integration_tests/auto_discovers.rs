use super::spec_fixture;
use crate::{
    integration_tests::harness::{draft_catalog, InjectBuildError, TestHarness},
    publications, ControlPlane,
};
use models::status::{capture::DiscoverChange, AlertType, StatusSummaryType};
use proto_flow::capture::response::{discovered::Binding, Discovered};
use serde_json::json;

#[tokio::test]
#[serial_test::serial]
async fn test_auto_discovers_add_new_bindings() {
    let mut harness = TestHarness::init("test_auto_discovers_new").await;

    let user_id = harness.setup_tenant("marmots").await;

    let init_draft = draft_catalog(json!({
        "captures": {
            "marmots/capture": {
                "autoDiscover": {
                    "addNewBindings": true,
                    "evolveIncompatibleCollections": true,
                },
                "shards": {
                    "logLevel": "debug"
                },
                "interval": "42s",
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": { "squeak": "squeak" }
                    },
                },
                "bindings": [ ]
            },
            "marmots/no-auto-discover": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": { "squeak": "squeak" }
                    },
                },
                "bindings": [ ]
            }
        },
        "materializations": {
            "marmots/materialize": {
                "sourceCapture": "marmots/capture",
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": { "squeak": "squeak squeak" }
                    }
                },
                "bindings": []
            }
        }
    }));

    let result = harness
        .user_publication(user_id, "initial publication", init_draft)
        .await;
    assert!(result.status.is_success());
    assert_eq!(3, result.live_specs.len());

    harness.run_pending_controllers(None).await;

    // Assert that we've initialized auto-discover state appropriately.
    let capture_state = harness.get_controller_state("marmots/capture").await;
    assert!(capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .is_some());
    harness.assert_controller_pending("marmots/capture").await;

    let no_disco_capture_state = harness
        .get_controller_state("marmots/no-auto-discover")
        .await;
    assert!(no_disco_capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .is_none());

    let discovered = Discovered {
        bindings: vec![
            Binding {
                recommended_name: "grass".to_string(),
                resource_config_json: r#"{"id": "grass", "extra": "grass" }"#.to_string(),
                document_schema_json: document_schema(2).to_string(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            Binding {
                recommended_name: "moss".to_string(),
                resource_config_json: r#"{"id": "moss", "extra": "stuff" }"#.to_string(),
                document_schema_json: document_schema(1).to_string(),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
        ],
    };

    harness
        .discover_handler
        .connectors
        .mock_discover("marmots/capture", Ok((spec_fixture(), discovered)));
    harness.set_auto_discover_due("marmots/capture").await;
    harness.run_pending_controller("marmots/capture").await;

    let capture_state = harness.get_controller_state("marmots/capture").await;
    let model = capture_state
        .live_spec
        .as_ref()
        .unwrap()
        .as_capture()
        .unwrap();
    // Expect to see the new bindings added
    insta::assert_json_snapshot!(model.bindings, @r###"
    [
      {
        "resource": {
          "$serde_json::private::RawValue": "{\"_meta\":{\"path\":[\"grass\"]},\"extra\":\"grass\",\"id\":\"grass\"}"
        },
        "target": "marmots/grass"
      },
      {
        "resource": {
          "$serde_json::private::RawValue": "{\"id\": \"moss\", \"extra\": \"stuff\" }"
        },
        "disable": true,
        "target": "marmots/moss"
      }
    ]
    "###);

    let status = capture_state.current_status.unwrap_capture();
    let auto_discover = status
        .auto_discover
        .as_ref()
        .unwrap()
        .last_success
        .as_ref()
        .unwrap();
    insta::assert_json_snapshot!(auto_discover, {
        ".ts" => "[ts]",
    }, @r###"
    {
      "ts": "[ts]",
      "added": [
        {
          "resource_path": [
            "grass"
          ],
          "target": "marmots/grass",
          "disable": false
        },
        {
          "resource_path": [
            "moss"
          ],
          "target": "marmots/moss",
          "disable": true
        }
      ],
      "publish_result": {
        "type": "success"
      }
    }
    "###);
    let last_success_time = auto_discover.ts;

    // Subsequent discover with the same bindings should result in a no-op
    harness.set_auto_discover_due("marmots/capture").await;
    harness.run_pending_controller("marmots/capture").await;

    let capture_state = harness.get_controller_state("marmots/capture").await;
    let status = capture_state.current_status.unwrap_capture();
    let auto_discover = status.auto_discover.as_ref().unwrap();
    assert!(auto_discover.failure.is_none());
    let success = auto_discover.last_success.as_ref().unwrap();
    assert!(success.ts > last_success_time);
    assert!(success.added.is_empty());
    assert!(success.modified.is_empty());
    assert!(success.removed.is_empty());
    let last_success_time = success.ts;

    let discovered = Discovered {
        bindings: vec![
            Binding {
                recommended_name: "grass".to_string(),
                resource_config_json:
                    r#"{"id": "grass", "expect": "ignore in favor of existing" }"#.to_string(),
                document_schema_json: document_schema(2).to_string(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            Binding {
                recommended_name: "flowers".to_string(),
                resource_config_json: r#"{"id": "flowers", "extra": "flowers" }"#.to_string(),
                document_schema_json: document_schema(1).to_string(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
        ],
    };
    harness
        .discover_handler
        .connectors
        .mock_discover("marmots/capture", Ok((spec_fixture(), discovered)));

    harness.set_auto_discover_due("marmots/capture").await;
    harness.run_pending_controller("marmots/capture").await;

    let capture_state = harness.get_controller_state("marmots/capture").await;
    let status = capture_state.current_status.unwrap_capture();
    let auto_discover = status.auto_discover.as_ref().unwrap();
    assert!(auto_discover.failure.is_none());
    let success = auto_discover.last_success.as_ref().unwrap();
    assert!(success.ts > last_success_time);
    insta::assert_json_snapshot!(success, {
        ".ts" => "[ts]",
    }, @r###"
    {
      "ts": "[ts]",
      "added": [
        {
          "resource_path": [
            "flowers"
          ],
          "target": "marmots/flowers",
          "disable": false
        }
      ],
      "removed": [
        {
          "resource_path": [
            "moss"
          ],
          "target": "marmots/moss",
          "disable": true
        }
      ],
      "publish_result": {
        "type": "success"
      }
    }
    "###);
    let bindings = &capture_state
        .live_spec
        .as_ref()
        .unwrap()
        .as_capture()
        .unwrap()
        .bindings;
    insta::assert_json_snapshot!(bindings, @r###"
    [
      {
        "resource": {
          "$serde_json::private::RawValue": "{\"_meta\":{\"path\":[\"flowers\"]},\"extra\":\"flowers\",\"id\":\"flowers\"}"
        },
        "target": "marmots/flowers"
      },
      {
        "resource": {
          "$serde_json::private::RawValue": "{\"_meta\":{\"path\":[\"grass\"]},\"extra\":\"grass\",\"id\":\"grass\"}"
        },
        "target": "marmots/grass"
      }
    ]
    "###);

    harness.run_pending_controllers(Some(6)).await;
    let materialization_state = harness.get_controller_state("marmots/materialize").await;
    let model = materialization_state.live_spec.as_ref().unwrap();
    let bindings = &model.as_materialization().unwrap().bindings;
    insta::assert_json_snapshot!(bindings, @r###"
    [
      {
        "resource": {
          "$serde_json::private::RawValue": "{\"_meta\":{\"path\":[\"marmots_flowers\"]},\"id\":\"marmots_flowers\"}"
        },
        "source": "marmots/flowers",
        "fields": {
          "recommended": true
        }
      },
      {
        "resource": {
          "$serde_json::private::RawValue": "{\"_meta\":{\"path\":[\"marmots_grass\"]},\"id\":\"marmots_grass\"}"
        },
        "source": "marmots/grass",
        "fields": {
          "recommended": true
        }
      }
    ]
    "###);

    // Final snapshot of the publication history
    let pub_history = &capture_state
        .current_status
        .unwrap_capture()
        .publications
        .history
        .iter()
        .map(|e| (e.detail.as_ref(), &e.result))
        .collect::<Vec<_>>();
    insta::assert_json_snapshot!(pub_history, @r###"
    [
      [
        "auto-discover changes (1 added, 0 modified, 1 removed)\nUpdated 'marmots/capture':\nupdated resource /_meta of 1 bindings",
        {
          "type": "success"
        }
      ],
      [
        "auto-discover changes (2 added, 0 modified, 0 removed)\nUpdated 'marmots/capture':\nupdated resource /_meta of 1 bindings",
        {
          "type": "success"
        }
      ]
    ]
    "###);
}

#[tokio::test]
#[serial_test::serial]
async fn test_auto_discovers_no_evolution() {
    let mut harness = TestHarness::init("test_auto_discovers_no_evolution").await;

    let user_id = harness.setup_tenant("mules").await;

    // Start out by doing a user-initiated discover and publishing the results.
    // The discover should be merged with this spec.
    let init_draft = draft_catalog(json!({
        "captures": {
            "mules/capture": {
                "autoDiscover": {
                    "addNewBindings": false,
                    "evolveIncompatibleCollections": false,
                },
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": { "hee": "haw" }
                    },
                },
                "bindings": [ ]
            },
        }
    }));
    let draft_id = harness
        .create_draft(user_id, "mules-init-draft", init_draft)
        .await;
    let discovered = Discovered {
        bindings: vec![Binding {
            recommended_name: "hey".to_string(),
            resource_config_json: r#"{"id": "hey"}"#.to_string(),
            document_schema_json: document_schema(1).to_string(),
            key: vec!["/id".to_string()],
            disable: false,
            resource_path: Vec::new(),
            is_fallback_key: false,
        }],
    };
    let result = harness
        .user_discover(
            "source/test",
            ":test",
            "mules/capture",
            draft_id,
            r#"{"hee": "hawwww"}"#,
            false,
            Ok((spec_fixture(), discovered.clone())),
        )
        .await;
    assert!(result.job_status.is_success());
    let result = harness
        .create_user_publication(user_id, draft_id, "mules init_draft")
        .await;
    assert!(result.status.is_success());

    harness.run_pending_controllers(None).await;

    let discovered_diff_key = Discovered {
        bindings: vec![Binding {
            recommended_name: "hey".to_string(),
            resource_config_json: r#"{"id": "hey"}"#.to_string(),
            document_schema_json: document_schema(1).to_string(),
            key: vec!["/id".to_string(), "/squeaks".to_string()],
            disable: false,
            resource_path: Vec::new(),
            is_fallback_key: false,
        }],
    };
    harness.set_auto_discover_due("mules/capture").await;
    harness
        .discover_handler
        .connectors
        .mock_discover("mules/capture", Ok((spec_fixture(), discovered_diff_key)));
    harness.run_pending_controller("mules/capture").await;

    let capture_state = harness.get_controller_state("mules/capture").await;
    assert!(capture_state.error.is_some());
    assert_eq!(1, capture_state.failures);
    harness.assert_controller_pending("mules/capture").await;
    let capture_status = capture_state.current_status.unwrap_capture();
    // Expect to see that the discover succeeded, but that the publication failed.
    insta::assert_json_snapshot!(capture_status, {
        ".activation.last_activated" => "[build_id]",
        ".activation.last_activated_at" => "[ts]",
        ".activation.shard_status.first_ts" => "[ts]",
        ".activation.shard_status.last_ts" => "[ts]",
        ".auto_discover.next_at" => "[ts]",
        ".auto_discover.failure.first_ts" => "[ts]",
        ".auto_discover.failure.last_outcome.ts" => "[ts]",
        ".publications.max_observed_pub_id" => "[pub_id]",
        ".publications.history[].id" => "[pub_id]",
        ".publications.history[].created" => "[ts]",
        ".publications.history[].completed" => "[ts]",
    }, @r###"
    {
      "publications": {
        "max_observed_pub_id": "[pub_id]",
        "history": [
          {
            "id": "[pub_id]",
            "created": "[ts]",
            "completed": "[ts]",
            "detail": "auto-discover changes (0 added, 1 modified, 0 removed)",
            "result": {
              "type": "buildFailed"
            },
            "errors": [
              {
                "catalog_name": "mules/hey",
                "scope": "flow://collection/mules/hey#/key",
                "detail": "the key of existing collection mules/hey cannot change (from [\"/id\"] to [\"/id\", \"/squeaks\"]) without also resetting it"
              }
            ]
          }
        ]
      },
      "activation": {
        "last_activated": "[build_id]",
        "shard_status": {
          "last_ts": "[ts]",
          "first_ts": "[ts]",
          "status": "Pending"
        },
        "last_activated_at": "[ts]"
      },
      "auto_discover": {
        "next_at": "[ts]",
        "failure": {
          "count": 1,
          "first_ts": "[ts]",
          "last_outcome": {
            "ts": "[ts]",
            "modified": [
              {
                "resource_path": [
                  "hey"
                ],
                "target": "mules/hey",
                "disable": false
              }
            ],
            "publish_result": {
              "type": "buildFailed"
            }
          }
        }
      }
    }
    "###);

    let status = harness.status_summary("mules/capture").await;
    assert_eq!(StatusSummaryType::Error, status.status);
    assert!(
        status
            .message
            .contains("auto-discover publication failed with: BuildFailed"),
        "unexpected status summary: {}",
        status.message
    );
    // No alert should yet have fired.
    harness
        .assert_alert_clear("mules/capture", AlertType::AutoDiscoverFailed)
        .await;

    // Attempt the discover twice more, for a total of 3 failures, which should
    // trigger an alert.
    for expect_fail_count in 2..4 {
        harness.set_auto_discover_due("mules/capture").await;
        let state = harness.run_pending_controller("mules/capture").await;
        assert!(state.error.is_some());
        let ad_status = state
            .current_status
            .unwrap_capture()
            .auto_discover
            .as_ref()
            .unwrap();
        let failure = ad_status.failure.as_ref().unwrap();
        assert_eq!(expect_fail_count, failure.count);

        let status = harness.status_summary("mules/capture").await;
        assert_eq!(StatusSummaryType::Error, status.status);
        assert!(
            status
                .message
                .contains("auto-discover publication failed with: BuildFailed"),
            "unexpected status summary: {}",
            status.message
        );
    }

    harness
        .assert_alert_firing("mules/capture", AlertType::AutoDiscoverFailed)
        .await;

    // Now simulate the discovered key going back to normal and assert that it succeeds
    harness.set_auto_discover_due("mules/capture").await;
    harness
        .discover_handler
        .connectors
        .mock_discover("mules/capture", Ok((spec_fixture(), discovered)));
    harness.run_pending_controller("mules/capture").await;

    let capture_state = harness.get_controller_state("mules/capture").await;
    assert!(
        capture_state.error.is_none(),
        "expected no error, got: {:?}",
        capture_state.error
    );
    let auto_discover = capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .as_ref()
        .unwrap();
    assert!(auto_discover.last_success.is_some());
    assert!(auto_discover
        .last_success
        .as_ref()
        .unwrap()
        .publish_result
        .is_none());
    assert!(auto_discover.failure.is_none());

    harness
        .assert_alert_clear("mules/capture", AlertType::AutoDiscoverFailed)
        .await;
}

#[tokio::test]
#[serial_test::serial]
async fn test_auto_discovers_update_only() {
    let mut harness = TestHarness::init("test_auto_discovers_update_only").await;

    let user_id = harness.setup_tenant("pikas").await;

    let init_draft = draft_catalog(json!({
        "captures": {
            "pikas/capture": {
                "autoDiscover": {
                    "addNewBindings": false,
                    "evolveIncompatibleCollections": true,
                },
                "shards": {
                    "logLevel": "debug"
                },
                "interval": "42s",
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": { "squeak": "squeak" }
                    },
                },
                "bindings": [
                    {
                        "resource": { "id": "grass", "extra": "grass" },
                        "target": "pikas/alpine-grass"
                    },
                    {
                        "resource": { "id": "moss", "extra": "moss" },
                        "target": "pikas/moss"
                    },
                    {
                        "resource": { "id": "lichen", "extra": "lichen" },
                        "target": "pikas/lichen",
                        "disable": true,
                    }
                ]
            },
            // This is just to ensure that we don't auto-discover disabled captures
            "pikas/disabled-capture": {
                "autoDiscover": {
                    "addNewBindings": false,
                    "evolveIncompatibleCollections": true,
                },
                "shards": {
                    "disable": true
                },
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": { "squeak": "" }
                    },
                },
                "bindings": [ ]
            },
            "pikas/capture-auto-disco-disabled": {
                "autoDiscover": null,
                "shards": {
                    "disable": true
                },
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": { "squeak": "" }
                    },
                },
                "bindings": [ ]
            },
        },
        "collections": {
            "pikas/alpine-grass": {
                "schema": document_schema(1),
                "key": ["/id"]
            },
            "pikas/moss": {
                "schema": document_schema(1),
                "key": ["/id"]
            },
            "pikas/lichen": {
                "writeSchema": document_schema(1),
                "readSchema": models::Schema::default_inferred_read_schema(),
                "key": ["/id"]
            }
        },
        "materializations": {
            "pikas/materialize": {
                "sourceCapture": "pikas/capture",
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": { "squeak": "squeak squeak" }
                    }
                },
                "bindings": [] // let the materialization controller fill them in
            }
        }
    }));

    let result = harness
        .user_publication(user_id, "init publication", init_draft)
        .await;
    assert!(result.status.is_success());

    harness.run_pending_controllers(None).await;

    // Expect to see that the controller has initialized a blank auto-capture status.
    let capture_state = harness.get_controller_state("pikas/capture").await;
    harness.assert_controller_pending("pikas/capture").await;
    assert!(capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .is_some());

    let disabled_state = harness.get_controller_state("pikas/disabled-capture").await;
    harness
        .assert_controller_not_pending("pikas/disabled-capture")
        .await;
    assert!(
        disabled_state
            .current_status
            .unwrap_capture()
            .auto_discover
            .is_none(),
        "expect auto-discover status to be None since this was published as disabled"
    );
    let ad_disabled_state = harness
        .get_controller_state("pikas/capture-auto-disco-disabled")
        .await;
    harness
        .assert_controller_not_pending("pikas/capture-auto-disco-disabled")
        .await;
    assert!(ad_disabled_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .is_none());

    harness.set_auto_discover_due("pikas/capture").await;
    let discovered = Discovered {
        bindings: vec![
            Binding {
                recommended_name: "grass".to_string(),
                resource_config_json: r#"{"id": "grass"}"#.to_string(),
                document_schema_json: document_schema(2).to_string(),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            Binding {
                recommended_name: "moss".to_string(),
                resource_config_json:
                    r#"{"id": "moss", "expect": "existing config takes precedence" }"#.to_string(),
                document_schema_json: document_schema(1).to_string(),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            Binding {
                recommended_name: "lichen".to_string(),
                resource_config_json: r#"{"id": "lichen"}"#.to_string(),
                document_schema_json: document_schema(1).to_string(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
        ],
    };
    harness
        .discover_handler
        .connectors
        .mock_discover("pikas/capture", Ok((spec_fixture(), discovered)));

    harness.run_pending_controller("pikas/capture").await;
    let capture_state = harness.get_controller_state("pikas/capture").await;
    let auto_discover = capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .as_ref()
        .unwrap();

    assert!(auto_discover.failure.is_none());
    assert!(auto_discover.last_success.is_some());
    let last_success = auto_discover.last_success.as_ref().unwrap();

    assert_eq!(
        &changes(&[(&["grass"], "pikas/alpine-grass", false),]),
        &last_success.modified
    );
    assert!(last_success.added.is_empty());
    assert!(last_success.removed.is_empty());
    assert!(last_success
        .publish_result
        .as_ref()
        .is_some_and(|pr| pr.is_success()));
    let last_disco_time = last_success.ts;

    harness.set_auto_discover_due("pikas/capture").await;
    harness.run_pending_controller("pikas/capture").await;

    let capture_state = harness.get_controller_state("pikas/capture").await;
    let auto_discover = capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .as_ref()
        .unwrap();

    assert!(auto_discover.failure.is_none());
    assert!(auto_discover.last_success.is_some());
    let last_success = auto_discover.last_success.as_ref().unwrap();
    assert!(last_success.ts > last_disco_time);
    assert!(last_success.added.is_empty());
    assert!(last_success.modified.is_empty());
    assert!(last_success.removed.is_empty());
    assert!(last_success.publish_result.is_none());

    // Now simulate a discover error, and expect to see the error status reported.
    harness.discover_handler.connectors.mock_discover(
        "pikas/capture",
        Err("a simulated discover error".to_string()),
    );
    harness.set_auto_discover_due("pikas/capture").await;
    harness.run_pending_controller("pikas/capture").await;

    let capture_state = harness.get_controller_state("pikas/capture").await;
    assert!(capture_state.error.is_some());
    assert_eq!(1, capture_state.failures);
    harness.assert_controller_pending("pikas/capture").await;
    let auto_discover = capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .as_ref()
        .unwrap();
    assert!(auto_discover.failure.is_some());
    let failure = auto_discover.failure.as_ref().unwrap();
    assert!(failure.last_outcome.errors[0]
        .detail
        .contains("a simulated discover error"));
    assert_eq!(1, failure.count);
    assert!(
        auto_discover
            .next_at
            .is_some_and(|n| n > chrono::Utc::now()),
        "expected next_at to be in the future after discover failed, got: {:?}",
        auto_discover.next_at
    );
    // Alert should not have been triggered yet
    harness
        .assert_alert_clear("pikas/capture", AlertType::AutoDiscoverFailed)
        .await;
    // But the status should show the error
    let status = harness.status_summary("pikas/capture").await;
    assert_eq!(StatusSummaryType::Error, status.status);
    assert!(
        status.message.contains("auto-discover failed"),
        "unexpected status: {status:?}"
    );

    // A subsequent controller run should result in an error due to backing off the auto-discover
    let capture_state = harness.run_pending_controller("pikas/capture").await;
    let error = capture_state
        .error
        .as_deref()
        .expect("expected controller error, got None");
    assert!(
        error.contains("backing off auto-discover after"),
        "wrong error, got: '{error}'"
    );
    let auto_discover = capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .as_ref()
        .unwrap();
    assert!(auto_discover.failure.is_some());
    let failure = auto_discover.failure.as_ref().unwrap();
    assert_eq!(
        1, failure.count,
        "expect auto-discover was not attempted again"
    );

    harness
        .assert_alert_clear("pikas/capture", AlertType::AutoDiscoverFailed)
        .await;
    let status = harness.status_summary("pikas/capture").await;
    assert_eq!(StatusSummaryType::Error, status.status);
    assert!(
        status
            .message
            .contains("backing off auto-discover after 1 failure"),
        "unexpected status: {status:?}"
    );

    // Simulate the passage of time, and expect the controller to attempt another auto-discover
    harness.set_auto_discover_due("pikas/capture").await;
    let capture_state = harness.run_pending_controller("pikas/capture").await;
    let auto_discover = capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .as_ref()
        .unwrap();
    assert!(auto_discover.failure.is_some());
    let failure = auto_discover.failure.as_ref().unwrap();
    assert_eq!(2, failure.count, "expect auto-discover was re-attempted");
    let status = harness.status_summary("pikas/capture").await;
    assert_eq!(StatusSummaryType::Error, status.status);
    assert!(
        status.message.contains("auto-discover failed"),
        "unexpected status: {status:?}"
    );
    harness
        .assert_alert_clear("pikas/capture", AlertType::AutoDiscoverFailed)
        .await;

    // Now simulate a successful discover, but with a failure to publish. We'll
    // expect to see the error count go up, and an alert to fire because it's the
    // third failed attempt.
    harness.set_auto_discover_due("pikas/capture").await;
    let discovered = Discovered {
        bindings: vec![
            Binding {
                recommended_name: "grass".to_string(),
                resource_config_json: r#"{"id": "grass"}"#.to_string(),
                document_schema_json: document_schema(2).to_string(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            Binding {
                recommended_name: "moss".to_string(),
                resource_config_json:
                    r#"{"id": "moss", "expect": "existing config takes precedence" }"#.to_string(),
                document_schema_json: document_schema(2).to_string(),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            // Lichens is missing, and we expect the corresponding binding to be
            // removed once a successful discover is published.
        ],
    };
    harness
        .discover_handler
        .connectors
        .mock_discover("pikas/capture", Ok((spec_fixture(), discovered)));
    harness.control_plane().fail_next_build(
        "pikas/capture",
        InjectBuildError::new(
            tables::synthetic_scope(models::CatalogType::Capture, "pikas/capture"),
            anyhow::anyhow!("a simulated build failure"),
        ),
    );

    let capture_state = harness.run_pending_controller("pikas/capture").await;
    let error = capture_state
        .error
        .as_deref()
        .expect("expected controller error, got None");
    assert!(
        !error.contains("backing off auto-discover after"),
        "expected _not_ a backoff error, got: '{error}'"
    );
    let auto_discover = capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .as_ref()
        .unwrap();
    assert!(auto_discover.failure.is_some());
    let failure = auto_discover.failure.as_ref().unwrap();
    assert_eq!(3, failure.count, "expect auto-discover was attempted again");
    assert_eq!(
        Some(publications::JobStatus::build_failed()),
        failure.last_outcome.publish_result
    );
    // Ensure that the failed publication is shown in the history.
    let pub_history = capture_state
        .current_status
        .unwrap_capture()
        .publications
        .history
        .front()
        .unwrap();
    assert!(pub_history.errors[0]
        .detail
        .contains("a simulated build failure"));
    let last_fail_time = failure.last_outcome.ts;

    let alert_state = harness
        .assert_alert_firing("pikas/capture", AlertType::AutoDiscoverFailed)
        .await;
    assert_eq!(3, alert_state.count);
    assert!(
        alert_state
            .error
            .contains("auto-discover publication failed"),
        "unexpected alert state: {alert_state:?}"
    );
    assert_eq!(models::CatalogType::Capture, alert_state.spec_type);

    let status = harness.status_summary("pikas/capture").await;
    assert_eq!(StatusSummaryType::Error, status.status);
    assert!(
        status.message.contains("auto-discover publication failed"),
        "unexpected status: {status:?}"
    );

    // Now this time, we'll discover a changed key, and expect that the initial publication fails
    // due to the key change, and that a subsequent publication of a _v2 collection is successful.
    let discovered = Discovered {
        bindings: vec![
            Binding {
                recommended_name: "grass".to_string(),
                resource_config_json: r#"{"id": "grass"}"#.to_string(),
                document_schema_json: document_schema(2).to_string(),
                key: vec!["/id".to_string(), "/squeaks".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            Binding {
                recommended_name: "moss".to_string(),
                resource_config_json:
                    r#"{"id": "moss", "expect": "existing config takes precedence" }"#.to_string(),
                document_schema_json: document_schema(2).to_string(),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            // Lichens is missing, and we expect the corresponding binding to be
            // removed once a successful discover is published.
        ],
    };
    harness
        .discover_handler
        .connectors
        .mock_discover("pikas/capture", Ok((spec_fixture(), discovered)));
    harness.set_auto_discover_due("pikas/capture").await;
    harness.run_pending_controller("pikas/capture").await;

    let capture_state = harness.get_controller_state("pikas/capture").await;
    let auto_discover = capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .as_ref()
        .unwrap();
    let last_success = auto_discover.last_success.as_ref().unwrap();
    assert!(last_success.ts > last_fail_time);

    harness
        .assert_alert_clear("pikas/capture", AlertType::AutoDiscoverFailed)
        .await;

    // Assert that the materialization binding has been backfilled for the re-created collection.
    let materialization_state = harness.get_controller_state("pikas/materialize").await;
    let model = materialization_state.live_spec.as_ref().unwrap();
    let bindings = &model.as_materialization().unwrap().bindings;
    insta::assert_json_snapshot!(bindings, @r###"
    [
      {
        "resource": {
          "$serde_json::private::RawValue": "{\"_meta\":{\"path\":[\"pikas_alpine-grass\"]},\"id\":\"pikas_alpine-grass\"}"
        },
        "source": "pikas/alpine-grass",
        "fields": {
          "recommended": true
        }
      },
      {
        "resource": {
          "$serde_json::private::RawValue": "{\"_meta\":{\"path\":[\"pikas_moss\"]},\"id\":\"pikas_moss\"}"
        },
        "source": "pikas/moss",
        "fields": {
          "recommended": true
        }
      }
    ]
    "###);

    // Final snapshot of the publication history
    let pub_history = &capture_state
        .current_status
        .unwrap_capture()
        .publications
        .history
        .iter()
        .map(|e| {
            (
                e.detail.as_ref().map(|d| {
                    regex::Regex::new("generation [0-9a-f]+")
                        .unwrap()
                        .replace_all(d, "generation <redacted>")
                }),
                &e.result,
            )
        })
        .collect::<Vec<_>>();

    insta::assert_json_snapshot!(pub_history, @r###"
    [
      [
        "auto-discover changes (0 added, 2 modified, 1 removed)\nUpdated 'pikas/capture':\nbackfilled binding of reset collection pikas/alpine-grass\nUpdated 'pikas/alpine-grass':\nreset collection to new generation <redacted>",
        {
          "type": "success"
        }
      ],
      [
        "auto-discover changes (0 added, 1 modified, 1 removed)",
        {
          "type": "buildFailed"
        }
      ],
      [
        "auto-discover changes (0 added, 1 modified, 0 removed)",
        {
          "type": "success"
        }
      ]
    ]
    "###);
}

fn changes(c: &[(&[&str], &str, bool)]) -> Vec<DiscoverChange> {
    c.into_iter()
        .map(|(path, target, disable)| DiscoverChange {
            resource_path: path.iter().map(|s| s.to_string()).collect(),
            target: models::Collection::new(*target),
            disable: *disable,
        })
        .collect()
}

fn document_schema(version: usize) -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "squeaks": { "type": "integer", "maximum": version },
        },
        "required": ["id", "squeaks"]
    })
}
