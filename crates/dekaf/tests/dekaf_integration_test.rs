use anyhow::Context;
use dekaf::connector::DekafConfig;
use futures::StreamExt;
use locate_bin;
use rand::Rng;
use rdkafka::{consumer::Consumer, Message};
use schema_registry_converter::async_impl::avro;
use schema_registry_converter::async_impl::schema_registry;
use serde_json::json;
use std::{collections::HashMap, env, io::Write, time::Duration};

async fn create_consumer<'a>(
    username: &'a str,
    token: &'a str,
    topic: &'a str,
) -> (rdkafka::consumer::StreamConsumer, avro::AvroDecoder<'a>) {
    #[allow(non_snake_case)]
    let DEKAF_BROKER = env::var("DEKAF_BROKER").expect("Missing DEKAF_BROKER environment variable");
    #[allow(non_snake_case)]
    let SCHEMA_REGISTRY =
        env::var("DEKAF_REGISTRY").expect("Missing DEKAF_REGISTRY environment variable");

    let consumer: rdkafka::consumer::StreamConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", DEKAF_BROKER)
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanism", "PLAIN")
        .set("sasl.username", username)
        .set("sasl.password", token)
        .set("group.id", "this_needs_to_be_set_but_we_dont_use_it")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "smallest")
        .set("enable.auto.offset.store", "false")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(vec![topic].as_slice())
        .expect("Consumer subscription failed");

    let decoder = avro::AvroDecoder::new(
        schema_registry::SrSettings::new_builder(String::from(SCHEMA_REGISTRY))
            .set_basic_authorization(username, Some(token))
            .build()
            .expect("failed to build avro decoder"),
    );

    (consumer, decoder)
}

#[derive(Debug)]
enum SpecAction {
    Create,
    Delete,
}

async fn test_specs(
    name: &str,
    action: SpecAction,
    mut capture: models::CaptureDef,
    collections: HashMap<&str, models::CollectionDef>,
    mut materialization: models::MaterializationDef,
) -> anyhow::Result<(String, String)> {
    let mut temp_flow = tempfile::NamedTempFile::new()?;

    let suffix: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(4)
        .map(char::from)
        .collect();

    let capture_name = format!("{}/{suffix}/source-http-ingest", name);
    let materialization_name = format!("{}/{suffix}/test-dekaf", name);

    tracing::info!(temp=?temp_flow, "Attempting to {:?}", action);

    // rewrite capture bindings
    capture.bindings.iter_mut().for_each(|binding| {
        binding.target = models::Collection::new(format!("{name}/{}", binding.target.to_string()))
    });

    // rewrite materialization sources
    materialization.bindings.iter_mut().for_each(|binding| {
        binding
            .source
            .set_collection(models::Collection::new(format!(
                "{name}/{}",
                binding.source.collection()
            )))
    });

    let collections_mapped = collections
        .into_iter()
        .fold(HashMap::new(), |mut state, (k, v)| {
            state.insert(format!("{name}/{k}"), v);
            state
        });

    let file_contents = json!({
        "captures": {
            &capture_name: capture
        },
        "collections": collections_mapped,
        "materializations": {
            &materialization_name: materialization
        }
    });

    temp_flow.write(serde_json::to_vec(&file_contents)?.as_slice())?;

    let flowctl = locate_bin::locate("flowctl").context("failed to locate flowctl")?;

    let async_process::Output {
        stderr,
        stdout: _stdout,
        status,
    } = async_process::output(
        async_process::Command::new(flowctl).args(
            match action {
                SpecAction::Create => vec![
                    "catalog",
                    "publish",
                    "--auto-approve",
                    "--default-data-plane",
                    "ops/dp/public/local-cluster",
                    "--source",
                    temp_flow.path().to_str().unwrap(),
                ],
                SpecAction::Delete => vec![
                    "catalog",
                    "delete",
                    "--prefix",
                    name,
                    "--captures=true",
                    "--collections=true",
                    "--materializations=true",
                    "--dangerous-auto-approve",
                ],
            }
            .as_slice(),
        ),
    )
    .await
    .context("failed to invoke flowctl")?;

    if !status.success() {
        let output = String::from_utf8_lossy(&stderr);
        if !output.contains("no specs found matching given selector") {
            anyhow::bail!("flowctl failed: {}", output);
        }
    }

    tracing::info!(
        ?capture_name,
        ?materialization_name,
        "Successful {:?}",
        action
    );

    Ok((capture_name, materialization_name))
}

async fn sops_encrypt(input: models::RawValue) -> anyhow::Result<models::RawValue> {
    #[allow(non_snake_case)]
    let KEYRING = env::var("SOPS_KEYRING").unwrap_or(
        "projects/estuary-control/locations/us-central1/keyRings/sops/cryptoKeys/cd-github-control"
            .to_string(),
    );

    let sops = locate_bin::locate("sops").context("failed to locate sops")?;

    let async_process::Output {
        stderr,
        stdout,
        status,
    } = async_process::input_output(
        async_process::Command::new(sops).args([
            "--encrypt",
            "--input-type",
            "json",
            "--output-type",
            "json",
            "--gcp-kms",
            &KEYRING,
            "--encrypted-suffix",
            "_sops",
            "/dev/stdin",
        ]),
        input.get().as_bytes(),
    )
    .await
    .context("failed to run sops")?;

    if !status.success() {
        anyhow::bail!(
            "decrypting sops document failed: {}",
            String::from_utf8_lossy(&stderr),
        );
    }

    Ok(models::RawValue::from_string(
        std::str::from_utf8(stdout.as_slice())
            .context("failed to parse sops output")?
            .to_string(),
    )?)
}

async fn get_shard_info(
    task_name: &str,
) -> anyhow::Result<proto_gazette::consumer::list_response::Shard> {
    let flowctl = locate_bin::locate("flowctl").context("failed to locate flowctl")?;

    let async_process::Output {
        stderr,
        stdout,
        status,
    } = async_process::output(async_process::Command::new(flowctl).args([
        "raw",
        "list-shards",
        "--task",
        task_name,
        "-ojson",
    ]))
    .await
    .context("failed to list shards")?;

    if !status.success() {
        anyhow::bail!(
            "listing shards failed: {}",
            String::from_utf8_lossy(&stderr),
        );
    }

    Ok(serde_json::from_slice(&stdout)?)
}

async fn wait_for_primary(task_name: &str) -> anyhow::Result<()> {
    loop {
        match get_shard_info(task_name).await {
            Err(e) => {
                tracing::warn!(?e, "Error getting shard info");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Ok(info)
                if info.status.iter().any(|status| {
                    status.code() == gazette::consumer::replica_status::Code::Primary
                }) =>
            {
                return Ok(())
            }
            Ok(info)
                if info.status.iter().any(|status| {
                    status.code() == gazette::consumer::replica_status::Code::Failed
                }) =>
            {
                tracing::warn!(statuses = ?info.status, "Shard failed");
                anyhow::bail!("Shard failed");
            }
            Ok(info) => {
                tracing::info!(statuses = ?info.status,"Waiting for primary");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        }
    }
}

async fn send_docs(task_name: &str, path: &str, docs: Vec<models::RawValue>) -> anyhow::Result<()> {
    let shard = get_shard_info(task_name).await?;

    let shard_endpoint = shard
        .route
        .context("missing shard route")?
        .endpoints
        .first()
        .context("missing shard endpoint")?
        .replace("https://", "");
    let shard_labels = shard
        .spec
        .context("missing shard spec")?
        .labels
        .context("missing shard labels")?
        .labels;

    let hostname = &shard_labels
        .iter()
        .find(|lab| lab.name == labels::HOSTNAME)
        .context("missing HOSTNAME label")?
        .value;
    let port = &shard_labels
        .iter()
        .find(|lab| lab.name == labels::EXPOSE_PORT)
        .context("missing HOSTNAME label")?
        .value;

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;
    let url = format!("https://{hostname}-{port}.{shard_endpoint}/{path}");

    tracing::info!(url, "Sending docs");

    for doc in docs {
        let response = client
            .post(url.clone())
            .header("Content-Type", "application/json")
            .body(doc.get().to_string())
            .send()
            .await
            .context("failed to send document")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("failed to send document: status={}, body={}", status, body);
        }
    }

    Ok(())
}

// Note: For the moment, this is going to use an externally-running `agent`, either from Tilt
// or the one running in prod. That means that any changes to `dekaf::connector` will not get
// tested, as that functionality gets baked into the agent/runtime directly. An improvement
// to these tests would be to additionally run/test against a local build of the latest `agent`.
async fn roundtrip(
    name: String,
    endpoint_config: DekafConfig,
    schema: serde_json::Value,
    field_selection: serde_json::Value,
    docs: Vec<serde_json::Value>,
) -> anyhow::Result<()> {
    // Create test collection with specific schema
    // Create test Dekaf materialization
    let materialization_config = sops_encrypt(models::RawValue::from_value(&serde_json::to_value(
        endpoint_config.clone(),
    )?))
    .await?;

    let capture: models::CaptureDef = serde_json::from_value(json!({
        "endpoint": {
            "connector": {
                "image": "ghcr.io/estuary/source-http-ingest:dev",
                "config": sops_encrypt(models::RawValue::from_value(&json!({
                    "paths": ["/data"]
                }))).await?
            }
        },
        "bindings": [{
            "resource": {
                "path": "/data",
                "stream": "/data"
            },
            "target": "single_collection"
        }]
    }))?;

    let collections = HashMap::from_iter(
        vec![("single_collection", serde_json::from_value(schema)?)].into_iter(),
    );

    let materialization: models::MaterializationDef = serde_json::from_value(json!({
        "endpoint": {
            "dekaf": {
                "variant": "foo",
                "config": materialization_config
            }
        },
        "bindings": [
            {
                "resource": {
                    "topic_name": "test_topic"
                },
                "source": "single_collection",
                "fields": field_selection
            }
        ]
    }))?;

    let task_name_prefix = format!("test/dekaf_testing/{name}");

    test_specs(
        &task_name_prefix,
        SpecAction::Delete,
        capture.clone(),
        collections.clone(),
        materialization.clone(),
    )
    .await?;

    let (capture_name, materialization_name) = test_specs(
        &task_name_prefix,
        SpecAction::Create,
        capture.clone(),
        collections.clone(),
        materialization.clone(),
    )
    .await?;

    wait_for_primary(&capture_name).await?;

    tracing::info!("Capture is primary");

    send_docs(
        &capture_name,
        "data",
        docs.iter()
            .map(models::RawValue::from_value)
            .collect::<Vec<_>>(),
    )
    .await?;

    tracing::info!("Sent test docs");

    // Consume test documents
    let (consumer, decoder) =
        create_consumer(&materialization_name, &endpoint_config.token, "test_topic").await;

    let mut doc_stream = consumer.stream();

    let mut counter = 0;
    while let Some(consumed) = doc_stream.next().await {
        let consumed = consumed?;

        // Connfirm that field selection was applied

        let decoded_key = match decoder.decode(consumed.key()).await {
            Err(e) => {
                tracing::error!(err=?e, "Error decoding key");
                return Err(anyhow::Error::from(e));
            }
            Ok(d) => apache_avro::from_value::<serde_json::Value>(&d.value),
        }?;

        insta::assert_json_snapshot!(format!("{name}-key-{counter}"), &decoded_key);

        let decoded_payload = match decoder.decode(consumed.payload()).await {
            Err(e) => {
                tracing::error!(err=?e, "Error decoding value");
                return Err(anyhow::Error::from(e));
            }
            Ok(d) => apache_avro::from_value::<serde_json::Value>(&d.value),
        }?;

        insta::assert_json_snapshot!(format!("{name}-value-{counter}"), &decoded_payload, {
            ".flow_published_at.json" => "[timestamp]",
            ".flow_document._flow_extra._meta.json" => "[contains_timestamp]"
        });

        counter += 1;
        if counter >= docs.len() {
            break;
        }
    }

    // Delete test specs

    test_specs(
        &task_name_prefix,
        SpecAction::Delete,
        capture.clone(),
        collections.clone(),
        materialization.clone(),
    )
    .await?;
    Ok(())
}

#[ignore]
#[tokio::test]
#[tracing_test::traced_test]
async fn test_field_selection_specific() -> anyhow::Result<()> {
    roundtrip(
        "field_selection_specific".to_string(),
        dekaf::connector::DekafConfig {
            deletions: dekaf::connector::DeletionMode::Kafka,
            strict_topic_names: false,
            token: "1234".to_string(),
        },
        json!({
            "schema": {
                "properties": {
                    "key": {
                        "type": "string"
                    },
                    "field_a": {
                        "type": "string",
                    },
                    "field_b": {
                        "type": "string",
                    }
                },
                "type": "object",
                "required": [
                    "key",
                    "field_a",
                    "field_b"
                ],
            },
            "key": [
                "/key"
            ]
        }),
        json!({
            "include": {
                "field_a": {}
            },
            "recommended": false
        }),
        vec![json!({
            "key": "first",
            "field_a": "foo",
            "field_b": "bar"
        })],
    )
    .await
}

#[ignore]
#[tokio::test]
#[tracing_test::traced_test]
async fn test_field_selection_recommended() -> anyhow::Result<()> {
    roundtrip(
        "field_selection_recommended".to_string(),
        dekaf::connector::DekafConfig {
            deletions: dekaf::connector::DeletionMode::Kafka,
            strict_topic_names: false,
            token: "1234".to_string(),
        },
        json!({
            "schema": {
                "properties": {
                    "key": {
                        "type": "string"
                    },
                    "field_a": {
                        "type": "string",
                    },
                    "field_b": {
                        "type": "string",
                    }
                },
                "type": "object",
                "required": [
                    "key",
                    "field_a",
                    "field_b"
                ],
            },
            "key": [
                "/key"
            ]
        }),
        json!({
            "recommended": true
        }),
        vec![json!({
            "key": "first",
            "field_a": "foo",
            "field_b": "bar"
        })],
    )
    .await
}

#[ignore]
#[tokio::test]
#[tracing_test::traced_test]
async fn test_field_selection_flow_document() -> anyhow::Result<()> {
    roundtrip(
        "field_selection_flow_document".to_string(),
        dekaf::connector::DekafConfig {
            deletions: dekaf::connector::DeletionMode::Kafka,
            strict_topic_names: false,
            token: "1234".to_string(),
        },
        json!({
            "schema": {
                "properties": {
                    "key": {
                        "type": "string"
                    },
                    "field_a": {
                        "type": "string",
                    },
                    "field_b": {
                        "type": "string",
                    }
                },
                "type": "object",
                "required": [
                    "key",
                    "field_a",
                    "field_b"
                ],
            },
            "key": [
                "/key"
            ]
        }),
        json!({
            "include": {
                "key": {},
                "flow_document": {}
            },
            "exclude": [
                "field_a",
                "field_b"
            ],
            "recommended": true
        }),
        vec![json!({
            "key": "first",
            "field_a": "foo",
            "field_b": "bar"
        })],
    )
    .await
}

#[ignore]
#[tokio::test]
#[tracing_test::traced_test]
async fn test_meta_is_deleted() -> anyhow::Result<()> {
    roundtrip(
        "meta_is_deleted".to_string(),
        dekaf::connector::DekafConfig {
            deletions: dekaf::connector::DeletionMode::CDC,
            strict_topic_names: false,
            token: "1234".to_string(),
        },
        json!({
            "schema": {
                "properties": {
                    "key": {
                        "type": "string"
                    },
                    "field_a": {
                        "type": "string",
                    },
                    "field_b": {
                        "type": "string",
                    },
                    "_meta": {
                        "type": "object",
                        "properties": {
                            "op": {
                                "type": "string"
                            }
                        }
                    }
                },
                "type": "object",
                "required": [
                    "key",
                ],
            },
            "key": [
                "/key"
            ]
        }),
        json!({
            "include": {
                "key": {},
            },
            "recommended": false
        }),
        vec![
            json!({
                "key": "first",
                "field_a": "foo",
                "field_b": "bar",
                "_meta": {
                    "op": "c"
                }
            }),
            json!({
                "key": "first",
                "field_a": "foo",
                "field_b": "bar",
                "_meta": {
                    "op": "d"
                }
            }),
        ],
    )
    .await
}

#[ignore]
#[tokio::test]
#[tracing_test::traced_test]
async fn test_fields_not_required() -> anyhow::Result<()> {
    roundtrip(
        "fields_not_required".to_string(),
        dekaf::connector::DekafConfig {
            deletions: dekaf::connector::DeletionMode::Kafka,
            strict_topic_names: false,
            token: "1234".to_string(),
        },
        json!({
            "schema": {
                "properties": {
                    "key": {
                        "type": "string"
                    },
                    "field_a": {
                        "type": "string",
                    },
                },
                "type": "object",
                "required": [
                    "key",
                ],
            },
            "key": [
                "/key"
            ]
        }),
        json!({
            "recommended": true
        }),
        vec![json!({
            "key": "first",
            // Omitting "field_a"
        })],
    )
    .await
}
