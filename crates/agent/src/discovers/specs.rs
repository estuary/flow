use proto_flow::capture::{response::discovered::Binding, response::Discovered};
use std::collections::BTreeMap;

pub fn parse_response(
    endpoint_config: &serde_json::value::RawValue,
    image_name: &str,
    image_tag: &str,
    response: &[u8],
) -> Result<(models::CaptureEndpoint, Vec<Binding>), serde_json::Error> {
    let image_composed = format!("{image_name}{image_tag}");
    tracing::debug!(%image_composed, response=%String::from_utf8_lossy(response), "converting response");

    let Discovered { mut bindings } = serde_json::from_slice(response)?;

    // Sort bindings so they're consistently ordered on their recommended name.
    // This reduces potential churn if an established capture is refreshed.
    bindings.sort_by(|l, r| l.recommended_name.cmp(&r.recommended_name));

    Ok((
        models::CaptureEndpoint::Connector(models::ConnectorConfig {
            image: image_composed,
            config: endpoint_config.to_owned().into(),
        }),
        bindings,
    ))
}

pub fn merge_capture(
    capture_name: &str,
    endpoint: models::CaptureEndpoint,
    discovered_bindings: Vec<Binding>,
    fetched_capture: Option<models::CaptureDef>,
    update_only: bool,
) -> (models::CaptureDef, Vec<Binding>) {
    let capture_prefix = capture_name.rsplit_once("/").unwrap().0;

    let (fetched_bindings, interval, shards) = match fetched_capture {
        Some(models::CaptureDef {
            endpoint: _,
            bindings: fetched_bindings,
            interval,
            shards,
        }) => (fetched_bindings, interval, shards),

        None => (
            Vec::new(),
            models::CaptureDef::default_interval(),
            models::ShardTemplate::default(),
        ),
    };

    let mut capture_bindings = Vec::new();
    let mut filtered_bindings = Vec::new();

    for discovered_binding in discovered_bindings {
        let Binding {
            recommended_name,
            resource_config_json,
            ..
        } = &discovered_binding;

        let resource: serde_json::Value = serde_json::from_str(&resource_config_json).unwrap();

        // Attempt to find a fetched binding such that the discovered resource
        // spec is a strict subset of the fetched resource spec. In other
        // words the fetched resource spec may have _extra_ locations,
        // but all locations of the discovered resource spec must be equal.
        let fetched_binding = fetched_bindings
            .iter()
            .filter(|fetched| {
                doc::diff(Some(&serde_json::json!(&fetched.resource)), Some(&resource)).is_empty()
            })
            .next();

        if let Some(fetched_binding) = fetched_binding {
            // Preserve the fetched version of a matched CaptureBinding.
            capture_bindings.push(fetched_binding.clone());
            filtered_bindings.push(discovered_binding);
        } else if !update_only {
            // Create a new CaptureBinding.
            capture_bindings.push(models::CaptureBinding {
                target: models::Collection::new(format!("{capture_prefix}/{recommended_name}")),
                resource: models::RawValue::from_value(&resource),
            });
            filtered_bindings.push(discovered_binding);
        }
    }

    (
        models::CaptureDef {
            endpoint,
            bindings: capture_bindings,
            interval,
            shards,
        },
        filtered_bindings,
    )
}

pub fn merge_collections(
    discovered_bindings: Vec<Binding>,
    mut fetched_collections: BTreeMap<models::Collection, models::CollectionDef>,
    targets: Vec<models::Collection>,
) -> BTreeMap<models::Collection, models::CollectionDef> {
    assert_eq!(targets.len(), discovered_bindings.len());

    let mut collections = BTreeMap::new();

    for (
        target,
        Binding {
            key,
            document_schema_json,
            ..
        },
    ) in targets.into_iter().zip(discovered_bindings.into_iter())
    {
        let document_schema: models::Schema = serde_json::from_str(&document_schema_json).unwrap();
        // Unwrap a fetched collection, or initialize a blank one.
        let mut collection =
            fetched_collections
                .remove(&target)
                .unwrap_or_else(|| models::CollectionDef {
                    schema: None,
                    write_schema: None,
                    read_schema: None,
                    key: models::CompositeKey::new(Vec::new()),
                    projections: Default::default(),
                    journals: Default::default(),
                    derive: None,
                    derivation: None,
                });

        if collection.read_schema.is_some() {
            collection.write_schema = Some(document_schema);
        } else {
            collection.schema = Some(document_schema)
        }

        // If the discover didn't provide a key, don't over-write a user's chosen key.
        if !key.is_empty() {
            let pointers = key
                .into_iter()
                .map(models::JsonPointer::new)
                .collect::<Vec<_>>();
            collection.key = models::CompositeKey::new(pointers);
        }

        collections.insert(target.clone(), collection);
    }

    collections
}

#[cfg(test)]
mod tests {
    use super::{BTreeMap, Binding};
    use serde_json::json;

    #[test]
    fn test_response_parsing() {
        let response = json!({
            "bindings": [
                {
                    "recommendedName": "greetings",
                    "resourceConfig": {
                        "stream": "greetings",
                        "syncMode": "incremental"
                    },
                    "documentSchema": {
                        "type": "object",
                        "properties": {
                            "count": { "type": "integer" },
                            "message": { "type": "string" }
                        },
                        "required": [ "count", "message" ]
                    },
                    "key": [ "/count" ]
                },
                {
                    "recommendedName": "frogs",
                    "resourceConfig": {
                        "stream": "greetings",
                        "syncMode": "incremental"
                    },
                    "documentSchema": {
                        "type": "object",
                        "properties": {
                            "croak": { "type": "string" }
                        },
                        "required": [ "croak" ]
                    },
                    "key": [ "/croak" ]
                }
            ]
        })
        .to_string();

        let out = super::parse_response(
            &serde_json::value::RawValue::from_string("{\"some\":\"config\"}".to_string()).unwrap(),
            "ghcr.io/foo/bar/source-potato",
            ":v1.2.3",
            response.as_bytes(),
        )
        .unwrap();

        insta::assert_json_snapshot!(json!(out));
    }

    #[test]
    fn test_merge_collection() {
        let (discovered_bindings, fetched_collections, targets): (
            Vec<Binding>,
            BTreeMap<models::Collection, models::CollectionDef>,
            Vec<models::Collection>,
        ) = serde_json::from_value(json!([
            [
                // case/1: if there is no fetched collection, one is assembled.
                {"documentSchema": {"const": 42}, "key": ["/foo", "/bar"], "recommendedName": "", "resourceConfig": {}},
                // case/2: expect key and schema are updated, but other fields remain.
                {"documentSchema": {"const": 42}, "key": ["/foo", "/bar"], "recommendedName": "", "resourceConfig": {}},
                // case/3: If discovered key is empty, it doesn't replace the collection key.
                {"documentSchema": {"const": 42}, "key": [], "recommendedName": "", "resourceConfig": {}},
                // case/4: If fetched collection has read & write schemas, only the write schema is updated.
                {"documentSchema": {"const": "write!"}, "key": ["/foo", "/bar"], "recommendedName": "", "resourceConfig": {}},
            ],
            {
                "case/2": {
                    "schema": false,
                    "key": ["/old"],
                    "projections": {"field": "/ptr"},
                    "derive": {
                        "using": {"sqlite": {}},
                        "transforms": [],
                    },
                    "journals": {"fragments": {"length": 1234}},
                },
                "case/3": {
                    "schema": false,
                    "key": ["/one", "/two"],
                },
                "case/4": {
                    "writeSchema": false,
                    "readSchema": {"const": "read!"},
                    "key": ["/old"],
                },
            },
            [
                "case/1",
                "case/2",
                "case/3",
                "case/4",
            ]
        ]))
        .unwrap();

        let out = super::merge_collections(discovered_bindings, fetched_collections, targets);

        insta::assert_display_snapshot!(serde_json::to_string_pretty(&out).unwrap());
    }

    #[test]
    fn test_capture_merge_create() {
        let (discovered_endpoint, discovered_bindings)  =
            serde_json::from_value(json!([
                { "connector": { "config": { "discovered": 1 }, "image": "new/image" } },
                [
                    { "recommendedName": "foo", "resourceConfig": { "stream": "foo" }, "key": ["/foo-key"], "documentSchema": { "const": "foo" } },
                ],
            ]))
            .unwrap();

        let out = super::merge_capture(
            "acmeCo/my/capture",
            discovered_endpoint,
            discovered_bindings,
            None,
            false,
        );

        insta::assert_json_snapshot!(json!(out));
    }

    #[test]
    fn test_capture_merge_update() {
        // Fixture is an update of an existing capture, which uses a non-suggested collection name.
        // Additional discovered bindings are filtered.
        let (discovered_endpoint, discovered_bindings, fetched_capture) =
            serde_json::from_value(json!([
                { "connector": { "config": { "discovered": 1 }, "image": "new/image" } },
                [
                    { "recommendedName": "suggested", "resourceConfig": { "stream": "foo" }, "documentSchema": { "const": "discovered" } },
                    { "recommendedName": "other", "resourceConfig": { "stream": "bar" }, "documentSchema": false },
                ],
                {
                  "bindings": [
                    { "resource": { "stream": "foo", "modified": 1 }, "target": "acmeCo/renamed" },
                    { "resource": { "stream": "removed" }, "target": "acmeCo/discarded" },
                  ],
                  "endpoint": { "connector": { "config": { "fetched": 1 }, "image": "old/image" } },
                  // Extra fields which are passed-through.
                  "interval": "34s",
                  "shards": {
                    "maxTxnDuration": "12s"
                  },
                },
            ]))
            .unwrap();

        let out = super::merge_capture(
            "acmeCo/my-capture",
            discovered_endpoint,
            discovered_bindings,
            fetched_capture,
            true,
        );

        // Expect we:
        // * Preserved the modified binding configuration.
        // * Dropped the removed binding.
        // * Updated the endpoint configuration.
        // * Preserved unrelated fields of the capture (shard template and interval).
        insta::assert_json_snapshot!(json!(out));
    }

    #[test]
    fn test_capture_merge_upsert() {
        // Fixture is an upsert of an existing capture which uses a non-suggested collection name.
        let (discovered_endpoint, discovered_bindings, fetched_capture) =
            serde_json::from_value(json!([
                { "connector": { "config": { "discovered": 1 }, "image": "new/image" } },
                [
                    { "recommendedName": "foo", "resourceConfig": { "stream": "foo" }, "documentSchema": { "const": 1 } },
                    { "recommendedName": "bar", "resourceConfig": { "stream": "bar" }, "documentSchema": { "const": 2 } },
                    { "recommendedName": "baz", "resourceConfig": { "stream": "baz" }, "documentSchema": { "const": 3 } },
                ],
                {
                  "bindings": [
                    { "resource": { "stream": "bar", "modified": 2 }, "target": "acmeCo/bar" },
                    { "resource": { "stream": "foo", "modified": 1 }, "target": "acmeCo/renamed" },
                  ],
                  "endpoint": { "connector": { "config": { "fetched": 1 }, "image": "old/image" } }
                },
            ]))
            .unwrap();

        let out = super::merge_capture(
            "acmeCo/my-capture",
            discovered_endpoint,
            discovered_bindings,
            fetched_capture,
            false,
        );

        // Expect we:
        // * Preserved the modified binding configurations.
        // * Added the new binding.
        // * Updated the endpoint configuration.
        insta::assert_json_snapshot!(json!(out));
    }
}
