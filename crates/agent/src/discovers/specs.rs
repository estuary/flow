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

    for binding in &mut bindings {
        binding.recommended_name = normalize_recommended_name(&binding.recommended_name);
    }

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

    let (fetched_bindings, interval, shards, auto_discover) = match fetched_capture {
        Some(models::CaptureDef {
            auto_discover,
            endpoint: _,
            bindings: fetched_bindings,
            interval,
            shards,
        }) => (fetched_bindings, interval, shards, auto_discover),

        None => (
            Vec::new(),
            models::CaptureDef::default_interval(),
            models::ShardTemplate::default(),
            None,
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
                disable: discovered_binding.disable,
                resource: models::RawValue::from_value(&resource),
            });
            filtered_bindings.push(discovered_binding);
        }
    }

    (
        models::CaptureDef {
            auto_discover,
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
        let document_schema =
            models::Schema::new(models::RawValue::from_string(document_schema_json).unwrap());

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
                });

        if collection.read_schema.is_some() {
            collection.write_schema = Some(document_schema);
        } else if matches!(
            // Does the connector use schema inference?
            document_schema.to_value().get("x-infer-schema"),
            Some(serde_json::Value::Bool(true))
        ) {
            collection.schema = None;
            collection.write_schema = Some(document_schema);

            // Synthesize a minimal read schema.
            collection.read_schema = Some(models::Schema::new(models::RawValue::from_value(
                &serde_json::json!({
                    "allOf": [{"$ref":"flow://write-schema"},{"$ref":"flow://inferred-schema"}],
                }),
            )));
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

fn normalize_recommended_name(name: &str) -> String {
    let parts: Vec<_> = models::Collection::regex()
        .find_iter(name)
        .map(|m| m.as_str())
        .collect();

    parts.join("_")
}

#[cfg(test)]
mod tests {
    use super::{normalize_recommended_name, BTreeMap, Binding};
    use serde_json::json;

    #[test]
    fn test_response_parsing() {
        let response = json!({
            "bindings": [
                {
                    "recommendedName": "some greetings!",
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
                    "key": [ "/croak" ],
                    "disable": true
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
                {"documentSchema": {"x-infer-schema": true, "const": "write!"}, "key": ["/foo", "/bar"], "recommendedName": "", "resourceConfig": {}},
                // case/5: If there is no fetched collection but schema inference is used, an initial read schema is created.
                {"documentSchema": {"x-infer-schema": true, "const": "write!"}, "key": ["/key"], "recommendedName": "", "resourceConfig": {}},
                // case/6: The fetched collection did not use schema inference, but now does.
                {"documentSchema": {"x-infer-schema": true, "const": "write!"}, "key": ["/key"], "recommendedName": "", "resourceConfig": {}},
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
                "case/6": {
                    "schema": false,
                    "key": ["/old"],
                },
            },
            [
                "case/1",
                "case/2",
                "case/3",
                "case/4",
                "case/5",
                "case/6",
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
                    { "recommendedName": "bar", "resourceConfig": { "stream": "bar" }, "key": ["/bar-key"], "documentSchema": { "const": "bar" }, "disable": true },
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
        // There is also a disabled binding, which is expected to remain disabled after the merge.
        // Additional discovered bindings are filtered.
        let (discovered_endpoint, discovered_bindings, fetched_capture) =
            serde_json::from_value(json!([
                { "connector": { "config": { "discovered": 1 }, "image": "new/image" } },
                [
                    { "recommendedName": "suggested", "resourceConfig": { "stream": "foo" }, "documentSchema": { "const": "discovered" } },
                    { "recommendedName": "other", "resourceConfig": { "stream": "bar" }, "documentSchema": false },
                    { "recommendedName": "other", "resourceConfig": { "stream": "disabled" }, "documentSchema": false },
                ],
                {
                  "bindings": [
                    { "resource": { "stream": "foo", "modified": 1 }, "target": "acmeCo/renamed" },
                    { "resource": { "stream": "removed" }, "target": "acmeCo/discarded" },
                    { "resource": { "stream": "disabled", "modified": "yup" }, "disable": true, "target": "test/collection/disabled" },
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
                    { "recommendedName": "foo", "resourceConfig": { "stream": "foo" }, "documentSchema": { "const": 1 }, "disable": true },
                    { "recommendedName": "bar", "resourceConfig": { "stream": "bar" }, "documentSchema": { "const": 2 }, "disable": true },
                    { "recommendedName": "baz", "resourceConfig": { "stream": "baz" }, "documentSchema": { "const": 3 }, "disable": true },
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

    #[test]
    fn test_recommended_name_normalization() {
        for (name, expect) in [
            ("Foo", "Foo"),
            ("foo/bar", "foo/bar"),
            ("/foo/bar//baz/", "foo/bar_baz"), // Invalid leading, middle, & trailing slash.
            ("#੫൬    , bar-_!", "੫൬_bar-_"),   // Invalid leading, middle, & trailing chars.
            ("One! two/_three", "One_two/_three"),
        ] {
            assert_eq!(
                normalize_recommended_name(name),
                expect,
                "test case: {name}"
            );
        }
    }
}
