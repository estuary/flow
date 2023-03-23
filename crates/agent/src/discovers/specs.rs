use std::collections::BTreeMap;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredBinding {
    /// A recommended display name for this discovered binding.
    pub recommended_name: String,
    /// JSON-encoded object which specifies the endpoint resource to be captured.
    pub resource_spec: models::RawValue,
    /// JSON schema of documents produced by this binding.
    pub document_schema: models::Schema,
    /// Composite key of documents (if known), as JSON-Pointers.
    #[serde(default)]
    pub key_ptrs: Vec<models::JsonPointer>,
}

pub fn parse_response(
    endpoint_config: &serde_json::value::RawValue,
    image_name: &str,
    image_tag: &str,
    response: &[u8],
) -> Result<(models::CaptureEndpoint, Vec<DiscoveredBinding>), serde_json::Error> {
    let image_composed = format!("{image_name}{image_tag}");
    tracing::debug!(%image_composed, response=%String::from_utf8_lossy(response), "converting response");

    // Response is the expected shape of a discover response.
    #[derive(serde::Deserialize)]
    struct Response {
        #[serde(default)]
        bindings: Vec<DiscoveredBinding>,
    }
    let Response { mut bindings } = serde_json::from_slice(response)?;

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
    discovered_bindings: Vec<DiscoveredBinding>,
    fetched_capture: Option<models::CaptureDef>,
    update_only: bool,
) -> (models::CaptureDef, Vec<DiscoveredBinding>) {
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
        let DiscoveredBinding {
            recommended_name,
            resource_spec,
            ..
        } = &discovered_binding;

        // Attempt to find a fetched binding such that the discovered resource
        // spec is a strict subset of the fetched resource spec. In other
        // words the fetched resource spec may have _extra_ locations,
        // but all locations of the discovered resource spec must be equal.
        let fetched_binding = fetched_bindings
            .iter()
            .filter(|fetched| {
                doc::diff(
                    Some(&serde_json::json!(&fetched.resource)),
                    Some(&serde_json::json!(resource_spec)),
                )
                .is_empty()
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
                resource: resource_spec.clone(),
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
    discovered_bindings: Vec<DiscoveredBinding>,
    mut fetched_collections: BTreeMap<models::Collection, models::CollectionDef>,
    targets: Vec<models::Collection>,
) -> BTreeMap<models::Collection, models::CollectionDef> {
    assert_eq!(targets.len(), discovered_bindings.len());

    let mut collections = BTreeMap::new();

    for (
        target,
        DiscoveredBinding {
            key_ptrs,
            document_schema,
            ..
        },
    ) in targets.into_iter().zip(discovered_bindings.into_iter())
    {
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
        if !key_ptrs.is_empty() {
            collection.key = models::CompositeKey::new(key_ptrs);
        }

        collections.insert(target.clone(), collection);
    }

    collections
}

#[cfg(test)]
mod tests {
    use super::{BTreeMap, DiscoveredBinding};
    use serde_json::json;

    #[test]
    fn test_response_parsing() {
        let response = json!({
            "bindings": [
                {
                    "recommendedName": "greetings",
                    "resourceSpec": {
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
                    "keyPtrs": [ "/count" ]
                },
                {
                    "recommendedName": "frogs",
                    "resourceSpec": {
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
                    "keyPtrs": [ "/croak" ]
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
            Vec<DiscoveredBinding>,
            BTreeMap<models::Collection, models::CollectionDef>,
            Vec<models::Collection>,
        ) = serde_json::from_value(json!([
            [
                // case/1: if there is no fetched collection, one is assembled.
                {"documentSchema": {"const": 42}, "keyPtrs": ["/foo", "/bar"], "recommendedName": "", "resourceSpec": {}},
                // case/2: expect key and schema are updated, but other fields remain.
                {"documentSchema": {"const": 42}, "keyPtrs": ["/foo", "/bar"], "recommendedName": "", "resourceSpec": {}},
                // case/3: If discovered key is empty, it doesn't replace the collection key.
                {"documentSchema": {"const": 42}, "keyPtrs": [], "recommendedName": "", "resourceSpec": {}},
                // case/4: If fetched collection has read & write schemas, only the write schema is updated.
                {"documentSchema": {"const": "write!"}, "keyPtrs": ["/foo", "/bar"], "recommendedName": "", "resourceSpec": {}},
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
                    { "recommendedName": "foo", "resourceSpec": { "stream": "foo" }, "keyPtrs": ["/foo-key"], "documentSchema": { "const": "foo" } },
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
                    { "recommendedName": "suggested", "resourceSpec": { "stream": "foo" }, "documentSchema": { "const": "discovered" } },
                    { "recommendedName": "other", "resourceSpec": { "stream": "bar" }, "documentSchema": false },
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
                    { "recommendedName": "foo", "resourceSpec": { "stream": "foo" }, "documentSchema": { "const": 1 } },
                    { "recommendedName": "bar", "resourceSpec": { "stream": "bar" }, "documentSchema": { "const": 2 } },
                    { "recommendedName": "baz", "resourceSpec": { "stream": "baz" }, "documentSchema": { "const": 3 } },
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
