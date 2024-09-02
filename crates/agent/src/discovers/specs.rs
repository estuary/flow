use proto_flow::capture::{self, response::discovered::Binding};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;

pub fn parse_response(
    endpoint_config: &serde_json::value::RawValue,
    image_name: &str,
    image_tag: &str,
    response: capture::Response,
) -> anyhow::Result<(models::CaptureEndpoint, Vec<Binding>)> {
    let image_composed = format!("{image_name}{image_tag}");

    let capture::Response {
        discovered: Some(capture::response::Discovered { mut bindings }),
        ..
    } = response
    else {
        anyhow::bail!("response is not a discovered");
    };

    // Sort bindings so they're consistently ordered on their recommended name.
    // This reduces potential churn if an established capture is refreshed.
    bindings.sort_by(|l, r| l.recommended_name.cmp(&r.recommended_name));

    for binding in &mut bindings {
        binding.recommended_name = normalize_recommended_name(&binding.recommended_name);
    }
    if bindings.iter().any(|b| !b.resource_path.is_empty()) {
        tracing::warn!(%image_name, %image_tag,
            "connector discovered response includes deprecated field 'resource_path'");
    }

    Ok((
        models::CaptureEndpoint::Connector(models::ConnectorConfig {
            image: image_composed,
            config: endpoint_config.to_owned().into(),
        }),
        bindings,
    ))
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq)]
pub enum BindingType {
    Existing,
    Discovered,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InvalidResource {
    pub binding_type: BindingType,
    pub resource_path_pointer: String,
    pub resource_json: String,
}

impl fmt::Display for InvalidResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ty = match self.binding_type {
            BindingType::Existing => "existing",
            BindingType::Discovered => "discovered",
        };
        write!(
            f,
            "expected {ty} resource value at '{}' to be a string value, in resource: {}",
            self.resource_path_pointer, self.resource_json
        )
    }
}

type ResourcePath = Vec<String>;

/// Extracts the value of each of the given `resource_path_pointers` and encodes
/// them into a `ResourcePath`. Each pointed-to location must be either a string
/// value, null, or undefined. Null and undefined values are _not_ included in
/// the resulting path, and are thus treated as equivalent. Resource path values
/// other than strings will result in an error.
fn resource_path(
    resource_path_pointers: &[doc::Pointer],
    resource: &serde_json::Value,
) -> Result<ResourcePath, String> {
    let mut path = Vec::new();
    for pointer in resource_path_pointers {
        match pointer.query(resource) {
            None | Some(serde_json::Value::Null) => {
                continue;
            }
            Some(serde_json::Value::String(s)) => path.push(s.clone()),
            Some(_) => return Err(pointer.to_string()),
        }
    }
    Ok(path)
}

fn index_fetched_bindings<'a>(
    resource_path_pointers: &'_ [doc::Pointer],
    bindings: &'a [models::CaptureBinding],
) -> Result<HashMap<ResourcePath, &'a models::CaptureBinding>, InvalidResource> {
    bindings
        .iter()
        .map(|binding| {
            let resource = serde_json::from_str(binding.resource.get())
                .expect("parsing resource config json cannot fail");
            match resource_path(resource_path_pointers, &resource) {
                Ok(rp) => Ok((rp, binding)),
                Err(resource_path_pointer) => Err(InvalidResource {
                    binding_type: BindingType::Existing,
                    resource_path_pointer,
                    resource_json: binding.resource.clone().into(),
                }),
            }
        })
        .collect()
}

pub fn merge_capture(
    capture_name: &str,
    endpoint: models::CaptureEndpoint,
    discovered_bindings: Vec<Binding>,
    fetched_capture: Option<models::CaptureDef>,
    update_only: bool,
    resource_path_pointers: &[String],
) -> Result<(models::CaptureDef, Vec<Binding>), InvalidResource> {
    let capture_prefix = capture_name.rsplit_once("/").unwrap().0;

    let (fetched_bindings, interval, shards, auto_discover) = match fetched_capture {
        Some(models::CaptureDef {
            auto_discover,
            endpoint: _,
            bindings: fetched_bindings,
            interval,
            shards,
            expect_pub_id: _, // The persisted model always has this set to None.
            delete: _,        // The persisted model always has this set to false.
        }) => (fetched_bindings, interval, shards, auto_discover),

        None => (
            Vec::new(),
            models::CaptureDef::default_interval(),
            models::ShardTemplate::default(),
            Some(models::AutoDiscover {
                add_new_bindings: true,
                evolve_incompatible_collections: true,
            }),
        ),
    };

    let pointers = resource_path_pointers
        .iter()
        .map(|p| doc::Pointer::from_str(p.as_str()))
        .collect::<Vec<_>>();

    let fetched_bindings_by_path = if !pointers.is_empty() {
        index_fetched_bindings(&pointers, &fetched_bindings)?
    } else {
        Default::default()
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
        let fetched_binding = if resource_path_pointers.is_empty() {
            // TODO(phil): Legacy matching behavior, to be removed
            fetched_bindings
                .iter()
                .filter(|fetched| {
                    doc::diff(Some(&serde_json::json!(&fetched.resource)), Some(&resource))
                        .is_empty()
                })
                .next()
        } else {
            // New matching behavior
            let discovered_resource = serde_json::from_str(&resource_config_json)
                .expect("resource config must be valid json");
            let discovered_resource_path = resource_path(&pointers, &discovered_resource).map_err(
                |resource_path_pointer| InvalidResource {
                    binding_type: BindingType::Discovered,
                    resource_path_pointer,
                    resource_json: resource_config_json.clone(),
                },
            )?;

            fetched_bindings_by_path
                .get(&discovered_resource_path)
                .map(|b| *b)
        };

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
                backfill: 0,
            });
            filtered_bindings.push(discovered_binding);
        }
    }

    Ok((
        models::CaptureDef {
            auto_discover,
            endpoint,
            bindings: capture_bindings,
            interval,
            shards,
            expect_pub_id: None,
            delete: false,
        },
        filtered_bindings,
    ))
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
                    expect_pub_id: None,
                    delete: false,
                });

        if collection.read_schema.is_some() {
            // If read_schema is already set, it means we're updating an existing collection.
            // It's important that we don't update the `read_schema` in this case, or else we could
            // overwrite a users modifications to it.
            collection.write_schema = Some(document_schema);
        } else if matches!(
            // Does the connector use schema inference?
            document_schema.to_value().get("x-infer-schema"),
            Some(serde_json::Value::Bool(true))
        ) {
            // This is either a new collection, or else discovery has just started asking for
            // the inferred schema. In either case, we must initialize the read schema with the
            // inferred schema placeholder.
            let read_schema = models::Schema::default_inferred_read_schema();
            collection.read_schema = Some(read_schema);
            collection.write_schema = Some(document_schema);
            collection.schema = None;
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
    use itertools::Itertools;
    let mut parts = models::Collection::regex()
        .find_iter(name)
        .map(|m| models::collate::normalize(m.as_str().chars()).collect::<String>());

    parts.join("_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto_flow::capture::{self, response::discovered};
    use serde_json::json;

    #[test]
    fn test_response_parsing() {
        let response: capture::Response = serde_json::from_value(json!({
            "discovered": {
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
                ],
            }
        }))
        .unwrap();

        let out = super::parse_response(
            &serde_json::value::RawValue::from_string("{\"some\":\"config\"}".to_string()).unwrap(),
            "ghcr.io/foo/bar/source-potato",
            ":v1.2.3",
            response,
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

        insta::assert_snapshot!(serde_json::to_string_pretty(&out).unwrap());
    }

    #[test]
    fn test_capture_merge_resource_paths_update() {
        // This is meant to test our merge behavior in the presense of additional fields in the
        // `resource` that are not part of the resource path.
        // Fixture is an update of an existing capture, which uses a non-suggested collection name.
        // There is also a disabled binding, which is expected to remain disabled after the merge.
        // Additional discovered bindings are filtered.
        // Note that fields apart from stream and namespace are modified to demonstrate them being
        // ignored for the purposes of matching up discovered and live bindings (since it's done
        // by resource_path_pointers now)
        let (discovered_endpoint, discovered_bindings, fetched_capture) =
            serde_json::from_value::<(models::CaptureEndpoint, Vec<discovered::Binding>, Option<models::CaptureDef>)>(json!([
                { "connector": { "config": { "discovered": 1 }, "image": "new/image" } },
                [
                    { "recommendedName": "suggested", "resourceConfig": { "stream": "foo", "modified": 0 }, "documentSchema": { "const": "discovered" } },
                    { "recommendedName": "suggested2", "resourceConfig": { "stream": "foo", "namespace": "spacename", "modified": 0 }, "documentSchema": { "const": "discovered-namepaced" } },
                    { "recommendedName": "other", "resourceConfig": { "stream": "bar", "modified": 0 }, "documentSchema": false },
                    { "recommendedName": "other", "resourceConfig": { "stream": "disabled", "modified": 0 }, "documentSchema": false },
                ],
                {
                  "bindings": [
                    { "resource": { "stream": "foo", "modified": 1 }, "target": "acmeCo/renamed" },
                    { "resource": { "stream": "foo", "namespace": "spacename", "modified": 2 }, "target": "acmeCo/renamed-namepaced" },
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
            discovered_endpoint.clone(),
            discovered_bindings.clone(),
            fetched_capture.clone(),
            true,
            &["/stream".to_string(), "/namespace".to_string()],
        )
        .unwrap();

        // Expect we:
        // * Preserved the modified binding configuration.
        // * Dropped the removed binding.
        // * Updated the endpoint configuration.
        // * Preserved unrelated fields of the capture (shard template and interval).
        // * The resources that specify a namespace are treated separately
        insta::assert_json_snapshot!(json!(out));
    }

    #[test]
    fn test_capture_merge_create() {
        let (discovered_endpoint, discovered_bindings)  =
            serde_json::from_value::<(models::CaptureEndpoint, Vec<discovered::Binding>)>(json!([
                { "connector": { "config": { "discovered": 1 }, "image": "new/image" } },
                [
                    { "recommendedName": "foo", "resourceConfig": { "stream": "foo" }, "key": ["/foo-key"], "documentSchema": { "const": "foo" } },
                    { "recommendedName": "bar", "resourceConfig": { "stream": "bar" }, "key": ["/bar-key"], "documentSchema": { "const": "bar" }, "disable": true },
                ],
            ]))
            .unwrap();

        let out = super::merge_capture(
            "acmeCo/my/capture",
            discovered_endpoint.clone(),
            discovered_bindings.clone(),
            None,
            false,
            &[],
        )
        .unwrap();

        insta::assert_json_snapshot!(json!(out));
        // assert that the results of the merge are unchanged when using a valid
        // slice of resource path pointers.
        let path_merge_out = super::merge_capture(
            "acmeCo/my/capture",
            discovered_endpoint,
            discovered_bindings,
            None,
            false,
            &["/stream".to_string()],
        )
        .unwrap();

        assert_eq!(
            json!(out),
            json!(path_merge_out),
            "resource_path_pointers merge output was different"
        );
    }

    #[test]
    fn test_capture_merge_update() {
        // Fixture is an update of an existing capture, which uses a non-suggested collection name.
        // There is also a disabled binding, which is expected to remain disabled after the merge.
        // Additional discovered bindings are filtered.
        let (discovered_endpoint, discovered_bindings, fetched_capture) =
            serde_json::from_value::<(models::CaptureEndpoint, Vec<discovered::Binding>, Option<models::CaptureDef>)>(json!([
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
            discovered_endpoint.clone(),
            discovered_bindings.clone(),
            fetched_capture.clone(),
            true,
            &[],
        )
        .unwrap();

        // Expect we:
        // * Preserved the modified binding configuration.
        // * Dropped the removed binding.
        // * Updated the endpoint configuration.
        // * Preserved unrelated fields of the capture (shard template and interval).
        insta::assert_json_snapshot!(json!(out));
        // assert that the results of the merge are unchanged when using a valid
        // slice of resource path pointers.
        let path_merge_out = super::merge_capture(
            "acmeCo/my-capture",
            discovered_endpoint,
            discovered_bindings,
            fetched_capture,
            true,
            &["/stream".to_string()],
        )
        .unwrap();

        assert_eq!(
            json!(out),
            json!(path_merge_out),
            "resource_path_pointers merge output was different"
        );
    }

    #[test]
    fn test_capture_merge_upsert() {
        // Fixture is an upsert of an existing capture which uses a non-suggested collection name.
        let (discovered_endpoint, discovered_bindings, fetched_capture) =
            serde_json::from_value::<(models::CaptureEndpoint, Vec<discovered::Binding>, Option<models::CaptureDef>)>(json!([
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
            discovered_endpoint.clone(),
            discovered_bindings.clone(),
            fetched_capture.clone(),
            false,
            &[],
        )
        .unwrap();

        // Expect we:
        // * Preserved the modified binding configurations.
        // * Added the new binding.
        // * Updated the endpoint configuration.
        insta::assert_json_snapshot!(json!(out));

        // assert that the results of the merge are unchanged when using a valid
        // slice of resource path pointers.
        let path_merge_out = super::merge_capture(
            "acmeCo/my-capture",
            discovered_endpoint,
            discovered_bindings,
            fetched_capture,
            false,
            &["/stream".to_string()],
        )
        .unwrap();

        assert_eq!(
            json!(out),
            json!(path_merge_out),
            "resource_path_pointers merge output was different"
        );
    }

    #[test]
    fn test_merge_capture_invalid_resource() {
        let (discovered_endpoint, discovered_bindings, fetched_capture) =
            serde_json::from_value::<(models::CaptureEndpoint, Vec<discovered::Binding>, Option<models::CaptureDef>)>(json!([
                { "connector": { "config": { "discovered": 1 }, "image": "new/image" } },
                [
                    { "recommendedName": "foo", "resourceConfig": { "stream": 7 }, "documentSchema": { "const": 1 } },
                ],
                {
                  "bindings": [
                    { "resource": { "stream": {"invalid":"yup"} }, "target": "acmeCo/foo" },
                  ],
                  "endpoint": { "connector": { "config": { "fetched": 1 }, "image": "old/image" } }
                },
            ]))
            .unwrap();

        let err = super::merge_capture(
            "acmeCo/naughty-capture",
            discovered_endpoint.clone(),
            discovered_bindings.clone(),
            None, // omit fetched binding so we error on the discovered one
            false,
            &["/namespace".to_string(), "/stream".to_string()],
        )
        .expect_err("should fail because stream is not a string");
        assert_eq!(BindingType::Discovered, err.binding_type);
        assert_eq!("/stream", err.resource_path_pointer);

        // now assert that an existing invalid binding also results in an error
        let err = super::merge_capture(
            "acmeCo/naughty-capture",
            discovered_endpoint,
            discovered_bindings,
            fetched_capture,
            false,
            &["/namespace".to_string(), "/stream".to_string()],
        )
        .expect_err("should fail because stream is not a string");
        assert_eq!(BindingType::Existing, err.binding_type);
        assert_eq!("/stream", err.resource_path_pointer);
    }

    #[test]
    fn test_recommended_name_normalization() {
        for (name, expect) in [
            ("Foo", "Foo"),
            ("foo/bar", "foo/bar"),
            ("Faſt/Carſ", "Fast/Cars"), // First form is denormalized, assert that it gets NFKC normalized
            ("/", ""),                  // just documenting a weird edge case
            ("/foo/bar//baz/", "foo/bar_baz"), // Invalid leading, middle, & trailing slash.
            ("#੫൬    , bar-_!", "੫൬_bar-_"), // Invalid leading, middle, & trailing chars.
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
