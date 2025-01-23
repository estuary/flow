use crate::discovers::Changed;

use anyhow::Context;
use itertools::Itertools;
use models::{discovers::Changes, ResourcePath};
use proto_flow::capture::{self, response::discovered};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::LazyLock;
use tables::DraftCollection;

pub fn parse_response(response: capture::Response) -> anyhow::Result<Vec<discovered::Binding>> {
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
        if binding.recommended_name.trim().is_empty() {
            tracing::error!(
                ?binding,
                "connector discovered response includes a binding with an empty recommended name"
            );
            anyhow::bail!("connector protocol error: a binding was missing 'recommended_name'. Please contact support for assistance");
        }

        binding.recommended_name = normalize_recommended_name(&binding.recommended_name);
    }
    // Log this only once instead of for each binding
    if bindings.iter().any(|b| !b.resource_path.is_empty()) {
        tracing::warn!("connector discovered response includes deprecated field 'resource_path'");
    }

    Ok(bindings)
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
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

impl std::error::Error for InvalidResource {}

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
) -> anyhow::Result<HashMap<ResourcePath, &'a models::CaptureBinding>> {
    let mut map = HashMap::new();
    for binding in bindings.iter() {
        let resource = serde_json::from_str(binding.resource.get())
            .expect("parsing resource config json cannot fail");
        let path =
            resource_path(resource_path_pointers, &resource).map_err(|resource_path_pointer| {
                InvalidResource {
                    binding_type: BindingType::Existing,
                    resource_path_pointer,
                    resource_json: binding.resource.clone().into(),
                }
            })?;
        if map.contains_key(&path) {
            anyhow::bail!(
                "existing capture model contains multiple bindings with the same resource path ({})",
                path.iter().join("/")
            );
        }
        map.insert(path, binding);
    }
    Ok(map)
}

/// An intermediate representation of a discovered capture binding, along with
/// the resource path and target collection name.
#[derive(Debug)]
pub struct Binding {
    target: models::Collection,
    document_schema: models::Schema,
    collection_key: Vec<String>,
    resource_path: ResourcePath,
    disable: bool,
}

/// Updates the bingings of the given `model`, and returns a tuple of:
/// - Intermediate representations of all the discovered bindings. This list
///   reflects the new state of the capture bindings after the merge.
/// - The set of newly added bindings
/// - The set of removed bindings
pub fn update_capture_bindings(
    capture_name: &str,
    model: &mut models::CaptureDef,
    discovered_bindings: Vec<discovered::Binding>,
    update_only: bool,
    resource_path_pointers: &[doc::Pointer],
) -> anyhow::Result<(Vec<Binding>, Changes, Changes)> {
    assert!(
        !resource_path_pointers.is_empty(),
        "expected resource_path_pointers to be non-empty"
    );
    let capture_prefix = capture_name.rsplit_once("/").unwrap().0;

    let mut existing_bindings_by_path =
        index_fetched_bindings(resource_path_pointers, &model.bindings)?;
    let mut added_resources = BTreeMap::new();
    let mut used_bindings = Vec::with_capacity(discovered_bindings.len());

    let mut discovered_resource_paths = HashSet::new();
    let mut next_bindings = Vec::new();
    for discovered_binding in discovered_bindings {
        let discovered::Binding {
            recommended_name,
            resource_config_json,
            document_schema_json,
            key,
            ..
        } = discovered_binding;
        // Don't use the deprecated `resource_path` on the `proto_flow::...::Binding` struct.
        // Instead extract the resource path from the `resource_config_json` using the
        // `resource_path_pointers` from the `connector_tags` row.
        let discovered_resource: Value =
            serde_json::from_str(&resource_config_json).context("parsing resource config")?;
        let resource_path = resource_path(resource_path_pointers, &discovered_resource).map_err(
            |resource_path_pointer| InvalidResource {
                binding_type: BindingType::Discovered,
                resource_path_pointer,
                resource_json: resource_config_json.clone(),
            },
        )?;
        if !discovered_resource_paths.insert(resource_path.clone()) {
            anyhow::bail!(
                "connector discover response includes multiple bindings with the same resource path ({})",
                resource_path.iter().join("/")
            );
        }

        // Remove matched bindings from the existing map, so we can tell which ones are being removed.
        let existing_binding = existing_bindings_by_path.remove(&resource_path);
        let new_binding = if let Some(existing) = existing_binding {
            existing.clone()
        } else {
            let target = models::Collection::new(format!("{capture_prefix}/{recommended_name}"));
            let disable = update_only || discovered_binding.disable;
            added_resources.insert(
                resource_path.clone(),
                Changed {
                    target: target.clone(),
                    disable,
                },
            );
            models::CaptureBinding {
                target,
                disable,
                resource: models::RawValue::from_value(&discovered_resource),
                backfill: 0,
            }
        };
        let document_schema = models::Schema::new(
            models::RawValue::from_str(&document_schema_json)
                .context("parsing discovered collection schema")?,
        );
        used_bindings.push(Binding {
            target: new_binding.target.clone(),
            document_schema,
            collection_key: key,
            resource_path,
            disable: new_binding.disable,
        });
        next_bindings.push(new_binding);
    }

    // Any original bindings remaining in the index are now unused.
    let removed_resources = existing_bindings_by_path
        .into_iter()
        .map(|(path, binding)| {
            (
                path,
                Changed {
                    target: binding.target.clone(),
                    disable: binding.disable,
                },
            )
        })
        .collect();
    model.bindings = next_bindings;

    Ok((used_bindings, added_resources, removed_resources))
}

pub fn merge_collections(
    used_bindings: Vec<Binding>,
    draft: &mut tables::DraftCollections,
    live: &tables::LiveCollections,
) -> anyhow::Result<Changes> {
    let mut modified_collections = Changes::new();

    for binding in used_bindings {
        let Binding {
            target,
            document_schema,
            collection_key,
            resource_path,
            disable,
        } = binding;

        let draft_target = draft.get_or_insert_with(&target, || {
            if let Some(live_collection) = live.get_by_key(&target) {
                tracing::debug!(
                    ?target,
                    ?resource_path,
                    "adding new draft collection from live specs"
                );
                tables::DraftCollection {
                    collection: live_collection.collection.clone(),
                    scope: tables::synthetic_scope(models::CatalogType::Collection, &target),
                    expect_pub_id: Some(live_collection.last_pub_id),
                    model: Some(live_collection.model.clone()),
                    is_touch: true, // we might negate this later if we modify
                }
            } else {
                tracing::debug!(?target, ?resource_path, "creating new draft collection");
                let model = models::CollectionDef {
                    schema: None,
                    write_schema: None,
                    read_schema: None,
                    key: models::CompositeKey::new(Vec::new()),
                    projections: Default::default(),
                    journals: Default::default(),
                    derive: None,
                    expect_pub_id: None,
                    delete: false,
                };
                tables::DraftCollection {
                    collection: target.clone(),
                    scope: tables::synthetic_scope(models::CatalogType::Collection, &target),
                    expect_pub_id: Some(models::Id::zero()),
                    model: Some(model),
                    is_touch: false, // This is a new collection
                }
            }
        });

        let DraftCollection {
            ref collection,
            ref mut is_touch,
            ref mut model,
            ..
        } = draft_target;

        let Some(draft_model) = model.as_mut() else {
            // TODO: This should arguably be an error
            tracing::warn!(
                %target,
                "skipping merge of discovered target collection that is deleted in the draft"
            );
            continue;
        };

        // If the discover didn't provide a key, don't over-write a user's chosen key.
        if !collection_key.is_empty() {
            let discovered_key = models::CompositeKey::new(
                collection_key
                    .into_iter()
                    .map(models::JsonPointer::new)
                    .collect::<Vec<_>>(),
            );
            if discovered_key != draft_model.key {
                tracing::debug!(
                    %collection,
                    ?discovered_key,
                    model_key = ?draft_model.key,
                    "discovered key change"
                );
                *is_touch = false;
                modified_collections.insert(
                    resource_path.clone(),
                    Changed {
                        target: collection.clone(),
                        disable,
                    },
                );
                draft_model.key = discovered_key;
            }
        }

        let mut modified = false;
        if draft_model.read_schema.is_some() {
            if is_schema_changed(&document_schema, draft_model.write_schema.as_ref()) {
                tracing::debug!(
                    %collection,
                    "discovered writeSchema change"
                );
                modified = true;
                draft_model.write_schema = Some(document_schema);
            }
        } else if uses_inferred_schema(&document_schema) {
            tracing::debug!(
                %collection,
                "discovered new use of inferred schema, initializing readSchema with placeholder"
            );
            // This is either a new collection, or else discovery has just started asking for
            // the inferred schema. In either case, we must initialize the read schema with the
            // inferred schema placeholder.
            modified = true;
            draft_model.read_schema = Some(models::Schema::default_inferred_read_schema());
            draft_model.write_schema = Some(document_schema);
            draft_model.schema = None;
        } else if is_schema_changed(&document_schema, draft_model.schema.as_ref()) {
            tracing::debug!(
                %collection,
                "discovered schema change"
            );
            modified = true;
            draft_model.schema = Some(document_schema);
        }
        if modified {
            *is_touch = false;
            modified_collections.insert(
                resource_path,
                Changed {
                    target: collection.clone(),
                    disable,
                },
            );
        }
    }
    Ok(modified_collections)
}

fn uses_inferred_schema(schema: &models::Schema) -> bool {
    matches!(
        // Does the connector use schema inference?
        schema.to_value().get("x-infer-schema"),
        Some(serde_json::Value::Bool(true))
    )
}

/// Returns whether the discovered schema is different from the current schema.
/// This currently checks whether the schemas are byte-for-byte identical, which
/// means that insignificant serialization differences will be treated as
/// "changed". But it would probably also be correct, and potentially
/// beneficial, to ignore insignificant serialization differences.
fn is_schema_changed(discovered: &models::Schema, current: Option<&models::Schema>) -> bool {
    let Some(current_schema) = current else {
        return true;
    };
    return current_schema != discovered;
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
    use tables::DraftRow;

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

        let out = super::parse_response(response).unwrap();

        insta::assert_json_snapshot!(json!(out));
    }

    fn string_vec(strs: &[&str]) -> Vec<String> {
        strs.into_iter().map(|s| s.to_string()).collect()
    }

    fn ptr_vec(ptrs: &[&str]) -> Vec<doc::Pointer> {
        ptrs.into_iter()
            .map(|s| doc::Pointer::from_str(s))
            .collect()
    }

    #[test]
    fn test_merge_collection() {
        let discovered_bindings = vec![
            // case/1: if there is no fetched collection, one is assembled.
            Binding {
                target: models::Collection::new("case/1"),
                document_schema: models::Schema::new(
                    models::RawValue::from_str(r#"{"const": 42}"#).unwrap(),
                ),
                collection_key: string_vec(&["/foo", "/bar"]),
                resource_path: string_vec(&["1"]),
                disable: false,
            },
            // case/2: expect key and schema are updated, but other fields remain.
            Binding {
                target: models::Collection::new("case/2"),
                document_schema: models::Schema::new(
                    models::RawValue::from_str(r#"{"const": 42}"#).unwrap(),
                ),
                collection_key: string_vec(&["/foo", "/bar"]),
                resource_path: string_vec(&["2"]),
                disable: false,
            },
            // case/3: If discovered key is empty, it doesn't replace the collection key.
            Binding {
                target: models::Collection::new("case/3"),
                document_schema: models::Schema::new(
                    models::RawValue::from_str(r#"{"const": 42}"#).unwrap(),
                ),
                collection_key: Vec::new(),
                resource_path: string_vec(&["3"]),
                disable: false,
            },
            // case/4: If fetched collection has read & write schemas, only the write schema is updated.
            Binding {
                target: models::Collection::new("case/4"),
                document_schema: models::Schema::new(
                    models::RawValue::from_str(r#"{ "const": "write!", "x-infer-schema": true }"#)
                        .unwrap(),
                ),
                collection_key: string_vec(&["/foo", "/bar"]),
                resource_path: string_vec(&["4"]),
                disable: true,
            },
            // case/5: If there is no fetched collection but schema inference is used, an initial read schema is created.
            Binding {
                target: models::Collection::new("case/5"),
                document_schema: models::Schema::new(
                    models::RawValue::from_str(r#"{ "const": "write!", "x-infer-schema": true }"#)
                        .unwrap(),
                ),
                collection_key: string_vec(&["/key"]),
                resource_path: string_vec(&["5"]),
                disable: true,
            },
            // case/6: The fetched collection did not use schema inference, but now does.
            Binding {
                target: models::Collection::new("case/6"),
                document_schema: models::Schema::new(
                    models::RawValue::from_str(r#"{ "const": "write!", "x-infer-schema": true }"#)
                        .unwrap(),
                ),
                collection_key: string_vec(&["/key"]),
                resource_path: string_vec(&["6"]),
                disable: true,
            },
        ];

        let draft_catalog: models::Catalog = serde_json::from_value(json!({
            "collections": {
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
            }
        }))
        .unwrap();
        let mut draft = tables::DraftCatalog::from(draft_catalog);

        let mut live = tables::LiveCatalog::default();
        live.collections.insert(tables::LiveCollection {
            collection: models::Collection::new("case/3"),
            control_id: models::Id::zero(),
            data_plane_id: models::Id::zero(),
            last_pub_id: models::Id::zero(),
            last_build_id: models::Id::zero(),
            model: serde_json::from_value(json!({
                "schema": false,
                "key": ["/drafted-key-should-be-used-instead"],
            }))
            .unwrap(),
            spec: Default::default(),
            dependency_hash: None,
        });
        live.collections.insert(tables::LiveCollection {
            collection: models::Collection::new("case/6"),
            control_id: models::Id::zero(),
            data_plane_id: models::Id::zero(),
            last_pub_id: models::Id::zero(),
            last_build_id: models::Id::zero(),
            model: serde_json::from_value(json!({
                "schema": false,
                "key": ["/old"],
            }))
            .unwrap(),
            spec: Default::default(),
            dependency_hash: None,
        });

        let modified = super::merge_collections(
            discovered_bindings,
            &mut draft.collections,
            &live.collections,
        );

        insta::assert_debug_snapshot!(modified, @r###"
        Ok(
            {
                [
                    "1",
                ]: Changed {
                    target: Collection(
                        "case/1",
                    ),
                    disable: false,
                },
                [
                    "2",
                ]: Changed {
                    target: Collection(
                        "case/2",
                    ),
                    disable: false,
                },
                [
                    "3",
                ]: Changed {
                    target: Collection(
                        "case/3",
                    ),
                    disable: false,
                },
                [
                    "4",
                ]: Changed {
                    target: Collection(
                        "case/4",
                    ),
                    disable: true,
                },
                [
                    "5",
                ]: Changed {
                    target: Collection(
                        "case/5",
                    ),
                    disable: true,
                },
                [
                    "6",
                ]: Changed {
                    target: Collection(
                        "case/6",
                    ),
                    disable: true,
                },
            },
        )
        "###);

        let case6 = draft
            .collections
            .get_by_key(&models::Collection::new("case/6"))
            .unwrap();
        assert!(case6.model().unwrap().schema.is_none());
        assert!(case6.model().unwrap().read_schema.is_some());
        assert!(case6.model().unwrap().write_schema.is_some());
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
        let (discovered_bindings, mut fetched_capture) =
            serde_json::from_value::<(Vec<discovered::Binding>, models::CaptureDef)>(json!([
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
                  "endpoint": { "connector": { "config": { "fetched": 1 }, "image": "an/image" } },
                  // Extra fields which are passed-through.
                  "interval": "34s",
                  "shards": {
                    "maxTxnDuration": "12s"
                  },
                },
            ]))
            .unwrap();
        let pointers = ptr_vec(&["/stream", "/namespace"]);

        let out = super::update_capture_bindings(
            "acmeCo/my-capture",
            &mut fetched_capture,
            discovered_bindings.clone(),
            true,
            &pointers,
        )
        .unwrap();

        // Expect we:
        // * Preserved the modified binding configuration.
        // * Dropped the removed binding.
        // * Updated the endpoint configuration.
        // * Preserved unrelated fields of the capture (shard template and interval).
        // * The resources that specify a namespace are treated separately
        insta::assert_json_snapshot!(fetched_capture);
        insta::assert_debug_snapshot!(out);
    }

    #[test]
    fn test_capture_merge_create() {
        let discovered_bindings  =
            serde_json::from_value::<Vec<discovered::Binding>>(json!([
                    { "recommendedName": "foo", "resourceConfig": { "stream": "foo" }, "key": ["/foo-key"], "documentSchema": { "const": "foo" } },
                    { "recommendedName": "bar", "resourceConfig": { "stream": "bar" }, "key": ["/bar-key"], "documentSchema": { "const": "bar" }, "disable": true },
            ] ))
            .unwrap();

        let mut model = serde_json::from_value(json!({
            "endpoint": { "connector": { "config": { "discovered": 1 }, "image": "new/image" } },
            "bindings": []
        }))
        .unwrap();
        let resource_path_ptrs = ptr_vec(&["/stream"]);
        // assert that the results of the merge are unchanged when using a valid
        // slice of resource path pointers.
        let path_merge_out = super::update_capture_bindings(
            "acmeCo/my/capture",
            &mut model,
            discovered_bindings,
            false,
            &resource_path_ptrs,
        )
        .unwrap();

        insta::assert_debug_snapshot!(path_merge_out);
        insta::assert_json_snapshot!(model, @r###"
        {
          "endpoint": {
            "connector": {
              "image": "new/image",
              "config": {
                "$serde_json::private::RawValue": "{\"discovered\":1}"
              }
            }
          },
          "bindings": [
            {
              "resource": {
                "$serde_json::private::RawValue": "{\"stream\":\"foo\"}"
              },
              "target": "acmeCo/my/foo"
            },
            {
              "resource": {
                "$serde_json::private::RawValue": "{\"stream\":\"bar\"}"
              },
              "disable": true,
              "target": "acmeCo/my/bar"
            }
          ]
        }
        "###);
    }

    #[test]
    fn test_capture_merge_update() {
        // Fixture is an update of an existing capture, which uses a non-suggested collection name.
        // There is also a disabled binding, which is expected to remain disabled after the merge.
        // Additional discovered bindings are filtered.
        let (discovered_bindings, mut fetched_capture) =
            serde_json::from_value::<(Vec<discovered::Binding>, models::CaptureDef)>(json!([
                [
                    { "recommendedName": "fooName", "resourceConfig": { "stream": "foo" }, "documentSchema": { "const": "discovered" } },
                    { "recommendedName": "barName", "resourceConfig": { "stream": "bar" }, "documentSchema": false },
                    { "recommendedName": "disabledName", "resourceConfig": { "stream": "disabled" }, "documentSchema": false },
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

        let resource_path_ptrs = ptr_vec(&["/stream"]);
        let out = super::update_capture_bindings(
            "acmeCo/my-capture",
            &mut fetched_capture,
            discovered_bindings.clone(),
            true,
            &resource_path_ptrs,
        )
        .unwrap();

        // Expect we:
        // * Preserved the modified binding configuration.
        // * Dropped the removed binding.
        // * Updated the endpoint configuration.
        // * Preserved unrelated fields of the capture (shard template and interval).
        insta::assert_debug_snapshot!(out);
        insta::assert_json_snapshot!(fetched_capture);
    }

    #[test]
    fn test_capture_merge_duplicate_bindings() {
        let (discovered_bindings, mut fetched_capture) =
            serde_json::from_value::<(Vec<discovered::Binding>, models::CaptureDef)>(json!([
                [
                    { "recommendedName": "fooName", "resourceConfig": { "stream": "foo" }, "documentSchema": { "const": "discovered" } },
                    { "recommendedName": "fooName2", "resourceConfig": { "stream": "foo" }, "documentSchema": { "const": "discovered2" } },
                ],
                {
                  "bindings": [
                    { "resource": { "stream": "foo", "modified": 1 }, "target": "acmeCo/renamed" },
                    { "resource": { "stream": "foo", "modified": 1 }, "disable": true, "target": "acmeCo/does-not-exist" },
                  ],
                  "endpoint": { "connector": { "config": { "fetched": 1 }, "image": "old/image" } },
                },
            ]))
            .unwrap();

        let resource_path_ptrs = ptr_vec(&["/stream"]);

        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
        let error = super::update_capture_bindings(
            "acmeCo/my-capture",
            &mut fetched_capture,
            discovered_bindings.clone(),
            true,
            &resource_path_ptrs,
        )
        .unwrap_err();

        insta::assert_snapshot!(error);
    }

    #[test]
    fn test_merge_capture_invalid_resource() {
        let (discovered_bindings, mut capture_with_bindings, mut capture_no_bindings) =
            serde_json::from_value::<(Vec<discovered::Binding>, models::CaptureDef, models::CaptureDef)>(json!([
                [
                    { "recommendedName": "foo", "resourceConfig": { "stream": 7 }, "documentSchema": { "const": 1 } },
                ],
                {
                  "bindings": [
                    { "resource": { "stream": {"invalid":"yup"} }, "target": "acmeCo/foo" },
                  ],
                  "endpoint": { "connector": { "config": { "fetched": 1 }, "image": "old/image" } }
                },
                {
                  "bindings": [],
                  "endpoint": { "connector": { "config": { "fetched": 1 }, "image": "old/image" } }
                },
            ]))
            .unwrap();

        let pointers = ptr_vec(&["/namespace", "/stream"]);
        let err = super::update_capture_bindings(
            "acmeCo/naughty-capture",
            &mut capture_no_bindings,
            discovered_bindings.clone(),
            false,
            &pointers,
        )
        .expect_err("should fail because stream is not a string");
        let err = err.downcast::<InvalidResource>().unwrap();
        assert_eq!(BindingType::Discovered, err.binding_type);
        assert_eq!("/stream", err.resource_path_pointer);

        // now assert that an existing invalid binding also results in an error
        let err = super::update_capture_bindings(
            "acmeCo/naughty-capture",
            &mut capture_with_bindings,
            discovered_bindings,
            false,
            &pointers,
        )
        .expect_err("should fail because stream is not a string");
        let err = err.downcast::<InvalidResource>().unwrap();
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
