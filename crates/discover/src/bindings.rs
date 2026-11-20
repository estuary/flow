use super::{Collection, Discovered, Options};
use anyhow::Context;
use itertools::Itertools;
use models::{
    ResourcePath,
    discovers::{Changed, Changes},
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// Merged capture bindings, which `merge_collections` completes.
#[derive(Debug)]
pub struct Merged {
    /// Merged bindings of the capture, in order.
    pub(crate) bindings: Vec<MergedBinding>,
    /// Discovered collections, indexed by MergedBinding::collection.
    pub(crate) collections: Vec<Collection>,
    /// Bindings added to the capture. Added bindings are disabled
    /// if `update_only`, and are still represented here.
    pub added: Changes,
    /// Bindings removed from the capture, because they're no longer
    /// discovered. `disable` reflects whether the binding _was_ disabled.
    pub removed: Changes,
}

#[derive(Debug)]
pub(crate) struct MergedBinding {
    pub target: models::Collection,
    pub resource_path: ResourcePath,
    pub disable: bool,
    pub collection: usize,
}

impl Merged {
    /// Target collections of the merged capture bindings.
    /// `merge_collections` requires their live specs.
    pub fn targets(&self) -> BTreeSet<&models::Collection> {
        self.bindings.iter().map(|b| &b.target).collect()
    }
}

/// Merge a connector's Discovered response into the bindings of `model`,
/// the capture `capture_name`.
///
/// Discovered bindings are matched to existing bindings by resource path.
/// A matched binding is unchanged, and keeps its resource configuration and
/// target. An unmatched binding is added, and targets its recommended name
/// under the capture's prefix. Existing bindings which aren't discovered are
/// removed.
pub fn merge_bindings(
    capture_name: &str,
    model: &mut models::CaptureDef,
    discovered: proto_flow::capture::response::Discovered,
    options: &Options,
    resource_path_pointers: &[json::Pointer],
) -> anyhow::Result<Merged> {
    let Discovered {
        bindings: discovered,
        collections,
    } = super::parse(discovered)?;

    let capture_prefix = capture_name.rsplit_once("/").unwrap().0;

    let mut existing_bindings_by_path =
        index_fetched_bindings(resource_path_pointers, &model.bindings)?;
    let mut added = BTreeMap::new();
    let mut merged_bindings = Vec::with_capacity(discovered.len());
    let mut discovered_resource_paths = HashSet::new();
    let mut next_bindings = Vec::with_capacity(discovered.len());

    for binding in discovered {
        let super::Binding {
            resource_config_json,
            resource_path: discovered_path,
            disable,
            collection,
        } = binding;
        let recommended_name = &collections[collection].recommended_name;

        // A discovered path takes precedence. The connector's pointers (if
        // any) are a fallback for connectors which don't return one.
        let resource_path = if !discovered_path.is_empty() {
            discovered_path
        } else if resource_path_pointers.is_empty() {
            anyhow::bail!(
                "the connector's Discovered binding {recommended_name} has no `resourcePath`, \
                 and the connector declares no resource path pointers"
            );
        } else {
            resource_path(resource_path_pointers, &resource_config_json).with_context(|| {
                format!("extracting resource path from discovered binding {recommended_name}")
            })?
        };
        if !discovered_resource_paths.insert(resource_path.clone()) {
            anyhow::bail!(
                "connector discover response includes multiple bindings with the same resource path ({})",
                resource_path.iter().join("/")
            );
        }

        // Remove matched bindings from the existing map, so we can tell which ones are being removed.
        let next_binding = if let Some(existing) = existing_bindings_by_path.remove(&resource_path)
        {
            existing.clone()
        } else {
            let target = models::Collection::new(format!("{capture_prefix}/{recommended_name}"));
            let disable = options.update_only || disable;
            added.insert(
                resource_path.clone(),
                Changed {
                    target: target.clone(),
                    disable,
                    reason: None,
                },
            );
            // Record the path of a discovered binding, as validation would,
            // so it may be matched against a subsequent discover.
            let resource = validation::store_resource_meta(
                &serde_json::from_slice::<models::RawValue>(&resource_config_json)?,
                &resource_path,
            );
            models::CaptureBinding {
                target,
                disable,
                resource,
                backfill: 0,
            }
        };
        merged_bindings.push(MergedBinding {
            target: next_binding.target.clone(),
            resource_path,
            disable: next_binding.disable,
            collection,
        });
        next_bindings.push(next_binding);
    }

    // Any original bindings remaining in the index are now unused.
    let removed = existing_bindings_by_path
        .into_iter()
        .map(|(path, binding)| {
            (
                path,
                Changed {
                    target: binding.target.clone(),
                    disable: binding.disable,
                    reason: None,
                },
            )
        })
        .collect();
    model.bindings = next_bindings;

    Ok(Merged {
        bindings: merged_bindings,
        collections,
        added,
        removed,
    })
}

/// Determine the resource path of a resource configuration. An embedded
/// `/_meta/path` takes precedence. Otherwise the path is extracted using
/// `resource_path_pointers`. Pointers are the fallback for connectors which
/// don't return resource paths, and for bindings which predate embedded paths.
///
/// Each pointed-to location must be a string, null, or undefined. Null and
/// undefined locations are not included in the path, and are equivalent.
/// Other values are an error.
fn resource_path(
    resource_path_pointers: &[json::Pointer],
    resource_config_json: &[u8],
) -> anyhow::Result<ResourcePath> {
    let mut path = validation::load_resource_meta_path(resource_config_json);
    if !path.is_empty() {
        return Ok(path);
    }
    let resource: serde_json::Value =
        serde_json::from_slice(resource_config_json).context("parsing resource config JSON")?;
    for pointer in resource_path_pointers {
        match pointer.query(&resource) {
            None | Some(serde_json::Value::Null) => {
                continue;
            }
            Some(serde_json::Value::String(s)) => path.push(s.clone()),
            Some(_) => anyhow::bail!(
                "resource config includes non-string value at resource path pointer location '{pointer}'"
            ),
        }
    }
    Ok(path)
}

fn index_fetched_bindings<'a>(
    resource_path_pointers: &'_ [json::Pointer],
    bindings: &'a [models::CaptureBinding],
) -> anyhow::Result<HashMap<ResourcePath, &'a models::CaptureBinding>> {
    let mut map = HashMap::new();
    for (idx, binding) in bindings.iter().enumerate() {
        let target = &binding.target;

        let path = resource_path(resource_path_pointers, binding.resource.get().as_bytes())
            .with_context(|| {
                format!("extracting resource path from existing binding of {target} (index {idx})")
            })?;

        // Without pointers, an empty path means the binding has no `/_meta/path`.
        if path.is_empty() && resource_path_pointers.is_empty() {
            anyhow::bail!(
                "existing binding of {target} (index {idx}) has no resource path at `/_meta/path`, \
                 and the connector declares no resource path pointers"
            );
        }

        // Validation rejects duplicated `/_meta/path`s, but not duplicated
        // paths of pointers. The index requires unique paths: a binding
        // displaced from it would be neither matched nor removed.
        if map.insert(path.clone(), binding).is_some() {
            anyhow::bail!(
                "existing capture model contains multiple bindings with the same resource path ({})",
                path.iter().join("/")
            );
        }
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto_flow::capture::response;
    use serde_json::json;

    fn ptr_vec(ptrs: &[&str]) -> Vec<json::Pointer> {
        ptrs.into_iter()
            .map(|s| json::Pointer::from_str(s))
            .collect()
    }

    fn update_only() -> Options {
        Options {
            update_only: true,
            reset_on_key_change: false,
        }
    }

    fn discovered(bindings: serde_json::Value) -> response::Discovered {
        serde_json::from_value(json!({ "bindings": bindings })).unwrap()
    }

    #[test]
    fn test_capture_merge_resource_paths_update() {
        // This is meant to test our merge behavior in the presence of additional fields in the
        // `resource` that are not part of the resource path.
        // Fixture is an update of an existing capture, which uses a non-suggested collection name.
        // There is also a disabled binding, which is expected to remain disabled after the merge.
        // Additional discovered bindings are filtered.
        // Note that fields apart from stream and namespace are modified to demonstrate them being
        // ignored for the purposes of matching up discovered and live bindings (since it's done
        // by resource_path_pointers now)
        let (discovered_bindings, mut fetched_capture) =
            serde_json::from_value::<(serde_json::Value, models::CaptureDef)>(json!([
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

        let out = merge_bindings(
            "acmeCo/my-capture",
            &mut fetched_capture,
            discovered(discovered_bindings),
            &update_only(),
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
    fn test_capture_merge_without_pointers() {
        // A custom connector declares no resource path pointers. Existing
        // bindings are matched by their `/_meta/path`, and discovered ones by
        // their returned `resourcePath`. A binding which the discover adds
        // stores its path at `/_meta/path`, even when disabled by `update_only`.
        let discovered_bindings = json!([
            { "recommendedName": "greetings", "resourceConfig": { "name": "greetings", "interval": "PT1M" }, "documentSchema": { "const": "discovered" }, "resourcePath": ["greetings"] },
            { "recommendedName": "farewells", "resourceConfig": { "name": "farewells" }, "documentSchema": false, "resourcePath": ["farewells"] },
        ]);
        let mut fetched_capture = serde_json::from_value::<models::CaptureDef>(json!({
            "bindings": [
                { "resource": { "name": "greetings", "interval": "PT30S", "_meta": { "path": ["greetings"] } }, "target": "acmeCo/renamed-greetings" },
                { "resource": { "name": "removed", "_meta": { "path": ["removed"] } }, "target": "acmeCo/removed" },
            ],
            "endpoint": { "connector": { "image": "source/hello:v1", "config": { "greeting": "Howdy" } } },
        }))
        .unwrap();

        let out = merge_bindings(
            "acmeCo/source-hello",
            &mut fetched_capture,
            discovered(discovered_bindings.clone()),
            &update_only(),
            &[],
        )
        .unwrap();

        // The existing binding keeps its configuration and target, a new
        // binding is added with its path, and the removed binding is dropped.
        insta::assert_json_snapshot!(serde_json::to_value(&fetched_capture.bindings).unwrap(), @r#"
        [
          {
            "disable": true,
            "resource": {
              "_meta": {
                "path": [
                  "farewells"
                ]
              },
              "name": "farewells"
            },
            "target": "acmeCo/farewells"
          },
          {
            "resource": {
              "_meta": {
                "path": [
                  "greetings"
                ]
              },
              "interval": "PT30S",
              "name": "greetings"
            },
            "target": "acmeCo/renamed-greetings"
          }
        ]
        "#);
        insta::assert_debug_snapshot!((&out.added, &out.removed), @r#"
        (
            {
                [
                    "farewells",
                ]: Changed {
                    target: Collection(
                        "acmeCo/farewells",
                    ),
                    disable: true,
                    reason: None,
                },
            },
            {
                [
                    "removed",
                ]: Changed {
                    target: Collection(
                        "acmeCo/removed",
                    ),
                    disable: false,
                    reason: None,
                },
            },
        )
        "#);

        // Discovering again against the updated draft matches every binding,
        // and changes nothing.
        let before = fetched_capture.clone();
        let out = merge_bindings(
            "acmeCo/source-hello",
            &mut fetched_capture,
            discovered(discovered_bindings),
            &update_only(),
            &[],
        )
        .unwrap();
        assert_eq!(before, fetched_capture);
        assert!(out.added.is_empty() && out.removed.is_empty(), "{out:?}");
    }

    #[test]
    fn test_resource_path_of_unresolved_pointers_is_empty() {
        // Pointers which resolve to nothing yield an empty path, as they always
        // have, rather than an error: it's a valid path of a sole binding.
        let pointers = [json::Pointer::from_str("/stream")];
        assert_eq!(
            resource_path(&pointers, br#"{"path": "/hook"}"#).unwrap(),
            Vec::<String>::new(),
        );
    }

    #[test]
    fn test_capture_merge_create() {
        let discovered_bindings = json!([
            { "recommendedName": "foo", "resourceConfig": { "stream": "foo" }, "key": ["/foo-key"], "documentSchema": { "const": "foo" } },
            { "recommendedName": "bar", "resourceConfig": { "stream": "bar" }, "key": ["/bar-key"], "documentSchema": { "const": "bar" }, "disable": true },
        ]);

        let mut model = serde_json::from_value(json!({
            "endpoint": { "connector": { "config": { "discovered": 1 }, "image": "new/image" } },
            "bindings": []
        }))
        .unwrap();
        let resource_path_ptrs = ptr_vec(&["/stream"]);
        // assert that the results of the merge are unchanged when using a valid
        // slice of resource path pointers.
        let path_merge_out = merge_bindings(
            "acmeCo/my/capture",
            &mut model,
            discovered(discovered_bindings),
            &Options::default(),
            &resource_path_ptrs,
        )
        .unwrap();

        insta::assert_debug_snapshot!(path_merge_out);
        insta::assert_json_snapshot!(model, @r#"
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
                "$serde_json::private::RawValue": "{\"_meta\":{\"path\":[\"bar\"]},\"stream\":\"bar\"}"
              },
              "disable": true,
              "target": "acmeCo/my/bar"
            },
            {
              "resource": {
                "$serde_json::private::RawValue": "{\"_meta\":{\"path\":[\"foo\"]},\"stream\":\"foo\"}"
              },
              "target": "acmeCo/my/foo"
            }
          ]
        }
        "#);
    }

    #[test]
    fn test_capture_merge_update() {
        // Fixture is an update of an existing capture, which uses a non-suggested collection name.
        // There is also a disabled binding, which is expected to remain disabled after the merge.
        // Additional discovered bindings are filtered.
        let (discovered_bindings, mut fetched_capture) =
            serde_json::from_value::<(serde_json::Value, models::CaptureDef)>(json!([
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
        let out = merge_bindings(
            "acmeCo/my-capture",
            &mut fetched_capture,
            discovered(discovered_bindings),
            &update_only(),
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
            serde_json::from_value::<(serde_json::Value, models::CaptureDef)>(json!([
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

        let error = merge_bindings(
            "acmeCo/my-capture",
            &mut fetched_capture,
            discovered(discovered_bindings),
            &update_only(),
            &resource_path_ptrs,
        )
        .unwrap_err();

        insta::assert_snapshot!(error);
    }

    #[test]
    fn test_merge_capture_invalid_resource() {
        let (discovered_bindings, capture_with_bindings, capture_no_bindings) =
            serde_json::from_value::<(serde_json::Value, models::CaptureDef, models::CaptureDef)>(json!([
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
        let errors = [
            // A discovered binding's path is not a string.
            (&capture_no_bindings, &pointers[..]),
            // An existing binding's path is not a string.
            (&capture_with_bindings, &pointers[..]),
            // Without pointers, a discovered binding has no `resourcePath`.
            (&capture_no_bindings, &[][..]),
            // Without pointers, an existing binding has no `/_meta/path`.
            (&capture_with_bindings, &[][..]),
        ]
        .map(|(capture, pointers)| {
            let err = merge_bindings(
                "acmeCo/naughty-capture",
                &mut capture.clone(),
                discovered(discovered_bindings.clone()),
                &Options::default(),
                pointers,
            )
            .unwrap_err();
            format!("{err:#}")
        });
        insta::assert_debug_snapshot!(errors, @r#"
        [
            "extracting resource path from discovered binding foo: resource config includes non-string value at resource path pointer location '/stream'",
            "extracting resource path from existing binding of acmeCo/foo (index 0): resource config includes non-string value at resource path pointer location '/stream'",
            "the connector's Discovered binding foo has no `resourcePath`, and the connector declares no resource path pointers",
            "existing binding of acmeCo/foo (index 0) has no resource path at `/_meta/path`, and the connector declares no resource path pointers",
        ]
        "#);
    }
}
