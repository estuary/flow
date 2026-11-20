use super::{Collection, Merged, Options, bindings::MergedBinding};
use anyhow::Context;
use doc::shape::{X_INFER_SCHEMA, X_INITIAL_READ_SCHEMA};
use itertools::Itertools;
use models::discovers::{Changed, Changes};
use std::collections::BTreeMap;

/// Merge discovered collections into the target collections of `merged`
/// capture bindings, returning the bindings whose collections were modified.
///
/// Each target is merged once. Its enabled bindings determine the merge,
/// or all of its bindings if none are enabled. Of these, the last binding
/// provides the schema. The key changes only if each discovered the same key.
///
/// The merge applies to the target's model in `draft` if present,
/// or else to a copy of its `live` model, or else to a new model.
/// A collection already in `draft` remains in it, modified or not.
/// Any other collection is added to `draft` only if the merge modifies it.
/// A collection deleted in `draft` is replaced by a new model,
/// which is reset if the collection is live.
///
/// Each modified live collection is reported once, under the first of these bindings.
pub fn merge_collections(
    merged: &Merged,
    draft: &mut tables::DraftCollections,
    live: &tables::LiveCollections,
    options: &Options,
) -> anyhow::Result<Changes> {
    let mut targets: BTreeMap<&models::Collection, Vec<&MergedBinding>> = BTreeMap::new();
    for binding in &merged.bindings {
        targets.entry(&binding.target).or_default().push(binding);
    }

    let mut modified_collections = Changes::new();
    let mut inserts = Vec::new();

    for (target, mut bindings) in targets {
        if bindings.iter().any(|binding| !binding.disable) {
            bindings.retain(|binding| !binding.disable);
        }
        let bindings = bindings;
        let discovered = &merged.collections[bindings.last().unwrap().collection];
        let agreed = agreed_key(&bindings, &merged.collections);
        let live_collection = live.get_by_key(target);

        let merge = |model: &mut models::CollectionDef| {
            let (modified, mut reasons) = merge_collection(
                target,
                model,
                &discovered.document_schema,
                agreed,
                options.reset_on_key_change,
            )?;
            if modified && agreed.is_none() {
                reasons.extend(pending_key_change(
                    &bindings,
                    &merged.collections,
                    &model.key,
                ));
            }
            anyhow::Ok((modified, reasons))
        };

        let (modified, reasons) = if let Some(drafted) = draft.get_mut_by_key(target) {
            let (modified, reasons) = if let Some(model) = drafted.model.as_mut() {
                merge(model)?
            } else {
                // Discovering a collection deleted in the draft replaces it,
                // as if the deletion were published and then discovered.
                // A reset is how one publication replaces a live collection.
                let mut model = new_model(&discovered.key);
                model.reset = live_collection.is_some();
                let (_modified, mut reasons) = merge(&mut model)?;

                reasons.insert(
                    0,
                    if model.reset {
                        "replaced collection deleted in the draft, and reset it"
                    } else {
                        "replaced collection deleted in the draft"
                    }
                    .to_string(),
                );
                drafted.model = Some(model);
                (true, reasons)
            };
            if modified {
                drafted.is_touch = false;
            }
            (modified, reasons)
        } else {
            let (expect_pub_id, mut model) = match live_collection {
                Some(live) => (live.last_pub_id, live.model.clone()),
                None => (models::Id::zero(), new_model(&discovered.key)),
            };
            let (modified, reasons) = merge(&mut model)?;

            if modified {
                inserts.push(tables::DraftCollection {
                    collection: target.clone(),
                    scope: tables::synthetic_scope(models::CatalogType::Collection, target),
                    expect_pub_id: Some(expect_pub_id),
                    data_plane_id: models::Id::zero(),
                    model: Some(model),
                    is_touch: false,
                });
            }
            (modified, reasons)
        };

        if modified && live_collection.is_some() {
            let representative = bindings[0];
            modified_collections.insert(
                representative.resource_path.clone(),
                Changed {
                    target: target.clone(),
                    disable: representative.disable,
                    reason: (!reasons.is_empty()).then(|| reasons.join("; ")),
                },
            );
        }
    }
    draft.extend(inserts.into_iter());

    Ok(modified_collections)
}

/// The key which each of `bindings` discovered, and whether it's a fallback
/// key. It's a fallback only if each binding flags it as one.
fn agreed_key<'c>(
    bindings: &[&MergedBinding],
    collections: &'c [Collection],
) -> Option<(&'c models::CompositeKey, bool)> {
    let mut discovered = bindings
        .iter()
        .map(|binding| &collections[binding.collection]);
    let first = discovered.next()?;
    let mut is_fallback_key = first.is_fallback_key;

    for collection in discovered {
        if collection.key != first.key {
            return None;
        }
        is_fallback_key &= collection.is_fallback_key;
    }
    Some((&first.key, is_fallback_key))
}

/// Describe a key change which waits on `bindings` to agree, if any binding
/// discovered a key other than the collection's `current_key`.
fn pending_key_change(
    bindings: &[&MergedBinding],
    collections: &[Collection],
    current_key: &models::CompositeKey,
) -> Option<String> {
    // Group bindings by their discovered key.
    let mut by_key: BTreeMap<&models::CompositeKey, Vec<String>> = BTreeMap::new();
    for binding in bindings {
        by_key
            .entry(&collections[binding.collection].key)
            .or_default()
            .push(binding.resource_path.iter().join("/"));
    }
    if !by_key
        .iter()
        .any(|(key, _)| !key.is_empty() && *key != current_key)
    {
        return None;
    }
    let groups = by_key
        .into_iter()
        .map(|(key, paths)| {
            format!(
                "{:?} ({})",
                key.iter().map(|ptr| ptr.as_str()).collect::<Vec<_>>(),
                paths.join(", ")
            )
        })
        .join(" vs ");

    Some(format!("key change waits until bindings agree: {groups}"))
}

fn new_model(key: &models::CompositeKey) -> models::CollectionDef {
    models::CollectionDef {
        schema: None,
        write_schema: None,
        read_schema: None,
        key: key.clone(),
        projections: Default::default(),
        journals: Default::default(),
        derive: None,
        expect_pub_id: None,
        delete: false,
        reset: false,
    }
}

/// Merge a discovered `connector_schema` and `agreed_key` into the `model`
/// of collection `name`. Returns whether `model` was modified, and reasons
/// for a modification of its key.
fn merge_collection(
    name: &models::Collection,
    model: &mut models::CollectionDef,
    connector_schema: &models::Schema,
    agreed_key: Option<(&models::CompositeKey, bool)>,
    reset_on_key_change: bool,
) -> anyhow::Result<(bool, Vec<String>)> {
    let mut modified = false;
    let mut reasons = Vec::new();

    // Non-fallback keys always replace the model key. Fallback keys
    // only replace it when the existing key is provably invalid in
    // the discovered schema and the fallback key itself resolves,
    // to avoid replacing one broken key with another.
    let update_key = match agreed_key {
        Some((key, is_fallback_key)) if !key.is_empty() && *key != model.key => {
            if !is_fallback_key {
                Some(key)
            } else if !key_resolves_in_schema(&model.key, connector_schema)
                && key_resolves_in_schema(key, connector_schema)
            {
                Some(key)
            } else {
                None
            }
        }
        _ => None,
    };

    if let Some(key) = update_key {
        let is_fallback_key = agreed_key.unwrap().1;
        tracing::debug!(%name, ?key, %is_fallback_key, model_key = ?model.key, "discovered key change");

        let old_ptrs: Vec<&str> = model.key.iter().map(|p| p.as_str()).collect();
        let new_ptrs: Vec<&str> = key.iter().map(|p| p.as_str()).collect();
        let mut reason = if is_fallback_key {
            format!(
                "replaced collection key {old_ptrs:?} with fallback {new_ptrs:?} because the existing key no longer exists in the discovered schema",
            )
        } else {
            format!("changed collection key from {old_ptrs:?} to {new_ptrs:?}")
        };
        if reset_on_key_change {
            model.reset = true;
            reason.push_str(", and reset the collection");
        }
        reasons.push(reason);
        modified = true;
        model.key = key.clone();
    }

    // Determine the status of the collection's schema vs write/read schema.
    let effective_write_schema = if model.read_schema.is_some() {
        // Collection is using separate read & write schemas.
        // The read schema has been initialized and we do not modify it.
        &mut model.write_schema
    } else if let Some(initial_read_schema) = initializes_read_schema(connector_schema) {
        // Migrate singular `schema` into separate read & write schemas.
        model.write_schema = model.schema.take();
        let mut read_schema =
            models::Schema::new(models::RawValue::from_value(&initial_read_schema));
        // In practice, this check will probably always return true. But
        // having it here ensures that we won't mistakenly add the
        // placeholder inferred schema in case a connector provides an
        // explicit read schema that doesn't reference the inferred schema.
        if read_schema.references_inferred_schema() {
            read_schema = read_schema
                .add_defs(&[models::schemas::AddDef {
                    id: models::Schema::REF_INFERRED_SCHEMA_URL,
                    schema: models::Schema::inferred_schema_placeholder(),
                    overwrite: true,
                }])
                .with_context(|| {
                    format!("adding inferred schema placeholder to collection: '{name}'")
                })?;
        }
        model.read_schema = Some(read_schema);
        modified = true;
        &mut model.write_schema
    } else {
        // Collection uses a single schema for read & write.
        &mut model.schema
    };

    // Does the connector's schema update the effective write schema?
    if let Some(updated) =
        update_connector_schema(name, effective_write_schema.as_ref(), connector_schema)?
    {
        tracing::debug!(%name, "discovered schema change");
        *effective_write_schema = Some(updated);
        modified = true;
    }

    Ok((modified, reasons))
}

/// Returns whether every pointer in `key` resolves (Must or May) in `schema`.
/// Returns false on schema parse errors (conservative: don't change the key).
fn key_resolves_in_schema(key: &models::CompositeKey, schema: &models::Schema) -> bool {
    let Ok(built) = doc::validation::build_bundle(schema.get().as_bytes()) else {
        return false;
    };
    let Ok(validator) = doc::Validator::new(built) else {
        return false;
    };
    let shape = doc::Shape::infer(validator.schema(), validator.schema_index());

    key.iter().all(|ptr| {
        let (_, exists) = shape.locate(&json::Pointer::from_str(ptr.as_str()));
        matches!(
            exists,
            doc::shape::location::Exists::Must | doc::shape::location::Exists::May
        )
    })
}

fn initializes_read_schema(schema: &models::Schema) -> Option<serde_json::Value> {
    // Does the connector provide an explicit initial read schema?
    if let Some(initial) = schema.to_value().get(X_INITIAL_READ_SCHEMA) {
        return Some(initial.clone());
    }

    // Does the connector use the legacy schema inference annotation?
    // If so, initialize with a default read schema.
    if let Some(serde_json::Value::Bool(true)) = schema.to_value().get(X_INFER_SCHEMA) {
        return Some(serde_json::json!({
            "allOf": [
                {"$ref": models::Schema::REF_RELAXED_WRITE_SCHEMA_URL},
                {"$ref": models::Schema::REF_INFERRED_SCHEMA_URL},
            ],
        }));
    }

    None
}

fn has_connector_schema_def(schema: &models::Schema) -> anyhow::Result<bool> {
    type Skim = BTreeMap<String, models::RawValue>;

    let top =
        serde_json::from_str::<Skim>(schema.get()).context("failed to parse schema as object")?;
    let Some(defs_raw) = top.get("$defs") else {
        return Ok(false);
    };
    let defs = serde_json::from_str::<Skim>(defs_raw.get())
        .context("failed to parse schema $defs as object")?;
    Ok(defs.contains_key(models::Schema::REF_CONNECTOR_SCHEMA_URL))
}

fn update_connector_schema(
    collection: &models::Collection,
    current: Option<&models::Schema>,
    connector_schema: &models::Schema,
) -> anyhow::Result<Option<models::Schema>> {
    let base = match current {
        // Collection schema is being initialized.
        None => models::Schema::new(models::RawValue::from_value(&serde_json::json!({
            "$ref": models::Schema::REF_CONNECTOR_SCHEMA_URL
        }))),
        // Existing collection schema that already has a connector schema in $defs.
        Some(s) if has_connector_schema_def(s)? => s.clone(),
        // Existing collection schema that doesn't have a connector schema in $defs.
        // Error explicitly to prevent clobbering a user's manually crafted schema.
        // While injecting into $defs is harmless, it wouldn't be harmless if we
        // initialized a readSchema under the connector's direction.
        Some(_) => anyhow::bail!(
            "collection {collection} has a schema that is not managed by auto-discover \
            (it has no $defs entry for '{url}'). To opt in, add \
            {{\"$defs\": {{\"{url}\": {{}}}}}} to the collection schema",
            url = models::Schema::REF_CONNECTOR_SCHEMA_URL,
        ),
    };

    let updated = base.add_defs(&[models::schemas::AddDef {
        id: models::Schema::REF_CONNECTOR_SCHEMA_URL,
        schema: connector_schema,
        overwrite: true,
    }])?;

    // Return the updated schema only if it's actually changed.
    if current != Some(&updated) {
        Ok(Some(updated))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Build a Merged where each binding has its own discovered collection.
    fn merged(cases: &[(&str, &str, &[&str], bool, &str, bool)]) -> Merged {
        let mut out = Merged {
            bindings: Vec::new(),
            collections: Vec::new(),
            added: Default::default(),
            removed: Default::default(),
        };
        for (target, schema, key, is_fallback_key, path, disable) in cases {
            out.bindings.push(MergedBinding {
                target: models::Collection::new(*target),
                resource_path: vec![path.to_string()],
                disable: *disable,
                collection: out.collections.len(),
            });
            out.collections.push(Collection {
                recommended_name: target.to_string(),
                document_schema: models::Schema::new(models::RawValue::from_str(schema).unwrap()),
                key: models::CompositeKey::new(
                    key.iter()
                        .map(|k| models::JsonPointer::new(*k))
                        .collect::<Vec<_>>(),
                ),
                is_fallback_key: *is_fallback_key,
            });
        }
        out
    }

    fn live_collection(name: &str, model: serde_json::Value) -> tables::LiveCollection {
        tables::LiveCollection {
            collection: models::Collection::new(name),
            control_id: models::Id::zero(),
            data_plane_id: models::Id::zero(),
            last_pub_id: models::Id::new([0, 0, 0, 0, 0, 0, 0, 7]),
            last_build_id: models::Id::zero(),
            model: serde_json::from_value(model).unwrap(),
            spec: Default::default(),
            dependency_hash: None,
        }
    }

    const RESET: Options = Options {
        update_only: false,
        reset_on_key_change: true,
    };

    #[test]
    fn test_merge_collection() {
        let merged = merged(&[
            // case/1: if there is no fetched collection, one is assembled.
            (
                "case/1",
                r#"{"const": 42}"#,
                &["/foo", "/bar"],
                false,
                "1",
                false,
            ),
            // case/2: expect key and schema are updated, but other fields remain.
            (
                "case/2",
                r#"{"const": 42}"#,
                &["/foo", "/bar"],
                false,
                "2",
                false,
            ),
            // case/3: If discovered key is empty, it doesn't replace the collection key.
            ("case/3", r#"{"const": 42}"#, &[], false, "3", false),
            // case/4: If fetched collection has read & write schemas, only the write schema is updated.
            (
                "case/4",
                r#"{ "const": "write!", "x-infer-schema": true }"#,
                &["/foo", "/bar"],
                false,
                "4",
                true,
            ),
            // case/5: If there is no fetched collection but schema inference is used, an initial read schema is created.
            (
                "case/5",
                r#"{ "const": "write!", "x-infer-schema": true }"#,
                &["/key"],
                false,
                "5",
                true,
            ),
            // case/6: The fetched collection did not use schema inference, but now does.
            (
                "case/6",
                r#"{ "const": "write!", "x-infer-schema": true }"#,
                &["/key"],
                false,
                "6",
                true,
            ),
            // case/7: If discovered key is a fallback, it doesn't replace the collection key.
            (
                "case/7",
                r#"{"const": 42}"#,
                &["/fallback", "/key"],
                true,
                "7",
                false,
            ),
            // case/8: If there is no fetched collection and x-initial-read-schema is provided, it should be used for readSchema.
            (
                "case/8",
                r#"{ "const": "write!", "x-initial-read-schema": {"type": "object", "properties": {"id": {"type": "string"}}} }"#,
                &["/id"],
                false,
                "8",
                false,
            ),
            // case/9: Fallback key replaces existing key when old key is no
            // longer present in the discovered schema, as when a user removes
            // the primary key column from a postgres table.
            (
                "case/9",
                r#"{"type": "object", "properties": {"foo_id": {"type": "integer"}, "bar_id": {"type": "integer"}}, "required": ["foo_id", "bar_id"]}"#,
                &["/foo_id", "/bar_id"],
                true,
                "9",
                false,
            ),
            // case/10: Fallback key does NOT replace existing key when old key
            // fields still exist in the discovered schema (column exists but
            // is no longer the primary key).
            (
                "case/10",
                r#"{"type": "object", "properties": {"id": {"type": "integer"}, "foo_id": {"type": "integer"}, "bar_id": {"type": "integer"}}, "required": ["id", "foo_id", "bar_id"]}"#,
                &["/foo_id", "/bar_id"],
                true,
                "10",
                false,
            ),
        ]);

        let old_schema = json!({
            "$defs": {
                "flow://connector-schema": {"const": "old"}
            },
            "$ref": "flow://connector-schema",
        });
        let draft_catalog: models::Catalog = serde_json::from_value(json!({
            "collections": {
                "case/2": {
                    "schema": old_schema,
                    "key": ["/old"],
                    "projections": {"field": "/ptr"},
                    "derive": {
                        "using": {"sqlite": {}},
                        "transforms": [],
                    },
                    "journals": {"fragments": {"length": 1234}},
                },
                "case/3": {
                    "schema": old_schema,
                    "key": ["/one", "/two"],
                },
                "case/4": {
                    "writeSchema": old_schema,
                    "readSchema": {"const": "read!"},
                    "key": ["/foo", "/bar"],
                },
                "case/7": {
                    "schema": old_schema,
                    "key": ["/chosen", "/key"],
                },
                "case/9": {
                    "schema": old_schema,
                    "key": ["/id"],
                },
                "case/10": {
                    "schema": old_schema,
                    "key": ["/id"],
                },
            }
        }))
        .unwrap();
        let mut draft = tables::DraftCatalog::from(draft_catalog);

        let mut live = tables::LiveCatalog::default();
        live.collections.insert(live_collection(
            "case/3",
            json!({"schema": old_schema, "key": ["/drafted-key-should-be-used-instead"]}),
        ));
        live.collections.insert(live_collection(
            "case/6",
            json!({"schema": old_schema, "key": ["/key"]}),
        ));
        live.collections.insert(live_collection(
            "case/9",
            json!({"schema": old_schema, "key": ["/id"]}),
        ));
        live.collections.insert(live_collection(
            "case/10",
            json!({"schema": old_schema, "key": ["/id"]}),
        ));

        let modified =
            merge_collections(&merged, &mut draft.collections, &live.collections, &RESET);

        insta::assert_debug_snapshot!(draft.collections);
        insta::assert_debug_snapshot!(modified);
    }

    #[test]
    fn test_only_modified_collections_are_drafted() {
        let current = json!({
            "$defs": {"flow://connector-schema": {"$id": "flow://connector-schema", "const": "current"}},
            "$ref": "flow://connector-schema",
        });
        let merged = merged(&[
            // Unchanged from live, and not drafted.
            (
                "acmeCo/unchanged",
                r#"{"const": "current"}"#,
                &["/id"],
                false,
                "unchanged",
                false,
            ),
            // Changed from live, and drafted.
            (
                "acmeCo/changed",
                r#"{"const": "next"}"#,
                &["/id"],
                false,
                "changed",
                false,
            ),
            // Already drafted, and unchanged. It remains drafted with its edits,
            // and is not marked as a touch.
            (
                "acmeCo/edited",
                r#"{"const": "current"}"#,
                &["/id"],
                false,
                "edited",
                false,
            ),
        ]);

        let mut draft = tables::DraftCatalog::from(
            serde_json::from_value::<models::Catalog>(json!({
                "collections": {
                    "acmeCo/edited": {
                        "schema": current,
                        "key": ["/id"],
                        "projections": {"user": "/edit"},
                    },
                }
            }))
            .unwrap(),
        );
        let mut live = tables::LiveCatalog::default();
        for name in ["acmeCo/unchanged", "acmeCo/changed", "acmeCo/edited"] {
            live.collections.insert(live_collection(
                name,
                json!({"schema": current, "key": ["/id"]}),
            ));
        }

        let modified =
            merge_collections(&merged, &mut draft.collections, &live.collections, &RESET).unwrap();

        insta::assert_debug_snapshot!(
            draft.collections.iter().map(|r| (&r.collection, r.is_touch, r.expect_pub_id)).collect::<Vec<_>>(),
            @r#"
        [
            (
                Collection(
                    "acmeCo/changed",
                ),
                false,
                Some(
                    0000000000000007,
                ),
            ),
            (
                Collection(
                    "acmeCo/edited",
                ),
                false,
                None,
            ),
        ]
        "#
        );
        insta::assert_debug_snapshot!(modified.keys().collect::<Vec<_>>(), @r#"
        [
            [
                "changed",
            ],
        ]
        "#);
    }

    /// Summarize drafted collections as (name, key, reset, effective write schema).
    fn drafted(draft: &tables::DraftCollections) -> Vec<(String, Vec<String>, bool, String)> {
        draft
            .iter()
            .map(|row| {
                let model = row.model.as_ref().unwrap();
                let schema = model.write_schema.as_ref().or(model.schema.as_ref());
                (
                    row.collection.to_string(),
                    model.key.iter().map(|p| p.to_string()).collect(),
                    model.reset,
                    schema.map(|s| s.get().to_string()).unwrap_or_default(),
                )
            })
            .collect()
    }

    #[test]
    fn test_fan_in_provider_and_key_consensus() {
        let schema = |c: &str| {
            format!(
                r#"{{"type": "object", "properties": {{"id": {{"type": "integer"}}, "next": {{"type": "integer"}}}}, "const": "{c}"}}"#
            )
        };
        let (a, b, next, x1, x2) = (
            schema("a"),
            schema("b"),
            schema("next"),
            schema("x1"),
            schema("x2"),
        );
        let merged = merged(&[
            // A new collection fed by two bindings. It's merged and drafted once.
            ("acmeCo/new", &a, &["/id"], false, "new-1", false),
            ("acmeCo/new", &a, &["/id"], false, "new-2", false),
            // A disabled binding is passed over, though it sorts last.
            // Enabled `a` provides the schema, and the key is unchanged.
            ("acmeCo/provider", &a, &["/id"], false, "provider-a", false),
            ("acmeCo/provider", &b, &["/next"], false, "provider-b", true),
            // A rolling key migration: bindings disagree, so the key waits.
            // The provider's schema change is reported with the pending key.
            (
                "acmeCo/migrating",
                &next,
                &["/next"],
                false,
                "migrating-1",
                false,
            ),
            (
                "acmeCo/migrating",
                &next,
                &["/id"],
                false,
                "migrating-2",
                false,
            ),
            // A completed migration: bindings agree, and the key changes.
            (
                "acmeCo/migrated",
                &next,
                &["/next"],
                false,
                "migrated-1",
                false,
            ),
            (
                "acmeCo/migrated",
                &next,
                &["/next"],
                false,
                "migrated-2",
                false,
            ),
            // An agreed key is a fallback only if every binding flags it.
            // Here it isn't, so it replaces a key which still resolves.
            (
                "acmeCo/mixed-fallback",
                &next,
                &["/next"],
                true,
                "mixed-1",
                false,
            ),
            (
                "acmeCo/mixed-fallback",
                &next,
                &["/next"],
                false,
                "mixed-2",
                false,
            ),
            // Here it is, and the existing key resolves, so it's kept.
            (
                "acmeCo/all-fallback",
                &next,
                &["/next"],
                true,
                "fallback-1",
                false,
            ),
            (
                "acmeCo/all-fallback",
                &next,
                &["/next"],
                true,
                "fallback-2",
                false,
            ),
            // Without enabled bindings, disabled bindings determine the merge.
            ("acmeCo/disabled", &x1, &["/id"], false, "disabled-1", true),
            ("acmeCo/disabled", &x2, &["/id"], false, "disabled-2", true),
        ]);

        let mut draft = tables::DraftCatalog::default();
        let mut live = tables::LiveCatalog::default();
        for name in [
            "acmeCo/provider",
            "acmeCo/migrating",
            "acmeCo/migrated",
            "acmeCo/mixed-fallback",
            "acmeCo/all-fallback",
        ] {
            live.collections.insert(live_collection(
                name,
                json!({"schema": {"$defs": {"flow://connector-schema": {}}}, "key": ["/id"]}),
            ));
        }

        let modified =
            merge_collections(&merged, &mut draft.collections, &live.collections, &RESET).unwrap();

        insta::assert_debug_snapshot!(drafted(&draft.collections));
        insta::assert_debug_snapshot!(modified);
    }

    #[test]
    fn test_modified_is_reported_under_first_binding() {
        // A live collection fed only by an added binding is reported,
        // under the binding which is also reported as added.
        let mut merged = merged(&[
            (
                "acmeCo/only-added",
                r#"{"const": "next"}"#,
                &["/id"],
                false,
                "only-added",
                false,
            ),
            (
                "acmeCo/fan-in",
                r#"{"const": "next"}"#,
                &["/id"],
                false,
                "fan-in-disabled",
                true,
            ),
            (
                "acmeCo/fan-in",
                r#"{"const": "next"}"#,
                &["/id"],
                false,
                "fan-in-enabled",
                false,
            ),
        ]);
        merged.added.insert(
            vec!["only-added".to_string()],
            Changed {
                target: models::Collection::new("acmeCo/only-added"),
                disable: false,
                reason: None,
            },
        );

        let mut draft = tables::DraftCatalog::default();
        let mut live = tables::LiveCatalog::default();
        for name in ["acmeCo/only-added", "acmeCo/fan-in"] {
            live.collections.insert(live_collection(
                name,
                json!({"schema": {"$defs": {"flow://connector-schema": {}}}, "key": ["/id"]}),
            ));
        }

        let modified =
            merge_collections(&merged, &mut draft.collections, &live.collections, &RESET).unwrap();

        insta::assert_debug_snapshot!(
            modified.iter().map(|(path, c)| (path, &c.target, c.disable)).collect::<Vec<_>>(),
            @r#"
        [
            (
                [
                    "fan-in-enabled",
                ],
                Collection(
                    "acmeCo/fan-in",
                ),
                false,
            ),
            (
                [
                    "only-added",
                ],
                Collection(
                    "acmeCo/only-added",
                ),
                false,
            ),
        ]
        "#
        );
    }

    #[test]
    fn test_deleted_in_draft_is_replaced() {
        let merged = merged(&[
            (
                "acmeCo/live-deleted",
                r#"{"const": "next"}"#,
                &["/next"],
                false,
                "live",
                false,
            ),
            (
                "acmeCo/draft-deleted",
                r#"{"const": "next"}"#,
                &["/next"],
                false,
                "draft",
                false,
            ),
        ]);

        let mut draft = tables::DraftCatalog::default();
        for name in ["acmeCo/live-deleted", "acmeCo/draft-deleted"] {
            draft.collections.insert(tables::DraftCollection {
                collection: models::Collection::new(name),
                scope: tables::synthetic_scope(models::CatalogType::Collection, name),
                expect_pub_id: Some(models::Id::new([0, 0, 0, 0, 0, 0, 0, 7])),
                data_plane_id: models::Id::zero(),
                model: None,
                is_touch: false,
            });
        }
        let mut live = tables::LiveCatalog::default();
        live.collections.insert(live_collection(
            "acmeCo/live-deleted",
            json!({
                "schema": {"$defs": {"flow://connector-schema": {}}},
                "key": ["/id"],
                "projections": {"user": "/customized"},
            }),
        ));

        // Replacement doesn't depend on `reset_on_key_change`.
        let modified = merge_collections(
            &merged,
            &mut draft.collections,
            &live.collections,
            &Options::default(),
        )
        .unwrap();

        insta::assert_debug_snapshot!(
            draft.collections.iter().map(|r| (&r.collection, r.expect_pub_id, r.is_touch)).collect::<Vec<_>>(),
            @r#"
        [
            (
                Collection(
                    "acmeCo/draft-deleted",
                ),
                Some(
                    0000000000000007,
                ),
                false,
            ),
            (
                Collection(
                    "acmeCo/live-deleted",
                ),
                Some(
                    0000000000000007,
                ),
                false,
            ),
        ]
        "#
        );
        insta::assert_debug_snapshot!(drafted(&draft.collections));
        insta::assert_debug_snapshot!(modified);
    }

    #[test]
    fn test_connector_schema_update() {
        let collection = models::Collection::new("test/collection");
        let connector_schema =
            models::Schema::new(models::RawValue::from_value(&serde_json::json!({
                "type": "object",
                "properties": {"id": {"type": "string"}}
            })));

        // Case 1: No existing schema - should create new with connector schema reference
        let updated = update_connector_schema(&collection, None, &connector_schema)
            .unwrap()
            .unwrap();
        insta::assert_json_snapshot!(updated.to_value(), @r###"
        {
          "$defs": {
            "flow://connector-schema": {
              "$id": "flow://connector-schema",
              "properties": {
                "id": {
                  "type": "string"
                }
              },
              "type": "object"
            }
          },
          "$ref": "flow://connector-schema"
        }
        "###);

        // Case 2: Existing schema without connector $defs - should return an error
        let existing = models::Schema::new(
            models::RawValue::from_str(
                r#"{"type": "object", "properties": {"old": {"type": "string"}}}"#,
            )
            .unwrap(),
        );
        let err = update_connector_schema(&collection, Some(&existing), &connector_schema)
            .expect_err("should fail for schema without connector $defs");
        insta::assert_snapshot!(
            format!("{err:#}"),
            @r###"collection test/collection has a schema that is not managed by auto-discover (it has no $defs entry for 'flow://connector-schema'). To opt in, add {"$defs": {"flow://connector-schema": {}}} to the collection schema"###
        );

        // Case 3: Existing schema with connector reference - should preserve outer structure
        let existing_with_ref = models::Schema::new(
            models::RawValue::from_str(
                r#"{
                "$defs": {
                    "flow://connector-schema": {
                        "$id": "flow://connector-schema",
                        "const": "old"
                    }
                },
                "$ref": "flow://connector-schema",
                "properties": {
                    "userExtension": {"type": "string"}
                }
            }"#,
            )
            .unwrap(),
        );
        let updated =
            update_connector_schema(&collection, Some(&existing_with_ref), &connector_schema)
                .unwrap()
                .unwrap();
        insta::assert_json_snapshot!(updated.to_value(), @r###"
        {
          "$defs": {
            "flow://connector-schema": {
              "$id": "flow://connector-schema",
              "properties": {
                "id": {
                  "type": "string"
                }
              },
              "type": "object"
            }
          },
          "$ref": "flow://connector-schema",
          "properties": {
            "userExtension": {
              "type": "string"
            }
          }
        }
        "###);

        // Case 4: Existing schema already reflects connector schema - no update needed
        let existing_complete =
            models::Schema::new(models::RawValue::from_value(&serde_json::json!({
                "$ref": "flow://connector-schema",
                "title": "Example user modification",
                "$defs": {
                    "flow://connector-schema": {
                        "$id": "flow://connector-schema",
                        "type": "object",
                        "properties": {"id": {"type": "string"}}
                    }
                }
            })));
        let result =
            update_connector_schema(&collection, Some(&existing_complete), &connector_schema)
                .unwrap();
        assert!(result.is_none());

        // Case 5: Schema mentions connector URL (e.g. in $id) but NOT as a $defs key -
        // should still be rejected by the strict check.
        let has_url_but_no_def = models::Schema::new(
            models::RawValue::from_str(r#"{"$id": "flow://connector-schema", "type": "object"}"#)
                .unwrap(),
        );
        let err =
            update_connector_schema(&collection, Some(&has_url_but_no_def), &connector_schema)
                .expect_err("should fail for schema with URL but no $defs key");
        insta::assert_snapshot!(
            format!("{err:#}"),
            @r###"collection test/collection has a schema that is not managed by auto-discover (it has no $defs entry for 'flow://connector-schema'). To opt in, add {"$defs": {"flow://connector-schema": {}}} to the collection schema"###
        );
    }
}
