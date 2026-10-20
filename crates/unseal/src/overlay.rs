use anyhow::Context;
use json::ptr::Token;

/// Decrypt a `sops`-protected endpoint config, applying its `sops.overlay` if present.
///
/// An endpoint config may carry a plaintext `sops.overlay` object within its `sops`
/// metadata stanza. The overlay mirrors the structure of the config and is merged into
/// the SOPS-decrypted document using JSON Merge Patch (RFC 7396) semantics, which lets
/// non-security-relevant fields be modified without re-encrypting (and re-MAC-ing) the
/// configuration.
///
/// Before merging, the overlay is validated against `schema` to ensure it only modifies
/// locations annotated `nonsensitive: true`, or deletes properties the schema does not
/// describe. This is what prevents an overlay from, say, rewriting a database hostname
/// out from under an encrypted password.
///
/// When the config has no overlay this is byte-for-byte equivalent to [`super::decrypt_sops`].
pub async fn decrypt_with_overlay(
    sealed: &models::RawValue,
    schema: &[u8],
) -> anyhow::Result<models::RawValue> {
    let Some(overlay) = extract_overlay(sealed)? else {
        return super::decrypt_sops(sealed).await;
    };

    validate_overlay_nonsensitive(&overlay, schema)
        .context("validating endpoint config `sops.overlay`")?;

    let decrypted = super::decrypt_sops(sealed).await?;

    // Merge the overlay into the decrypted base via JSON Merge Patch (RFC 7396).
    let mut merged = decrypted.to_value();
    json_patch::merge(&mut merged, &overlay);

    Ok(models::RawValue::from_value(&merged))
}

/// Pull the `sops.overlay` object out of a sealed config, if one is present.
/// A missing or `null` overlay returns `None`, which is the backwards-compatible
/// no-op case shared by all configs encrypted before overlays existed.
fn extract_overlay(sealed: &models::RawValue) -> anyhow::Result<Option<serde_json::Value>> {
    let dom = sealed.to_value();

    let overlay = dom
        .as_object()
        .and_then(|doc| doc.get("sops"))
        .and_then(|sops| sops.as_object())
        .and_then(|sops| sops.get("overlay"));

    match overlay {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(overlay) => Ok(Some(overlay.clone())),
    }
}

/// Validate that every location an `overlay` would modify under JSON Merge Patch semantics
/// is annotated `nonsensitive: true` in `schema`, or lies within a `nonsensitive` subtree,
/// or is an undeclared property which the overlay deletes (see [`is_undeclared`]).
///
/// A `nonsensitive` annotation is a human assertion that everything within is safe to modify
/// outside the SOPS MAC. This is the sole protection, so the annotations must be vetted by a
/// human and should err on the side of safety when it's unclear if a location is sensitive.
fn validate_overlay_nonsensitive(overlay: &serde_json::Value, schema: &[u8]) -> anyhow::Result<()> {
    let bundle = doc::validation::build_bundle(schema).context("building config schema")?;
    let validator = doc::Validator::new(bundle).context("preparing config schema validator")?;
    let schema = ConfigSchema {
        shape: doc::Shape::infer(validator.schema(), validator.schema_index()),
        compiled: validator.schema(),
        index: validator.schema_index(),
    };

    check_location(&schema, &mut json::Pointer(Vec::new()), overlay)
}

/// A config schema, both as its inferred `Shape` and in its compiled form.
struct ConfigSchema<'s> {
    shape: doc::Shape,
    compiled: &'s doc::validation::Schema,
    index: &'s doc::validation::SchemaIndex<'s>,
}

fn check_location(
    schema: &ConfigSchema,
    ptr: &mut json::Pointer,
    node: &serde_json::Value,
) -> anyhow::Result<()> {
    let (located, _exists) = schema.shape.locate(ptr);

    // A nonsensitive annotation authorizes this location and/or subtree.
    if located.nonsensitive == Some(true) {
        return Ok(());
    }

    let fields = match node {
        // A non-empty object recurses: each member is itself a write, checked in turn.
        serde_json::Value::Object(fields) if !fields.is_empty() => fields,
        // An empty object looks inert but is not. Under JSON Merge Patch a non-object
        // base is first replaced by `{}` before members are merged (RFC 7396), so an
        // empty object could still silently clobber a scalar or array secret it lands
        // on. An empty object is safe IFF the schema requires that location to be an
        // object in the target.
        serde_json::Value::Object(_) if located.type_ == json::schema::types::OBJECT => {
            return Ok(());
        }
        // A scalar, array, `null`, or clobbering empty object is a merge-patch write
        // at a location which, per the check above, is not marked nonsensitive.
        _ => {
            anyhow::bail!("overlay modifies location '{ptr}', which is not marked nonsensitive")
        }
    };

    for (property, child) in fields {
        // A `null` deletes the property, which can't redirect or replace anything the
        // MAC protects. It's permitted where the schema says nothing about the property,
        // which is how connectors retire config fields they've removed from their schema.
        if child.is_null() && is_undeclared(schema, located, ptr, property) {
            continue;
        }
        ptr.push(Token::Property(property.clone()));
        check_location(schema, ptr, child)?;
        ptr.0.pop();
    }
    Ok(())
}

/// Whether `property` of the object at `ptr`, whose shape is `parent`, is one the schema
/// does not describe.
///
/// The object must be structured: it can only be an object, so a merge can't clobber it
/// with `{}`, and it declares properties, so its other keys are strays rather than the
/// entries of a free-form map. A free-form map's entries may be security-relevant (TLS
/// parameters of a connection string, for example), so they remain protected, as does
/// any property admitted by an `additionalProperties` schema other than `false`.
fn is_undeclared(
    schema: &ConfigSchema,
    parent: &doc::Shape,
    ptr: &json::Pointer,
    property: &str,
) -> bool {
    if parent.type_ != json::schema::types::OBJECT {
        return false;
    }
    let Some(path) = ptr
        .iter()
        .map(|token| match token {
            Token::Property(property) => Some(property.as_str()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };

    let mut found = Described::default();
    describe(
        schema.compiled,
        schema.index,
        &path,
        property,
        0,
        &mut found,
    );
    found.structured && !found.declared
}

#[derive(Default)]
struct Described {
    // Some subschema applying at the object declares properties.
    structured: bool,
    // Some subschema applying at the object describes the property.
    declared: bool,
}

// Bounds `describe` through cyclic references.
const MAX_DESCRIBE_DEPTH: usize = 64;

/// Walk `schema` to the object at `path`, recording into `out` what the subschemas
/// applying there say about `property`: whether they declare it by name or pattern, or
/// admit it through an `additionalProperties` or `unevaluatedProperties` other than
/// `false`. A reference which can't be followed counts as declaring it.
///
/// This walks the compiled schema rather than its inferred `Shape`, because a `Shape`
/// union of `oneOf` branches drops properties which only some of the branches declare.
fn describe(
    schema: &doc::validation::Schema,
    index: &doc::validation::SchemaIndex,
    path: &[&str],
    property: &str,
    depth: usize,
    out: &mut Described,
) {
    use json::schema::Keyword;

    if depth == MAX_DESCRIBE_DEPTH {
        out.declared = true;
        return;
    }
    // Whether a property or pattern of this subschema matched the next path component,
    // which decides if `additionalProperties` applies to it.
    let mut matched = false;

    for kw in schema.keywords.iter() {
        match (kw, path.split_first()) {
            // In-place applicators apply at this same location.
            (Keyword::AllOf { all_of: subs }, _)
            | (Keyword::AnyOf { any_of: subs }, _)
            | (Keyword::OneOf { one_of: subs }, _) => {
                for sub in subs.iter() {
                    describe(sub, index, path, property, depth + 1, out);
                }
            }
            (Keyword::If { r#if: sub }, _)
            | (Keyword::Then { then: sub }, _)
            | (Keyword::Else { r#else: sub }, _)
            | (Keyword::Not { not: sub }, _) => {
                describe(sub, index, path, property, depth + 1, out)
            }
            (Keyword::DependentSchemas { dependent_schemas }, _) => {
                for (_, sub) in dependent_schemas.iter() {
                    describe(sub, index, path, property, depth + 1, out);
                }
            }
            (Keyword::Ref { r#ref }, _) => match index.fetch(r#ref) {
                Some((sub, _dynamic)) => describe(sub, index, path, property, depth + 1, out),
                None => out.declared = true,
            },
            (Keyword::DynamicRef { .. }, _) => out.declared = true,

            // At the object itself.
            (Keyword::Properties { properties }, None) => {
                for (name, _) in properties.iter() {
                    // The name is prefixed by a status byte, where '+' is required-only.
                    out.structured |= name.as_bytes()[0] != b'+';
                    out.declared |= &name[1..] == property;
                }
            }
            (Keyword::PatternProperties { pattern_properties }, None) => {
                out.declared |= pattern_properties
                    .iter()
                    .any(|(re, _)| re.is_match(property));
            }
            (
                Keyword::AdditionalProperties {
                    additional_properties: sub,
                },
                None,
            )
            | (
                Keyword::UnevaluatedProperties {
                    unevaluated_properties: sub,
                },
                None,
            ) => {
                out.declared |= !is_false(sub);
            }

            // Above the object, on the way to it.
            (Keyword::Properties { properties }, Some((next, rest))) => {
                for (name, sub) in properties.iter() {
                    if name.as_bytes()[0] != b'+' && &name[1..] == *next {
                        matched = true;
                        describe(sub, index, rest, property, depth + 1, out);
                    }
                }
            }
            (Keyword::PatternProperties { pattern_properties }, Some((next, rest))) => {
                for (re, sub) in pattern_properties.iter() {
                    if re.is_match(next) {
                        matched = true;
                        describe(sub, index, rest, property, depth + 1, out);
                    }
                }
            }
            (
                Keyword::UnevaluatedProperties {
                    unevaluated_properties: sub,
                },
                Some((_, rest)),
            ) => {
                describe(sub, index, rest, property, depth + 1, out);
            }
            _ => {}
        }
    }

    if let Some((_, rest)) = path.split_first().filter(|_| !matched) {
        for kw in schema.keywords.iter() {
            if let Keyword::AdditionalProperties {
                additional_properties: sub,
            } = kw
            {
                describe(sub, index, rest, property, depth + 1, out);
            }
        }
    }
}

fn is_false(schema: &doc::validation::Schema) -> bool {
    schema
        .keywords
        .iter()
        .any(|kw| matches!(kw, json::schema::Keyword::False))
}

#[cfg(test)]
mod test {
    use super::{extract_overlay, validate_overlay_nonsensitive};
    use serde_json::json;

    // A source-postgres-esque schema: `address`/`credentials` are sensitive (unannotated),
    // `/advanced/backfill_chunk_size` is a single nonsensitive leaf, and `/tunables` is a
    // nonsensitive subtree.
    fn schema() -> Vec<u8> {
        json!({
            "type": "object",
            "properties": {
                "address": { "type": "string" },
                "credentials": {
                    "type": "object",
                    "properties": {
                        "user": { "type": "string" },
                        "password": { "type": "string", "secret": true },
                    },
                },
                "advanced": {
                    "type": "object",
                    "properties": {
                        "backfill_chunk_size": { "type": "integer", "nonsensitive": true },
                        "slot_name": { "type": "string" },
                    },
                },
                "tunables": {
                    "type": "object",
                    "nonsensitive": true,
                    "properties": {
                        "parallelism": { "type": "integer" },
                    },
                },
            },
        })
        .to_string()
        .into_bytes()
    }

    fn check(overlay: serde_json::Value) -> anyhow::Result<()> {
        validate_overlay_nonsensitive(&overlay, &schema())
    }

    #[test]
    fn nonsensitive_leaf_is_accepted() {
        check(json!({"advanced": {"backfill_chunk_size": 50000}})).unwrap();
    }

    #[test]
    fn nonsensitive_subtree_is_accepted() {
        // Anything within a `nonsensitive` subtree is permitted, including new
        // properties and nested objects the schema does not enumerate.
        check(json!({"tunables": {"parallelism": 8, "extra": {"deep": true}}})).unwrap();
    }

    #[test]
    fn empty_overlay_is_a_noop() {
        // A whole-config empty overlay, and an empty object over the object-typed
        // (and thus never-clobbered) `credentials` location, are both true no-ops.
        check(json!({})).unwrap();
        check(json!({"credentials": {}})).unwrap();
    }

    #[test]
    fn empty_object_clobbering_a_scalar_is_rejected() {
        // `address` is a sensitive string. An empty object is not the no-op it looks
        // like: merge-patch would reset the string to `{}`, dropping the secret, so it
        // must be rejected the same as any other write to a sensitive location.
        let err = check(json!({"address": {}})).unwrap_err();
        assert!(err.to_string().contains("/address"), "{err}");
    }

    #[test]
    fn sensitive_scalar_is_rejected() {
        // The hostname-rewrite attack: not nonsensitive, so it cannot be overlaid.
        let err = check(json!({"address": "evil.example.com:5432"})).unwrap_err();
        assert!(err.to_string().contains("/address"), "{err}");
    }

    #[test]
    fn nested_sensitive_field_is_rejected() {
        // A field nested within an unannotated object is sensitive like any other.
        let err = check(json!({"credentials": {"password": "pwn"}})).unwrap_err();
        assert!(err.to_string().contains("/credentials/password"), "{err}");
    }

    #[test]
    fn null_delete_of_sensitive_field_is_rejected() {
        // A merge-patch `null` deletes a key, which is still a modification.
        assert!(check(json!({"address": null})).is_err());
        assert!(check(json!({"advanced": {"slot_name": null}})).is_err());
    }

    #[test]
    fn sensitive_sibling_within_advanced_is_rejected() {
        // `/advanced` itself is not nonsensitive: only `backfill_chunk_size` is.
        assert!(check(json!({"advanced": {"slot_name": "flow_slot"}})).is_err());
    }

    #[test]
    fn unknown_field_is_rejected() {
        // A field absent from the schema cannot be proven nonsensitive.
        assert!(check(json!({"advanced": {"not_in_schema": 1}})).is_err());
        assert!(check(json!({"totally_unknown": "x"})).is_err());
    }

    #[test]
    fn non_object_root_overlay_is_rejected() {
        assert!(check(json!("just a string")).is_err());
        assert!(check(json!([1, 2, 3])).is_err());
    }

    #[test]
    fn unparseable_schema_rejects_overlay() {
        // Fail-safe: without a usable schema we cannot prove anything nonsensitive.
        assert!(
            validate_overlay_nonsensitive(&json!({"advanced": {"backfill_chunk_size": 1}}), b"")
                .is_err()
        );
    }

    // A schema whose objects take each shape that bears on deleting a property the
    // schema does not describe.
    fn retired_fields_schema() -> Vec<u8> {
        json!({
            "type": "object",
            "properties": {
                "address": { "type": "string" },
                // A closed object, by reference: undeclared properties cannot exist.
                "advanced": { "$ref": "#/$defs/advanced" },
                // A union whose branches declare different properties, which a `Shape`
                // union drops in favor of the commonly declared `kind`.
                "auth": {
                    "type": "object",
                    "properties": { "kind": { "type": "string" } },
                    "oneOf": [
                        { "properties": { "user": { "type": "string" } } },
                        { "properties": { "token": { "type": "string", "secret": true } } },
                    ],
                },
                // A nullable object, which a merge would turn from `null` into `{}`.
                "nullable": {
                    "type": ["object", "null"],
                    "properties": { "slot_name": { "type": "string" } },
                },
                // Free-form maps, whose entries are user data rather than strays.
                "params": { "type": "object" },
                "labels": {
                    "type": "object",
                    "properties": { "name": { "type": "string" } },
                    "additionalProperties": { "type": "string" },
                },
                "open": {
                    "type": "object",
                    "properties": { "name": { "type": "string" } },
                    "additionalProperties": true,
                },
                "patterned": {
                    "type": "object",
                    "properties": { "name": { "type": "string" } },
                    "patternProperties": { "^x-": { "type": "string" } },
                },
            },
            // Required, but otherwise undescribed.
            "required": ["password"],
            "$defs": {
                "advanced": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": { "slot_name": { "type": "string" } },
                },
            },
        })
        .to_string()
        .into_bytes()
    }

    fn check_retired(overlay: serde_json::Value) -> anyhow::Result<()> {
        validate_overlay_nonsensitive(&overlay, &retired_fields_schema())
    }

    #[test]
    fn deleting_undeclared_property_is_accepted() {
        check_retired(json!({"legacy_password": null})).unwrap();
        check_retired(json!({"legacy_block": null})).unwrap();
        check_retired(json!({"advanced": {"legacy_flag": null}})).unwrap();
        check_retired(json!({"auth": {"legacy_key": null}})).unwrap();
        check_retired(json!({"legacy_password": null, "advanced": {"legacy_flag": null}})).unwrap();
    }

    #[test]
    fn deleting_declared_property_is_rejected() {
        for (overlay, location) in [
            (json!({"address": null}), "/address"),
            (json!({"password": null}), "/password"),
            (
                json!({"advanced": {"slot_name": null}}),
                "/advanced/slot_name",
            ),
            (json!({"auth": {"token": null}}), "/auth/token"),
            (
                json!({"patterned": {"x-trace": null}}),
                "/patterned/x-trace",
            ),
        ] {
            let err = check_retired(overlay).unwrap_err();
            assert!(err.to_string().contains(location), "{err}");
        }
    }

    #[test]
    fn deleting_map_entry_is_rejected() {
        // Deleting `sslmode` from connection parameters could downgrade TLS.
        assert!(check_retired(json!({"params": {"sslmode": null}})).is_err());
        assert!(check_retired(json!({"labels": {"team": null}})).is_err());
        assert!(check_retired(json!({"open": {"team": null}})).is_err());
    }

    #[test]
    fn deleting_within_unstructured_parent_is_rejected() {
        // An undeclared parent may be a scalar, which the merge would replace with `{}`.
        assert!(check_retired(json!({"legacy_block": {"inner": null}})).is_err());
        // As would a declared scalar.
        assert!(check_retired(json!({"address": {"inner": null}})).is_err());
        // And a nullable object would go from `null` to `{}`.
        assert!(check_retired(json!({"nullable": {"legacy_flag": null}})).is_err());
    }

    #[test]
    fn writing_undeclared_property_is_rejected() {
        // Only deletion is permitted: any value, even an empty object, is a write.
        assert!(check_retired(json!({"legacy_password": "hunter2"})).is_err());
        assert!(check_retired(json!({"legacy_block": {}})).is_err());
        assert!(check_retired(json!({"advanced": {"legacy_flag": true}})).is_err());
    }

    #[test]
    fn extract_overlay_variants() {
        let absent: Box<models::RawValue> =
            serde_json::from_value(json!({"address": "db:5432"})).unwrap();
        assert!(extract_overlay(&absent).unwrap().is_none());

        let null: Box<models::RawValue> =
            serde_json::from_value(json!({"sops": {"overlay": null}})).unwrap();
        assert!(extract_overlay(&null).unwrap().is_none());

        let present: Box<models::RawValue> = serde_json::from_value(
            json!({"sops": {"overlay": {"advanced": {"backfill_chunk_size": 50000}}}}),
        )
        .unwrap();
        assert_eq!(
            extract_overlay(&present).unwrap().unwrap(),
            json!({"advanced": {"backfill_chunk_size": 50000}}),
        );
    }
}
