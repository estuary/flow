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
/// locations annotated `nonsensitive: true`. This is what prevents an overlay from, say,
/// rewriting a database hostname out from under an encrypted password.
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
/// is annotated `nonsensitive: true` in `schema`, or lies within a `nonsensitive` subtree.
///
/// A `nonsensitive` annotation is a human assertion that everything within is safe to modify
/// outside the SOPS MAC. This is the sole protection, so the annotations must be vetted by a
/// human and should err on the side of safety when it's unclear if a location is sensitive.
fn validate_overlay_nonsensitive(overlay: &serde_json::Value, schema: &[u8]) -> anyhow::Result<()> {
    let bundle = doc::validation::build_bundle(schema).context("building config schema")?;
    let validator = doc::Validator::new(bundle).context("preparing config schema validator")?;
    let shape = doc::Shape::infer(validator.schema(), validator.schema_index());

    check_location(&shape, &mut json::Pointer(Vec::new()), overlay)
}

fn check_location(
    root: &doc::Shape,
    ptr: &mut json::Pointer,
    node: &serde_json::Value,
) -> anyhow::Result<()> {
    let (located, _exists) = root.locate(ptr);

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
        ptr.push(Token::Property(property.clone()));
        check_location(root, ptr, child)?;
        ptr.0.pop();
    }
    Ok(())
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
