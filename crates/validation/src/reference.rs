use super::{Error, Scope};
use std::collections::BTreeSet;

pub fn gather_referenced_collections<'a>(
    captures: &'a tables::DraftCaptures,
    collections: &'a tables::DraftCollections,
    materializations: &'a tables::DraftMaterializations,
    tests: &'a tables::DraftTests,
) -> Vec<models::Collection> {
    let mut out = BTreeSet::new();

    for capture in captures.iter() {
        if let Some(spec) = &capture.spec {
            for binding in spec.bindings.iter().filter(|b| !b.disable) {
                out.insert(&binding.target);
            }
        }
    }
    for collection in collections.iter() {
        let Some(models::CollectionDef {
            derive: Some(derive),
            ..
        }) = &collection.spec
        else {
            continue;
        };

        for transform in derive.transforms.iter().filter(|b| !b.disable) {
            out.insert(&transform.source.collection());
        }
    }
    for materialization in materializations.iter() {
        if let Some(spec) = &materialization.spec {
            for binding in spec.bindings.iter().filter(|b| !b.disable) {
                out.insert(&binding.source.collection());
            }
        }
    }
    for test in tests.iter() {
        if let Some(spec) = &test.spec {
            for step in spec.iter() {
                match step {
                    models::TestStep::Ingest(models::TestStepIngest { collection, .. }) => {
                        out.insert(collection);
                    }
                    models::TestStep::Verify(models::TestStepVerify { collection, .. }) => {
                        out.insert(collection.collection());
                    }
                }
            }
        }
    }

    // Now remove collections which are included locally.
    for collection in collections.iter() {
        out.remove(&collection.catalog_name);
    }

    out.into_iter().cloned().collect()
}

pub fn gather_inferred_collections(
    collections: &tables::DraftCollections,
) -> Vec<models::Collection> {
    collections
        .iter()
        .filter_map(|row| {
            if row
                .spec
                .as_ref()
                .unwrap()
                .read_schema
                .as_ref()
                .map(|schema| schema.references_inferred_schema())
                .unwrap_or_default()
            {
                Some(row.catalog_name.clone())
            } else {
                None
            }
        })
        .collect()
}

pub fn walk_reference<'a, T, F>(
    this_scope: Scope<'a>,
    this_entity: &str,
    ref_entity: &'static str,
    ref_name: &str,
    entities: &'a [T],
    entity_fn: F,
    errors: &mut tables::Errors,
) -> Option<&'a T>
where
    F: Fn(&'a T) -> (&'a str, Scope<'a>),
{
    if let Some(entity) = entities.iter().find(|t| entity_fn(t).0 == ref_name) {
        return Some(entity);
    }

    let closest = entities
        .iter()
        .filter_map(|t| {
            let (name, scope) = entity_fn(t);
            let dist = strsim::osa_distance(&ref_name, &name);

            if dist <= 4 {
                Some((dist, name, scope.flatten()))
            } else {
                None
            }
        })
        .min();

    if let Some((_, suggest_name, suggest_scope)) = closest {
        Error::NoSuchEntitySuggest {
            this_entity: this_entity.to_string(),
            ref_entity,
            ref_name: ref_name.to_string(),
            suggest_name: suggest_name.to_string(),
            suggest_scope: suggest_scope,
        }
        .push(this_scope, errors);
    } else {
        Error::NoSuchEntity {
            this_entity: this_entity.to_string(),
            ref_entity,
            ref_name: ref_name.to_string(),
        }
        .push(this_scope, errors);
    }

    None
}
