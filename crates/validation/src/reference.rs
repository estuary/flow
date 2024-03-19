use super::{Error, Scope};
use std::collections::BTreeSet;

pub fn gather_referenced_collections<'a>(
    captures: &'a [tables::Capture],
    collections: &'a [tables::Collection],
    materializations: &'a [tables::Materialization],
    tests: &'a [tables::Test],
) -> Vec<models::Collection> {
    let mut out = BTreeSet::new();

    for capture in captures {
        for binding in capture.spec.bindings.iter().filter(|b| !b.disable) {
            out.insert(&binding.target);
        }
    }
    for collection in collections {
        let Some(derive) = &collection.spec.derive else {
            continue;
        };

        for transform in derive.transforms.iter().filter(|b| !b.disable) {
            out.insert(&transform.source.collection());
        }
    }
    for materialization in materializations {
        for binding in materialization.spec.bindings.iter().filter(|b| !b.disable) {
            out.insert(&binding.source.collection());
        }
    }
    for test in tests {
        for step in test.spec.iter() {
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

    // Now remove collections which are included locally.
    for collection in collections {
        out.remove(&collection.collection);
    }

    out.into_iter().cloned().collect()
}

pub fn gather_inferred_collections(collections: &[tables::Collection]) -> Vec<models::Collection> {
    collections
        .iter()
        .filter_map(|row| {
            if row
                .spec
                .read_schema
                .as_ref()
                .map(|schema| schema.references_inferred_schema())
                .unwrap_or_default()
            {
                Some(row.collection.clone())
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
