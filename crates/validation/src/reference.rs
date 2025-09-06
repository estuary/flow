use super::{Error, Scope};
use proto_flow::flow;

pub fn walk_reference<'s, 'a, F>(
    this_scope: Scope<'s>,
    this_entity: &'static str,
    this_name: F,
    ref_name: &models::Collection,
    built_collections: &'a tables::BuiltCollections,
    errors: Option<&mut tables::Errors>,
) -> Option<(flow::CollectionSpec, &'a tables::BuiltCollection)>
where
    F: Fn() -> String,
{
    const COLLECTION: &'static str = "collection";

    if let Some(row) = built_collections.get_key(ref_name) {
        if let Some(spec) = &row.spec {
            let mut spec = spec.clone();
            spec.derivation = None; // Clear interior derivation, returning just the collection.
            return Some((spec, row));
        } else if let Some(errors) = errors {
            Error::DeletedSpecStillInUse {
                this_entity: this_entity.to_string(),
                ref_entity: COLLECTION,
                ref_name: ref_name.to_string(),
            }
            .push(this_scope, errors);
            return None;
        }
    }

    let Some(errors) = errors else {
        return None;
    };

    if let Some((_, suggest_name)) = built_collections
        .iter()
        .map(|m| {
            (
                strsim::osa_distance(&ref_name, &m.collection),
                &m.collection,
            )
        })
        .min()
    {
        Error::NoSuchEntitySuggest {
            this_entity: this_entity,
            this_name: this_name(),
            ref_entity: COLLECTION,
            ref_name: ref_name.to_string(),
            suggest_name: suggest_name.to_string(),
        }
        .push(this_scope, errors);
    } else {
        Error::NoSuchEntity {
            this_entity: this_entity,
            this_name: this_name(),
            ref_entity: COLLECTION,
            ref_name: ref_name.to_string(),
        }
        .push(this_scope, errors);
    }

    None
}
