use super::{Error, Scope};
use proto_flow::flow;

pub fn walk_data_plane<'s, 'a>(
    this_scope: Scope<'s>,
    this_entity: &str,
    data_plane_id: models::Id,
    data_planes: &'a tables::DataPlanes,
    errors: &mut tables::Errors,
) -> Option<&'a tables::DataPlane> {
    let data_plane = data_planes.get_by_key(&data_plane_id);

    if data_plane.is_none() {
        Error::MissingDataPlane {
            this_entity: this_entity.to_string(),
            data_plane_id,
        }
        .push(this_scope, errors);
    }

    data_plane
}

pub fn walk_reference<'s, 'a>(
    this_scope: Scope<'s>,
    this_entity: &str,
    ref_name: &models::Collection,
    built_collections: &'a tables::BuiltCollections,
    errors: &mut tables::Errors,
) -> Option<(flow::CollectionSpec, &'a tables::BuiltCollection)> {
    const COLLECTION: &'static str = "collection";

    if let Some(row) = built_collections.get_key(ref_name) {
        if let Some(spec) = &row.spec {
            let mut spec = spec.clone();
            spec.derivation = None; // Clear interior derivation, returning just the collection.
            return Some((spec, row));
        }
        Error::DeletedSpecStillInUse {
            this_entity: this_entity.to_string(),
            ref_entity: COLLECTION,
            ref_name: ref_name.to_string(),
        }
        .push(this_scope, errors);

        return None;
    }

    let closest = built_collections
        .iter()
        .filter_map(|t| {
            let (name, scope) = (&t.collection, &t.scope);
            let dist = strsim::osa_distance(&ref_name, &name);

            if dist <= 4 {
                Some((dist, name, scope))
            } else {
                None
            }
        })
        .min();

    if let Some((_, suggest_name, suggest_scope)) = closest {
        Error::NoSuchEntitySuggest {
            this_entity: this_entity.to_string(),
            ref_entity: COLLECTION,
            ref_name: ref_name.to_string(),
            suggest_name: suggest_name.to_string(),
            suggest_scope: suggest_scope.clone(),
        }
        .push(this_scope, errors);
    } else {
        Error::NoSuchEntity {
            this_entity: this_entity.to_string(),
            ref_entity: COLLECTION,
            ref_name: ref_name.to_string(),
        }
        .push(this_scope, errors);
    }

    None
}
