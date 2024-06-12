use std::collections::BTreeSet;

use models::ModelDef;
use tables::{BuiltRow, Validations};

/// Prunes unbound collections from the build output. Collections are considered
/// unbound if they are new (updates to existing collections are never pruned),
/// and no other specs in the build read from or write to it.
pub fn prune_unbound_collections(built: &mut Validations) -> BTreeSet<models::Collection> {
    // Collect the set of all collection names that are used by any specs in
    // the build.
    let mut referenced_collections = BTreeSet::new();
    // Start by including the ops collections, to ensure that we never prune
    // those. This is a hack, which will need reconsidered once we move to
    // federated data planes, with ops collections being resolved dynamically.
    // This prevents us from pruning the ops collections when we first publish
    // them in a new environment.
    for ops_collection in crate::publications::specs::get_ops_collection_names() {
        referenced_collections.insert(models::Collection::new(ops_collection));
    }
    for r in built.built_captures.iter() {
        if let Some(m) = r.model() {
            referenced_collections.extend(m.writes_to());
        }
    }
    for r in built.built_collections.iter() {
        if let Some(m) = r.model() {
            referenced_collections.extend(m.reads_from());
        }
    }
    for r in built.built_materializations.iter() {
        if let Some(m) = r.model() {
            referenced_collections.extend(m.reads_from());
        }
    }
    for r in built.built_tests.iter() {
        if let Some(m) = r.model() {
            referenced_collections.extend(m.writes_to());
            referenced_collections.extend(m.reads_from());
        }
    }

    // Remove any _new_ collections that are not referenced by any other specs
    // in this build.
    let prune_collections = built
        .built_collections
        .iter()
        .filter(|r| {
            r.is_insert()
                && !referenced_collections.contains(r.catalog_name())
                // derivations should never be pruned
                && !r.model().is_some_and(|m| m.derive.is_some())
        })
        .map(|r| r.catalog_name().clone())
        .collect::<BTreeSet<_>>();

    built
        .built_collections
        .retain(|r| !prune_collections.contains(r.catalog_name()));

    prune_collections
}
