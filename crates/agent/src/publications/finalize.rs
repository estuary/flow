use super::UncommittedBuild;
use std::collections::BTreeSet;

use models::ModelDef;
use tables::{BuiltRow, Validations};

/// Inspect, and potentially modify a completed build. If the build has any errors after this
/// function returns, then it will be considered a failed build.
///
/// Note that modifications to the build are _not_ persisted in the build database, and thus
/// for the time being it would be incorrect to modify built specs here. It is permissible to
/// remove built specs, though, since there's no harm in having extra specs in the build db.
/// This restriction may be lifted in the future if we stop using sqlite databases, or if we
/// refactor builds to apply `FinalizeBuild`s before persisting the sqlite database. For now,
/// it does not seem worth the effort, though.
pub trait FinalizeBuild {
    fn finalize(&self, build: &mut UncommittedBuild) -> anyhow::Result<()>;
}

/// A `FinalizeBuild` that doesn't modify the build in any way.
pub struct NoopFinalize;
impl FinalizeBuild for NoopFinalize {
    fn finalize(&self, _build: &mut UncommittedBuild) -> anyhow::Result<()> {
        Ok(())
    }
}

/// A `FinalizeBuild` that removes any built collection specs that are not referenced by other
/// specs in the build.
pub struct PruneUnboundCollections;
impl FinalizeBuild for PruneUnboundCollections {
    fn finalize(&self, build: &mut UncommittedBuild) -> anyhow::Result<()> {
        let pruned_collections = prune_unbound_collections(&mut build.output.built);
        if !pruned_collections.is_empty() {
            tracing::info!(
                ?pruned_collections,
                remaining_specs = %build.output.built.spec_count(),
                "pruned unbound collections from built catalog"
            );
        }
        Ok(())
    }
}

/// Prunes unbound collections from the build output. Collections are considered
/// unbound if they are new (updates to existing collections are never pruned),
/// and no other specs in the build read from or write to it.
fn prune_unbound_collections(built: &mut Validations) -> BTreeSet<models::Collection> {
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
            r.is_insert() // never prune an update or touch
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
