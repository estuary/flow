use crate::publications::{FinalizeBuild, PruneUnboundCollections, UncommittedBuild};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

pub trait FailBuild: std::fmt::Debug + Send + 'static {
    fn modify(&mut self, result: &mut UncommittedBuild);
}

/// A `Finalize` that can inject build failures in order to test failure scenarios.
/// `FailBuild`s are applied based on matching catalog names in the publication.
#[derive(Clone)]
pub struct InjectBuildFailures(Arc<Mutex<BTreeMap<String, VecDeque<Box<dyn FailBuild>>>>>);
impl FinalizeBuild for InjectBuildFailures {
    fn finalize(&self, build: &mut UncommittedBuild) -> anyhow::Result<()> {
        let mut build_failures = self.0.lock().unwrap();
        for (catalog_name, modifications) in build_failures.iter_mut() {
            if !build
                .output
                .built
                .all_spec_names()
                .any(|name| name == catalog_name.as_str())
            {
                continue;
            }
            if let Some(mut failure) = modifications.pop_front() {
                // log just to make it easier to debug tests
                tracing::info!(publication_id = %build.publication_id, %catalog_name, ?failure, "modifing test publication");
                failure.modify(build);
            }
        }

        // This is necessary in order to match the behavior of
        // `PgControlPlane::publish`, which uses `PruneUnboundCollections` as
        // the finalizer.
        PruneUnboundCollections.finalize(build)
    }
}

impl InjectBuildFailures {
    pub fn inject_failure<F: FailBuild>(&self, catalog_name: &str, modify: F) {
        let mut build_failures = self.0.lock().unwrap();
        let modifications = build_failures.entry(catalog_name.to_string()).or_default();
        modifications.push_back(Box::new(modify));
    }
}

/// Returns a draft catalog for the given models::Catalog JSON.
pub fn draft_catalog(catalog_json: serde_json::Value) -> tables::DraftCatalog {
    let catalog: models::Catalog =
        serde_json::from_value(catalog_json).expect("failed to parse catalog");
    tables::DraftCatalog::from(catalog)
}
