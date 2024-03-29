mod handler;
mod inferred_schema;
mod publication_status;

#[cfg(test)]
pub mod test_util;

use chrono::{DateTime, Duration, Utc};
use itertools::EitherOrBoth;
use itertools::Itertools;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
};
use tables::AnySpec;

// TODO: move PublicationResult into publications module
/// Represents a publication that is just completing.
#[derive(Debug)]
pub struct PublicationResult {
    pub completed_at: DateTime<Utc>,
    pub publication_id: tables::Id,
    /// The draft that was just published
    pub draft: tables::DraftSpecs,
    /// The state of any related live_specs, prior to the draft being published
    pub live: tables::LiveSpecs,
    /// The inferred schemas that were resolved for the publication
    pub inferred_schemas: tables::InferredSchemas,
    /// Errors that occurred during the publication. If non-empty, then the
    /// `publication_status` will not be `Success`.
    pub errors: tables::Errors,
    /// The final status of the publication. Note that this is not neccessarily `Success`,
    /// even if there are no `errors`.
    pub publication_status: crate::publications::JobStatus,
}

impl PublicationResult {
    /// Returns a single `models::BaseCatalog` representing the full set of live specs
    pub fn to_base_catalog(&self) -> Option<models::BaseCatalog> {
        if !self.publication_status.is_success() {
            return None;
        }
        let mut catalog = self.live.to_catalog();
        catalog.merge(self.draft.to_catalog());
        Some(catalog)
    }

    // TODO: it might be better to convert into a `models::Catalog` and then define this sort of function for that type
    // pub fn to_final_catalog(&self) -> models::Catalog
    // TODO: this function assumes that the publication was successful
    pub fn consumers_of<'a, 'b: 'a>(
        &'a self,
        collection_name: &'b str,
    ) -> impl Iterator<Item = (&'a str, Option<AnySpec<'a>>, Option<AnySpec<'a>>)> + 'a {
        self.draft
            .consumers_of(collection_name)
            .merge_join_by(self.live.consumers_of(collection_name), |l, r| l.0.cmp(r.0))
            .map(move |eob| match eob {
                EitherOrBoth::Left((name, spec)) => (name, Some(spec), None),
                EitherOrBoth::Right((name, spec)) => (name, None, Some(spec)),
                EitherOrBoth::Both((name, drafted), (_, live)) => (name, Some(drafted), Some(live)),
            })
    }
}

/// Represents the state of a specific controller and catalog_name.
#[derive(Clone, Debug, Serialize)]
pub struct ControllerState<T: Debug + Serialize + DeserializeOwned + Clone> {
    pub active: bool,
    pub next_run: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub status: T,
    pub failures: i32,
}

impl<T: Debug + Serialize + DeserializeOwned + Clone> ControllerState<T> {
    pub fn to_update(&self) -> ControllerUpdate<T> {
        ControllerUpdate {
            active: self.active,
            next_run: None,
            status: None,
        }
    }
}

#[derive(Debug, Serialize, Clone, Copy)]
pub struct NextRun {
    pub after_seconds: u32,
    pub jitter_percent: u16,
}

impl NextRun {
    const DEFAULT_JITTER: u16 = 20;

    pub fn after_minutes(minutes: u32) -> NextRun {
        NextRun {
            after_seconds: minutes * 60,
            jitter_percent: NextRun::DEFAULT_JITTER,
        }
    }

    pub fn with_jitter_percent(self, jitter_percent: u16) -> Self {
        NextRun {
            after_seconds: self.after_seconds,
            jitter_percent,
        }
    }

    pub fn compute_time(&self) -> DateTime<Utc> {
        use rand::Rng;

        let mut delta_millis = self.after_seconds as i64 * 1000;

        let jitter_mul = self.jitter_percent as f64 / 100.0;
        let jitter_max = (delta_millis as f64 * jitter_mul) as i64;
        let jitter_add = rand::thread_rng().gen_range(0..jitter_max);
        let dur = chrono::TimeDelta::milliseconds(delta_millis + jitter_add);
        Utc::now() + dur
    }
}

/// Represents an update to the state of a controller for a given catalog entity.
#[derive(Debug, Serialize)]
pub struct ControllerUpdate<T: Debug + Serialize + DeserializeOwned + Clone> {
    /// Whether the controller is intended to be active or not. Inactive controllers
    /// will never have their `update` functions called (though `observe_publication` will be).
    pub active: bool,
    /// Time after which the controller's `update` function should be invoked.
    pub next_run: Option<NextRun>,
    /// Optional new status, which will entirely overwrite the old one, if `Some`.
    /// If `None`, then the existing status will be left as-is
    pub status: Option<T>,
}

impl<T: Debug + Serialize + DeserializeOwned + Clone> ControllerUpdate<T> {
    pub fn with_active(mut self, active: bool) -> Self {
        self.set_active(active);
        self
    }

    pub fn set_active(&mut self, active: bool) -> &mut Self {
        self.active = active;
        self
    }

    pub fn set_next_run(&mut self, next_run: Option<NextRun>) -> &mut Self {
        self.next_run = next_run;
        self
    }

    pub fn with_next_run(mut self, next_run: NextRun) -> Self {
        self.next_run = Some(next_run);
        self
    }

    pub fn with_status(mut self, status: T) -> Self {
        self.status = Some(status);
        self
    }

    pub fn set_status(&mut self, status: T) -> &mut Self {
        self.status = Some(status);
        self
    }
}

/// A `ControlJob` is a background controller of catalog specs, which can implement various types of
/// automation for catalog entities.
#[async_trait::async_trait]
pub trait ControlJob {
    type Status: Debug + Serialize + DeserializeOwned + Clone;

    /// Returns the name of the controller, which use used as part of the compound key for its state.
    /// One deployed, a `controller_name` should never be changed, or else old state will no longer
    /// be associated with it.
    fn controller_name(&self) -> String;

    /// Observe the results of a recent publication, and return a sparse map of updates to controller states.
    fn observe_publication(
        &self,
        current_states: &BTreeMap<String, ControllerState<Self::Status>>,
        publication: &PublicationResult,
    ) -> BTreeMap<String, ControllerUpdate<Self::Status>>;

    /// Invoked after the `next_run` time has passed. This function can query existing database rows and create publications.
    async fn update(
        &self,
        catalog_name: String,
        current_state: ControllerState<Self::Status>,
        txn: &mut dyn ControlPlane,
        //control_plane: &dyn ControlPlane,
    ) -> anyhow::Result<ControllerUpdate<Self::Status>>;
}

/// A provisional trait for allowing Controllers access to the database.
/// It's not clear whether we really want this, vs just passing controllers a handle to an open transaction.
#[async_trait::async_trait]
pub trait ControlPlane: Send {
    fn current_time(&self) -> DateTime<Utc>;

    async fn get_live_spec(&mut self, name: &str) -> anyhow::Result<tables::LiveSpecs> {
        let spec = self.get_live_specs(set_of(name)).await?;
        if spec.is_empty() {
            anyhow::bail!("no live spec found with name '{name}'");
        }
        Ok(spec)
    }

    async fn get_live_specs(
        &mut self,
        names: BTreeSet<String>,
    ) -> anyhow::Result<tables::LiveSpecs>;

    // async fn get_live_specs_consuming(
    //     &mut self,
    //     collection_names: Vec<String>,
    // ) -> anyhow::Result<tables::LiveSpecs>;

    // async fn get_live_specs_producing(
    //     &mut self,
    //     collection_names: Vec<String>,
    // ) -> anyhow::Result<tables::LiveSpecs>;

    // async fn get_linked_materializations(
    //     &mut self,
    //     capture_names: Vec<String>,
    // ) -> anyhow::Result<tables::LiveMaterializations>;

    async fn get_inferred_schema(
        &mut self,
        collection: &str,
    ) -> anyhow::Result<Option<tables::InferredSchema>> {
        let mut schemas = self.get_inferred_schemas(set_of(collection)).await?;
        Ok(schemas.pop())
    }

    async fn get_inferred_schemas(
        &mut self,
        collections: BTreeSet<String>,
    ) -> anyhow::Result<tables::InferredSchemas>;

    async fn create_publication(
        &mut self,
        draft: tables::DraftSpecs,
        dry_run: bool,
    ) -> anyhow::Result<tables::Id>;
}

fn set_of(s: &str) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    set.insert(s.to_owned());
    set
}

fn jittered_next_run(base: Duration, add_multiplier: f64) -> DateTime<Utc> {
    use rand::Rng;

    let max_jitter = (base.num_seconds() as f64 * add_multiplier) as i64;
    let add_secs = rand::thread_rng().gen_range(0..=max_jitter);
    Utc::now() + (base + chrono::Duration::seconds(add_secs))
}
