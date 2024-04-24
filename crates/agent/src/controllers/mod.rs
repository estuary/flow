mod handler;
mod inferred_schema;
mod observe;
mod publication_status;

#[cfg(test)]
pub mod test_util;

pub use observe::observe_publication;

use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
};

// TODO: move PublicationResult into publications module
/// Represents a publication that is just completing.
#[derive(Debug)]
pub struct PublicationResult {
    pub completed_at: DateTime<Utc>,
    pub publication_id: models::Id,
    /// The draft that was just published
    pub draft: tables::DraftCatalog,
    /// The state of any related live_specs, prior to the draft being published
    pub live: tables::LiveCatalog,
    /// The build specifications that were output from a successful publication.
    /// Will be empty if the publication was not successful.
    pub validated: tables::Validations,
    /// The final status of the publication. Note that this is not neccessarily `Success`,
    /// even if there are no `errors`.
    pub publication_status: crate::publications::JobStatus,
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

impl ControllerState<Box<RawValue>> {
    fn deserialized<T: DeserializeOwned + Debug + Serialize + Clone>(
        self,
    ) -> anyhow::Result<ControllerState<T>> {
        self.map_status(|s| {
            serde_json::from_str(&self.status.get())
                .context("Failed to deserialize controller status")
        })
    }
}

impl<T: Debug + Serialize + DeserializeOwned + Clone> ControllerState<T> {
    fn serialized(self) -> anyhow::Result<ControllerState<Box<RawValue>>> {
        self.map_status(|s| {
            serde_json::value::to_raw_value(s).context("Failed to serialize controller status")
        })
    }
    fn map_status<R, F>(self, fun: F) -> anyhow::Result<ControllerState<R>>
    where
        F: FnOnce(&T) -> anyhow::Result<R>,
        R: DeserializeOwned + Debug + Serialize + Clone,
    {
        let status = fun(&self.status)?;
        Ok(ControllerState {
            active: self.active,
            next_run: self.next_run,
            updated_at: self.updated_at,
            status,
            failures: self.failures,
        })
    }

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
    pub fn new(status: T) -> Self {
        ControllerUpdate {
            active: true,
            next_run: None,
            status: Some(status),
        }
    }

    fn serialized(self) -> anyhow::Result<ControllerUpdate<Box<RawValue>>> {
        let status = if let Some(s) = &self.status {
            Some(
                serde_json::value::to_raw_value(s)
                    .context("Failed to serialize controller status")?,
            )
        } else {
            None
        };
        Ok(ControllerUpdate {
            active: self.active,
            next_run: self.next_run,
            status,
        })
    }

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
        current_states: BTreeMap<String, ControllerState<Self::Status>>,
        publication: &PublicationResult,
    ) -> anyhow::Result<BTreeMap<String, ControllerUpdate<Self::Status>>>;

    /// Invoked after the `next_run` time has passed. This function can query existing database rows and create publications.
    async fn update(
        &self,
        catalog_name: String,
        current_state: ControllerState<Self::Status>,
        txn: &mut dyn ControlPlane,
        //control_plane: &dyn ControlPlane,
    ) -> anyhow::Result<ControllerUpdate<Self::Status>>;
}

use serde_json::value::RawValue;

pub struct ControllerWrapper<C: ControlJob>(C);

#[async_trait::async_trait]
impl<C: ControlJob> ControlJob for ControllerWrapper<C>
where
    C: Send + Sync,
    //C::Status: Debug + Serialize + DeserializeOwned + Clone + Send,
{
    type Status = Box<RawValue>;

    fn controller_name(&self) -> String {
        self.0.controller_name()
    }

    fn observe_publication(
        &self,
        current_states: BTreeMap<String, ControllerState<Self::Status>>,
        publication: &PublicationResult,
    ) -> anyhow::Result<BTreeMap<String, ControllerUpdate<Self::Status>>> {
        let deserialized_states = current_states
            .into_iter()
            .map(|(k, v)| v.deserialized::<C::Status>().map(|s| (k, s)))
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        let updates = self
            .0
            .observe_publication(deserialized_states, publication)?;
        let ser_states = updates
            .into_iter()
            .map(|(k, v)| v.serialized().map(|s| (k, s)))
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        Ok(ser_states)
    }

    async fn update(
        &self,
        catalog_name: String,
        current_state: ControllerState<Self::Status>,
        txn: &mut dyn ControlPlane,
    ) -> anyhow::Result<ControllerUpdate<Self::Status>> {
        let deserialized_state = current_state.deserialized::<C::Status>()?;
        let update = self.0.update(catalog_name, deserialized_state, txn).await?;
        let ser_update = update.serialized()?;
        Ok(ser_update)
    }
}

const ALL_CONTROLLERS: &[&dyn ControlJob<Status = Box<RawValue>>] = &[&ControllerWrapper(
    inferred_schema::InferredSchemaController,
)];

/// A trait for allowing controllers access to the database.
/// This makes it much easier to test controllers, because we don't need to mock the state of the
/// whole database inside a transaction.
#[async_trait::async_trait]
pub trait ControlPlane: Send {
    /// Returns the current time. Having controllers access the current time through this api
    /// allows tests of controllers to be deterministic.
    fn current_time(&self) -> DateTime<Utc>;

    async fn get_collections(
        &mut self,
        names: BTreeSet<models::Collection>,
    ) -> anyhow::Result<tables::LiveCollections>;
    async fn get_captures(
        &mut self,
        names: BTreeSet<models::Capture>,
    ) -> anyhow::Result<tables::LiveCaptures>;
    async fn get_materializations(
        &mut self,
        names: BTreeSet<models::Materialization>,
    ) -> anyhow::Result<tables::LiveMaterializations>;
    async fn get_tests(
        &mut self,
        names: BTreeSet<models::Test>,
    ) -> anyhow::Result<tables::LiveTests>;

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

    /// Fetches the inferred schemas for the given `collections`. The set of returned schemas
    /// may be sparse, if some did not exist.
    async fn get_inferred_schemas(
        &mut self,
        collections: BTreeSet<models::Collection>,
    ) -> anyhow::Result<tables::InferredSchemas>;

    async fn create_publication(
        &mut self,
        draft: tables::DraftCatalog,
    ) -> anyhow::Result<models::Id>;

    async fn get_inferred_schema(
        &mut self,
        collection: models::Collection,
    ) -> anyhow::Result<Option<tables::InferredSchema>> {
        let table = self.get_inferred_schemas(set_of(collection)).await?;
        Ok(to_single_row(table))
    }

    async fn get_collection(
        &mut self,
        collection: models::Collection,
    ) -> anyhow::Result<Option<tables::LiveCollection>> {
        let table = self.get_collections(set_of(collection)).await?;
        Ok(to_single_row(table))
    }

    async fn get_capture(
        &mut self,
        capture: models::Capture,
    ) -> anyhow::Result<Option<tables::LiveCapture>> {
        let table = self.get_captures(set_of(capture)).await?;
        Ok(to_single_row(table))
    }

    async fn get_materialization(
        &mut self,
        materialization: models::Materialization,
    ) -> anyhow::Result<Option<tables::LiveMaterialization>> {
        let table = self.get_materializations(set_of(materialization)).await?;
        Ok(to_single_row(table))
    }

    async fn get_test(&mut self, test: models::Test) -> anyhow::Result<Option<tables::LiveTest>> {
        let table = self.get_tests(set_of(test)).await?;
        Ok(to_single_row(table))
    }
}

fn to_single_row<T: tables::Row>(table: tables::Table<T>) -> Option<T> {
    if table.len() == 1 {
        Some(table.into_iter().next().unwrap())
    } else {
        None
    }
}

fn set_of<T: Eq + Ord>(s: T) -> BTreeSet<T> {
    let mut set = BTreeSet::new();
    set.insert(s);
    set
}

fn jittered_next_run(base: Duration, add_multiplier: f64) -> DateTime<Utc> {
    use rand::Rng;

    let max_jitter = (base.num_seconds() as f64 * add_multiplier) as i64;
    let add_secs = rand::thread_rng().gen_range(0..=max_jitter);
    Utc::now() + (base + chrono::Duration::seconds(add_secs))
}
