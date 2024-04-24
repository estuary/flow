use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use crate::controlplane::ConnectorSpec;
use crate::publications::PublicationResult;
use chrono::{DateTime, Utc};
use models::Id;
use serde::Serialize;
use serde_json::Value;
use sqlx::types::Uuid;
use tables::{
    DraftCapture, DraftCatalog, DraftCollection, DraftMaterialization, DraftTest, EitherOrBoth,
};

use crate::publications;

use super::{ControlPlane, ControllerState};

fn mock_inferred_schema(collection_name: &str, num_properties: usize) -> tables::InferredSchema {
    let properties = (0..num_properties)
        .into_iter()
        .map(|i| (format!("p{i}"), serde_json::json!({"type": "string"})))
        .collect::<serde_json::Map<_, _>>();
    let schema: models::Schema = serde_json::from_value(serde_json::json!({
        "type": "object",
        "properties": properties,
    }))
    .unwrap();
    let md5 = md5_hash(&schema);
    tables::InferredSchema {
        collection_name: models::Collection::new(collection_name),
        schema,
        md5,
    }
}

pub fn md5_hash<T: serde::Serialize>(val: &T) -> String {
    let s = serde_json::to_string(val).unwrap();
    let bytes = md5::compute(s);
    format!("{bytes:x}")
}

/// Updates `live` to reflect the successful publication of `draft`, which produced `built`.
fn update_live_specs(
    live: &mut tables::LiveCatalog,
    draft: tables::DraftCatalog,
    built: tables::Validations,
    pub_id: Id,
    id_gen: &mut TestIdGenerator,
) {
    todo!()
    // let tables::DraftCatalog {
    //     captures,
    //     collections,
    //     materializations,
    //     tests,
    //     // TODO: ensure no other crap was passed as part of the draft catalog
    //     ..
    // } = draft;
    // for row in captures.into_iter() {
    //     let DraftCapture {
    //         scope,
    //         capture,
    //         model,
    //         ..
    //     } = row;

    //     let model = model.expect("deletions are not yet supported in tests");
    //     let new_live = tables::LiveCapture {
    //         scope,
    //         last_pub_id: pub_id,
    //         capture,
    //         model,
    //         spec: Default::default(),
    //     };
    //     live.captures.upsert_overwrite(new_live);
    // }
    // for row in collections.into_iter() {
    //     let DraftCollection {
    //         collection,
    //         model,
    //         scope,
    //         ..
    //     } = row;
    //     let model = model.expect("deletions are not yet supported in tests");
    //     let new_live = tables::LiveCollection {
    //         scope,
    //         collection,
    //         model,
    //         last_pub_id: pub_id,
    //         spec: Default::default(),
    //     };
    //     live.collections.upsert_overwrite(new_live);
    // }
    // for row in materializations.into_iter() {
    //     let DraftMaterialization {
    //         materialization,
    //         model,
    //         scope,
    //         ..
    //     } = row;
    //     let model = model.expect("deletions are not yet supported in tests");
    //     let new_live = tables::LiveMaterialization {
    //         last_pub_id: pub_id,
    //         materialization,
    //         scope,
    //         model,
    //         spec: Default::default(),
    //     };
    //     live.materializations.upsert_overwrite(new_live);
    // }
    // for row in tests.into_iter() {
    //     let DraftTest {
    //         test, model, scope, ..
    //     } = row;
    //     let model = model.expect("deletions are not yet supported in tests");
    //     let new_live = tables::LiveTest {
    //         last_pub_id: pub_id,
    //         test,
    //         model,
    //         scope,
    //         spec: Default::default(),
    //     };
    //     live.tests.upsert_overwrite(new_live);
    // }

    // let tables::Validations {
    //     built_captures,
    //     built_collections,
    //     built_materializations,
    //     built_tests,
    //     ..
    // } = built;

    // let _ = live
    //     .captures
    //     .outer_join_mut(
    //         built_captures.into_iter().map(|r| (r.capture.clone(), r)),
    //         |eob| match eob {
    //             EitherOrBoth::Both(live, (_, built)) => {
    //                 live.spec = built.spec;
    //                 Some(1)
    //             }
    //             _ => None,
    //         },
    //     )
    //     .sum::<i32>();
    // let _ = live
    //     .collections
    //     .outer_join_mut(
    //         built_collections
    //             .into_iter()
    //             .map(|r| (r.collection.clone(), r)),
    //         |eob| match eob {
    //             EitherOrBoth::Both(live, (_, built)) => {
    //                 live.spec = built.spec;
    //                 Some(1)
    //             }
    //             _ => None,
    //         },
    //     )
    //     .sum::<i32>();
    // let _ = live
    //     .materializations
    //     .outer_join_mut(
    //         built_materializations
    //             .into_iter()
    //             .map(|r| (r.materialization.clone(), r)),
    //         |eob| match eob {
    //             EitherOrBoth::Both(live, (_, built)) => {
    //                 live.spec = built.spec;
    //                 Some(1)
    //             }
    //             _ => None,
    //         },
    //     )
    //     .sum::<i32>();
    // let _ = live
    //     .tests
    //     .outer_join_mut(
    //         built_tests.into_iter().map(|r| (r.test.clone(), r)),
    //         |eob| match eob {
    //             EitherOrBoth::Both(live, (_, built)) => {
    //                 live.spec = built.spec;
    //                 Some(1)
    //             }
    //             _ => None,
    //         },
    //     )
    //     .sum::<i32>();
}

/// Returns a new `DraftCatalog` from the given json representing a `models::Catalog`.
pub fn draft_of(catalog_json: Value) -> tables::DraftCatalog {
    let catalog: models::Catalog =
        serde_json::from_value(catalog_json).expect("failed to deserialize draft catalog");
    tables::DraftCatalog::from(catalog)
}

fn expanded_catalog(
    live: &tables::LiveCatalog,
    draft: &tables::DraftCatalog,
) -> tables::LiveCatalog {
    use tables::SpecExt;

    // let mut captures: tables::LiveCaptures = live
    //     .captures
    //     .inner_join(
    //         draft.captures.iter().map(|c| (&c.catalog_name, c)),
    //         |r, _, _| Some(r.clone()),
    //     )
    //     .collect();
    // let mut collections: tables::LiveCollections = live
    //     .collections
    //     .inner_join(
    //         draft.collections.iter().map(|c| (&c.catalog_name, c)),
    //         |r, _, _| Some(r.clone()),
    //     )
    //     .collect();
    // let mut materializations: tables::LiveMaterializations = live
    //     .materializations
    //     .inner_join(
    //         draft.materializations.iter().map(|c| (&c.catalog_name, c)),
    //         |r, _, _| Some(r.clone()),
    //     )
    //     .collect();
    // let mut tests: tables::LiveTests = live
    //     .tests
    //     .inner_join(
    //         draft.tests.iter().map(|c| (&c.catalog_name, c)),
    //         |r, _, _| Some(r.clone()),
    //     )
    //     .collect();

    // // Next get any specs that read from or write to one of the drafted collections
    // let all_collections = draft
    //     .collections
    //     .iter()
    //     .map(|c| c.catalog_name.clone())
    //     .collect();

    // for capture in live.captures.iter() {
    //     if capture.spec.uses_any(&all_collections) {
    //         captures.insert(capture.clone());
    //     }
    // }
    // for collection in live.collections.iter() {
    //     if collection.spec.uses_any(&all_collections) {
    //         collections.insert(collection.clone());
    //     }
    // }
    // for materialization in live.materializations.iter() {
    //     if materialization.spec.uses_any(&all_collections) {
    //         materializations.insert(materialization.clone());
    //     }
    // }
    // for test in live.tests.iter() {
    //     if test.spec.uses_any(&all_collections) {
    //         tests.insert(test.clone());
    //     }
    // }

    // // Now that we've expanded the specs, we can know which inferred schemas we need
    // let inferred_schemas = collections
    //     .inner_join(
    //         live.inferred_schemas
    //             .iter()
    //             .map(|s| (&s.collection_name, s)),
    //         |_, _, s| Some(s.clone()),
    //     )
    //     .collect();

    // tables::LiveCatalog {
    //     captures,
    //     collections,
    //     materializations,
    //     tests,
    //     inferred_schemas,
    //     storage_mappings: Default::default(),
    //     errors: Default::default(),
    // }
    todo!()
}

#[derive(Debug, Serialize)]
pub struct TestPublication {
    /// The draft that was published. When serializing, this is represented as a `models::Catalog`.
    /// TODO: consider preserving `expect_build_id` when serializing.
    #[serde(serialize_with = "serialize_draft_specs")]
    pub draft: tables::DraftCatalog,
    /// The status of a TestPublication determines the outcome that will be obvserved when
    /// simulating the completion of the publication. This allows testing observation of failed
    /// publications. We ignore the status when serializing because it makes no sense to include
    /// it in test snapshots.
    #[serde(skip)]
    pub status: publications::JobStatus,
    /// Metadata about the publication, which is added by MockControlPlane.
    /// This is useful in test snapshots, but it is not required to be present.
    #[serde(rename = "publication_meta", skip_serializing_if = "Option::is_none")]
    control_plane: Option<(Id, DateTime<Utc>)>,
}

impl TestPublication {
    pub fn of_draft(draft: tables::DraftCatalog) -> Self {
        TestPublication {
            draft,
            status: publications::JobStatus::Success {
                linked_materialization_publications: Vec::new(),
            },
            control_plane: None,
        }
    }

    pub fn with_status(mut self, status: publications::JobStatus) -> Self {
        self.status = status;
        self
    }
}

fn serialize_draft_specs<S>(specs: &DraftCatalog, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let cat = specs.to_models_catalog();
    cat.serialize(ser)
}

impl Default for TestPublication {
    fn default() -> Self {
        TestPublication {
            draft: Default::default(),
            status: publications::JobStatus::Success {
                linked_materialization_publications: Vec::new(),
            },
            control_plane: None,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct UpdateInfo {
    pub catalog_name: String,
    pub prev_state: ControllerState,
}

#[derive(Debug, Serialize)]
pub struct PublicationInfo {
    pub id: Id,
    pub completed_at: DateTime<Utc>,
    pub draft: models::Catalog,
    pub starting_state: ControllerState,
    pub publication_status: publications::JobStatus,
}

/// A testing harness for writing tests of a `ControlJob`, which simulates the necessary operations
/// of a production environment. Tests are written primarily in terms of calls to
/// `observe_publication` and `next_run_update`. The harness maintains persistent (for the life of
/// the harness) states for each controller, and will keep them up to date as the controller emits
/// updates to the state.
///
/// The harness also maintains a set of live specs, and simulates updates to them as publication
/// completions are observed. This frees the tests from needing to mock out existing specs or
/// publication spec expansion. Spec expansion is technically slightly more minimal than what's
/// done in production currently. This is because we intend to tighten spec expansion in the future,
/// and the more minimal expansion here will ensure that controllers don't rely on any "extra"
/// expansion that's done currently.
///
/// The harness also completely manages the current time, as observed through `ControlPlane::current_time`
/// and the timestamps associated with publications. This allows tests and snapshots to be completely
/// deterministic, provided the controller uses no other time source.
pub struct Harness {
    control_plane: MockControlPlane,

    states: BTreeMap<String, ControllerState>,
    rt: tokio::runtime::Runtime,
}

impl Harness {
    pub fn new() -> Harness {
        // Arbitrary start time, but making it consistent helps tests be more readable
        let time = "2024-01-01T08:00:00Z".parse().unwrap();
        Harness {
            control_plane: MockControlPlane::new(time),
            states: BTreeMap::new(),
            rt: tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
        }
    }

    /// Simulates the background update of the inferred schema for the given `collection_name`.
    /// To simplify testing, the `schema_generation` directly corresponds to the number of properties
    /// in the inferred schema. Each generation corresponds to a deterministic md5 hash.
    pub fn update_inferred_schema(&mut self, collection_name: &str, schema_generation: usize) {
        self.control_plane.live.inferred_schemas.upsert(
            mock_inferred_schema(collection_name, schema_generation),
            |_, _| {},
        );
    }

    // Simulate the completion of the given `publication`. This includes having the controller
    // observe the publication and produce updates, which are automatically merged into the
    // persistent state of each controller. Returns a tuple of:
    // - A `PublicationInfo` struct with additional information about the publication, which is
    //   especially helpful as contextual information attached to `insta` snapshots.
    // - The map of updates that was returned by the `ControlJob` under test. These updates will
    //   have already been applied to the persistent `ControllerState`s managed by the harness.

    // The given `publication` can be either one that was created by the controller itself (which
    // would be returned by `next_run_update`), or one that simulates an out-of-band publication
    // that was created some other way.
    // pub fn observe_publication(
    //     &mut self,
    //     publication: TestPublication,
    // ) -> (
    //     PublicationInfo<C>,
    //     BTreeMap<String, ControllerUpdate<C::Status>>,
    // ) {
    //     let TestPublication {
    //         mut draft,
    //         status,
    //         control_plane,
    //     } = publication;

    //     let (pub_id, time) = control_plane.unwrap_or_else(|| self.control_plane.next_pub());

    //     // If we're simulating a failed publication, ensure that there's at least on error.
    //     match &status {
    //         publications::JobStatus::Queued => panic!("cannot observe Queued publication"),
    //         publications::JobStatus::Success { .. } => {}
    //         publications::JobStatus::EmptyDraft => {}
    //         other => {
    //             let err = tables::Error {
    //                 scope: "test://test.test/test".parse().unwrap(),
    //                 error: anyhow::anyhow!("oh no the publication failed: {other:?}"),
    //             };
    //             draft.errors.insert(err);
    //         }
    //     };

    //     // Determine the subset of live specs that should be part of the publication.
    //     let live = expanded_catalog(&self.control_plane.live, &draft);

    //     // Determine the set of controller states that will observe this publication
    //     let filtered_states = live
    //         .all_spec_names()
    //         .flat_map(|name: &'_ str| {
    //             self.states
    //                 .get(name)
    //                 .map(|s| (name.to_string(), (*s).clone()))
    //         })
    //         .collect::<BTreeMap<_, _>>();

    //     let draft_cat = draft.to_models_catalog();

    //     // TODO: call validation crate to get built catalog
    //     let built_catalog = tables::Validations::default();
    //     let result = PublicationResult {
    //         completed_at: time,
    //         publication_id: pub_id,
    //         draft,
    //         live,
    //         validated: built_catalog,
    //         publication_status: status,
    //     };

    //     let updates = self
    //         .controller
    //         .observe_publication(filtered_states, &result)
    //         .expect("observe_publication failed");
    //     // Update the persistent controller states based on the updates.
    //     self.apply_updates(&updates);
    //     // Update all the live specs to reflect the drafted changes.

    //     self.control_plane
    //         .update_live_specs(result.draft, result.validated, pub_id);

    //     let pub_info = PublicationInfo {
    //         id: pub_id,
    //         completed_at: time,
    //         draft: draft_cat,
    //         starting_states: filtered_states,
    //         publication_status: result.publication_status,
    //         _phantom: std::marker::PhantomData,
    //     };

    //     (pub_info, updates)
    // }

    // Returns a description of the next controller that would be run if `next_run_update` were
    // called. Returns `None` if no controllers have a `next_run` value set.
    // pub fn next_run(&self) -> Option<(&str, &ControllerState<C::Status>)> {
    //     self.states
    //         .iter()
    //         .filter(|(_, s)| s.next_run.is_some())
    //         .min_by_key(|(_, s)| s.next_run.unwrap())
    //         .map(|(n, s)| (n.as_str(), s))
    // }

    // Jumps time forward to that of the smalles `next_run` of any controller state, and invokes
    // the `update` function for that controller. Returns a tuple of:
    // - An `UpdateInfo` struct with additional information about the invocation, which is
    //   especially helpful as contextual information attached to `insta` snapshots.
    // - The actual `ControllerUpdate` that was returned by the controller. This will have already
    //   been applied to the persistent state maintained by the harness.
    // - A vector of publications that were created by the controller as part of this update. These
    //   publications are still considered "pending" and will not have updated any live specs. In
    //   order to simulate the completion of the publications, you must call `observe_publication`
    //   for each one. Note that you have the opportunity to set the publication status before that
    //   point, in order to simulate failed publications.
    // pub fn next_run_update(
    //     &mut self,
    // ) -> (
    //     UpdateInfo<C>,
    //     ControllerUpdate<C::Status>,
    //     Vec<TestPublication>,
    // ) {
    //     let Some((next_name, next_state)) = self.next_run().map(|(n, s)| (n.to_owned(), s.clone()))
    //     else {
    //         panic!("no controller has a next_run in states: {:?}", self.states);
    //     };

    //     let state_copy = next_state.clone();

    //     let Harness {
    //         controller,
    //         control_plane,
    //         rt,
    //         ..
    //     } = self;

    //     // Jump our time forward so that it's at the next_run time.
    //     control_plane.time = next_state.next_run.unwrap();

    //     let update_result = rt.block_on(async {
    //         controller
    //             .update(next_name.clone(), state_copy.clone(), control_plane)
    //             .await
    //     });

    //     let update = update_result.unwrap_or_else(|err| {
    //         panic!(
    //             "update error processing '{next_name}' with state:\n{next_state:?}\nerror: {err:?}"
    //         )
    //     });
    //     let pubs = std::mem::take(&mut control_plane.publications);
    //     self.apply_state_update(next_name.as_str(), &update);
    //     let info = UpdateInfo {
    //         catalog_name: next_name.clone(),
    //         prev_state: state_copy,
    //     };
    //     (info, update, pubs)
    // }

    // fn apply_state_update(&mut self, name: &str, update: &ControllerUpdate<C::Status>) {
    //     let time = self.control_plane.time;
    //     // Compute the next run time without applying any jitter so that run times are stable.
    //     let next_run = update
    //         .next_run
    //         .as_ref()
    //         .map(|n| time + chrono::TimeDelta::seconds(n.after_seconds as i64));

    //     if let Some(state) = self.states.get_mut(name) {
    //         state.active = update.active;
    //         state.next_run = next_run;
    //         if let Some(new_status) = &update.status {
    //             state.status = (*new_status).clone();
    //         }
    //         state.updated_at = time;
    //     } else {
    //         let Some(status) = &update.status else {
    //             panic!("initial update for '{name}' is missing a status");
    //         };
    //         let state = ControllerState {
    //             catalog_name: todo!(),
    //             live_spec: todo!(),
    //             next_run,
    //             updated_at: todo!(),
    //             failures: todo!(),
    //             errror: todo!(),
    //             last_pub_id: todo!(),
    //             logs_token: todo!(),
    //             controller_version: todo!(),
    //             current_status: todo!(),
    //         };
    //         self.states.insert(name.to_owned(), state);
    //     }
    // }

    // fn apply_updates(&mut self, updates: &BTreeMap<String, ControllerUpdate<C::Status>>) {
    //     for (name, update) in updates.iter() {
    //         self.apply_state_update(name.as_str(), update);
    //     }
    // }
}

/// Used by the `Harness` to simulate interactions with the control plane database.
pub struct MockControlPlane {
    live: tables::LiveCatalog,

    publications: Vec<TestPublication>,
    /// The current time point for the test. This is moved forward deterministically, so that
    /// tests and snapshots can rely on deterministic timestamps.
    time: DateTime<Utc>,
    id_gen: TestIdGenerator,
}

pub struct TestIdGenerator(u64);
impl TestIdGenerator {
    pub fn new(starting: u64) -> TestIdGenerator {
        TestIdGenerator(starting)
    }

    pub fn next_id(&mut self) -> Id {
        self.0 += 1;
        Id::new(self.0.to_be_bytes())
    }
}

impl MockControlPlane {
    pub fn new(time: DateTime<Utc>) -> MockControlPlane {
        MockControlPlane {
            live: Default::default(),
            publications: Default::default(),
            time,
            id_gen: TestIdGenerator::new(0),
        }
    }

    fn update_live_specs(
        &mut self,
        draft: tables::DraftCatalog,
        built: tables::Validations,
        pub_id: Id,
    ) {
        let MockControlPlane { live, id_gen, .. } = self;
        update_live_specs(live, draft, built, pub_id, id_gen)
    }

    fn next_pub(&mut self) -> (Id, DateTime<Utc>) {
        (self.id_gen.next_id(), self.time)
    }
}

#[async_trait::async_trait]
impl ControlPlane for MockControlPlane {
    fn current_time(&self) -> DateTime<Utc> {
        self.time
    }

    fn next_pub_id(&mut self) -> Id {
        self.id_gen.next_id()
    }

    async fn publish(
        &mut self,
        pub_id: Id,
        detail: Option<String>,
        logs_token: Uuid,
        draft: tables::DraftCatalog,
    ) -> anyhow::Result<PublicationResult> {
        todo!()
    }

    async fn notify_dependents(
        &mut self,
        catalog_name: String,
        publication_id: Id,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_connector_spec(&mut self, image: String) -> anyhow::Result<ConnectorSpec> {
        todo!()
    }

    async fn get_live_specs(
        &mut self,
        names: BTreeSet<String>,
    ) -> anyhow::Result<tables::LiveCatalog> {
        // let captures = self
        //     .live
        //     .captures
        //     .inner_join(names.iter().map(|c| (c, ())), |c, _, _| Some(c.clone()))
        //     .collect();
        // let collections = self
        //     .live
        //     .collections
        //     .inner_join(names.iter().map(|c| (c, ())), |c, _, _| Some(c.clone()))
        //     .collect();
        // let inferred_schemas = self
        //     .live
        //     .inferred_schemas
        //     .inner_join(names.iter().map(|s| (s, ())), |s, _, _| Some(s.clone()))
        //     .collect();
        // let materializations = self
        //     .live
        //     .materializations
        //     .inner_join(names.iter().map(|m| (m, ())), |m, _, _| Some(m.clone()))
        //     .collect();
        // let tests = self
        //     .live
        //     .tests
        //     .inner_join(names.iter().map(|t| (t, ())), |t, _, _| Some(t.clone()))
        //     .collect();
        // Ok(tables::LiveCatalog {
        //     captures,
        //     collections,
        //     materializations,
        //     tests,
        //     inferred_schemas,
        //     storage_mappings: Default::default(),
        // })
        todo!()
    }
}
