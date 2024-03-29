use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::Serialize;
use serde_json::Value;
use tables::Id;

use crate::publications;

use super::{ControlJob, ControlPlane, ControllerState, ControllerUpdate, PublicationResult};

pub fn id_of(id: &str) -> Id {
    id.parse().expect("invalid id str")
}

pub fn random_id() -> Id {
    let bytes = rand::thread_rng().next_u64().to_le_bytes();
    Id::new(bytes)
}

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
        collection_name: collection_name.to_owned(),
        schema,
        md5,
    }
}

pub fn md5_hash<T: serde::Serialize>(val: &T) -> String {
    let s = serde_json::to_string(val).unwrap();
    let bytes = md5::compute(s);
    format!("{bytes:x}")
}

pub fn redact_next_run() -> insta::internals::Redaction {
    use insta::internals::Content;

    insta::dynamic_redaction(|value, _| match value {
        Content::None => Content::String("redacted(None)".to_string()),
        Content::Some(_) => Content::String("redacted(Some)".to_string()),
        other => panic!("invalid next_run value: {other:?}"),
    })
}

pub fn draft_of(catalog_json: Value) -> tables::Catalog {
    let catalog: models::Catalog =
        serde_json::from_value(catalog_json).expect("failed to deserialize draft catalog");
    tables::Catalog::from(catalog)
}

fn participating_specs(live: &tables::Catalog, draft: tables::Catalog) -> tables::Catalog {
    use tables::SpecExt;

    // Start with all specs directly included in the draft, and then add any collections that are
    // read from or written to by spec in the draft.
    let mut all_named = draft.all_spec_names();
    for c in draft.captures.iter() {
        all_named.extend(c.drafted.as_ref().unwrap().writes_to());
    }
    for c in draft.collections.iter() {
        all_named.extend(c.drafted.as_ref().unwrap().reads_from());
    }
    for m in draft.materializations.iter() {
        all_named.extend(m.drafted.as_ref().unwrap().reads_from());
    }
    for t in draft.tests.iter() {
        all_named.extend(t.drafted.as_ref().unwrap().reads_from());
        all_named.extend(t.drafted.as_ref().unwrap().writes_to());
    }

    let mut filtered = live.get_named(&all_named);

    // Next get any specs that read from or write to one of the drafted collections
    let all_collections = draft
        .collections
        .iter()
        .map(|c| c.collection.to_string())
        .collect();

    // Combine the existing live specs with the expanded specs
    let related = live.related_tasks(&all_collections);
    filtered.merge(related);

    // Add in the draft specs, to result in rows where both draft and live specs may be present
    filtered.extend_draft(draft);
    filtered
}

#[derive(Serialize, Debug)]
pub struct TestPublication {
    #[serde(serialize_with = "serialize_draft_specs")]
    pub draft: tables::Catalog,
    // skip because status only indicates the desired status upon completion of the publication.
    // It has no relevance when verifying that a publication has been _created_ correctly.
    #[serde(skip)]
    pub status: publications::JobStatus,
    #[serde(rename = "publication_meta", skip_serializing_if = "Option::is_none")]
    control_plane: Option<(Id, DateTime<Utc>)>,
}

impl TestPublication {
    pub fn of_draft(draft: tables::Catalog) -> Self {
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

fn serialize_draft_specs<S>(specs: &tables::Catalog, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let cat = specs.draft_to_catalog();
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
pub struct UpdateInfo<C: ControlJob> {
    pub catalog_name: String,
    pub prev_state: ControllerState<C::Status>,
}

#[derive(Debug, Serialize)]
pub struct PublicationInfo<C: ControlJob> {
    pub id: Id,
    pub completed_at: DateTime<Utc>,
    pub draft: models::Catalog,
    pub live: models::Catalog,
    pub starting_states: BTreeMap<String, ControllerState<C::Status>>,
    pub publication_status: publications::JobStatus,
    #[serde(skip)]
    _phantom: std::marker::PhantomData<C>,
}

pub struct Harness<C: ControlJob> {
    controller: C,
    control_plane: MockControlPlane,

    last_pub_info: Option<PublicationInfo<C>>,

    states: BTreeMap<String, ControllerState<C::Status>>,
    rt: tokio::runtime::Runtime,
}

impl<C: ControlJob> Harness<C> {
    pub fn new(controller: C) -> Harness<C> {
        let time = "2024-01-01T08:00:00Z".parse().unwrap();
        Harness {
            controller,
            control_plane: MockControlPlane::new(time),
            last_pub_info: None,
            states: BTreeMap::new(),
            rt: tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
        }
    }

    pub fn last_pub_info(&self) -> &PublicationInfo<C> {
        self.last_pub_info
            .as_ref()
            .expect("no publication was observed")
    }

    pub fn update_inferred_schema(&mut self, collection_name: &str, schema_generation: usize) {
        self.control_plane
            .inferred_schemas
            .upsert_overwrite(mock_inferred_schema(collection_name, schema_generation))
    }

    pub fn observe_publication(
        &mut self,
        publication: TestPublication,
    ) -> (
        PublicationInfo<C>,
        BTreeMap<String, ControllerUpdate<C::Status>>,
    ) {
        let TestPublication {
            draft,
            status,
            control_plane,
        } = publication;

        let (pub_id, time) = control_plane.unwrap_or_else(|| self.control_plane.next_pub());

        let errors = match &status {
            publications::JobStatus::Queued => panic!("cannot observe Queued publication"),
            publications::JobStatus::Success { .. } => tables::Errors::new(),
            publications::JobStatus::EmptyDraft => tables::Errors::new(),
            other => {
                let err = tables::Error {
                    scope: "test://test.test/test".parse().unwrap(),
                    error: anyhow::anyhow!("oh no the publication failed: {other:?}"),
                };
                let mut t = tables::Errors::new();
                t.insert(err);
                t
            }
        };

        let publication_specs = participating_specs(&self.control_plane.live, draft);

        let inferred_schemas = tables::inner_join(
            publication_specs.collections.iter(),
            self.control_plane.inferred_schemas.iter(),
        )
        .map(|j| j.1.clone())
        .collect();

        // Convert the draft and live tables into `models::Catalog`s, so that they can be
        // directly serialized as part of PublicationInfo.
        let draft_catalog = publication_specs.draft_to_catalog();
        let live_catalog = publication_specs.live_to_catalog();

        // Determine the set of controller states that will observe this publication
        let filtered_states = publication_specs
            .all_spec_names()
            .into_iter()
            .flat_map(|name: String| self.states.get(&name).map(|s| (name, (*s).clone())))
            .collect::<BTreeMap<_, _>>();

        let result = PublicationResult {
            completed_at: time,
            publication_id: pub_id,
            catalog: publication_specs,
            inferred_schemas,
            errors,
            publication_status: status,
        };

        let updates = self
            .controller
            .observe_publication(&filtered_states, &result);
        self.apply_updates(&updates);
        self.control_plane.update_live_specs(result.catalog, pub_id);

        let pub_info = PublicationInfo {
            id: pub_id,
            completed_at: time,
            draft: draft_catalog,
            live: live_catalog,
            starting_states: filtered_states,
            publication_status: result.publication_status,
            _phantom: std::marker::PhantomData,
        };

        (pub_info, updates)
    }

    pub fn next_run(&self) -> Option<(&str, &ControllerState<C::Status>)> {
        self.states
            .iter()
            .filter(|(_, s)| s.next_run.is_some())
            .min_by_key(|(_, s)| s.next_run.unwrap())
            .map(|(n, s)| (n.as_str(), s))
    }

    pub fn next_run_update(
        &mut self,
    ) -> (
        UpdateInfo<C>,
        ControllerUpdate<C::Status>,
        Vec<TestPublication>,
    ) {
        let Some((next_name, next_state)) = self.next_run().map(|(n, s)| (n.to_owned(), s.clone()))
        else {
            panic!("no controller has a next_run in states: {:?}", self.states);
        };

        let state_copy = next_state.clone();

        let Harness {
            controller,
            control_plane,
            rt,
            ..
        } = self;

        // Jump our time forward so that it's at the next_run time.
        control_plane.time = next_state.next_run.unwrap();

        let update_result = rt.block_on(async {
            controller
                .update(next_name.clone(), state_copy.clone(), control_plane)
                .await
        });

        let update = update_result.unwrap_or_else(|err| {
            panic!(
                "update error processing '{next_name}' with state:\n{next_state:?}\nerror: {err:?}"
            )
        });
        let pubs = std::mem::take(&mut control_plane.publications);
        self.apply_state_update(next_name.as_str(), &update);
        let info = UpdateInfo {
            catalog_name: next_name.clone(),
            prev_state: state_copy,
        };
        (info, update, pubs)
    }

    fn apply_state_update(&mut self, name: &str, update: &ControllerUpdate<C::Status>) {
        let time = self.control_plane.time;
        // Compute the next run time without applying any jitter so that run times are stable.
        let next_run = update
            .next_run
            .as_ref()
            .map(|n| time + chrono::TimeDelta::seconds(n.after_seconds as i64));

        if let Some(state) = self.states.get_mut(name) {
            state.active = update.active;
            state.next_run = next_run;
            if let Some(new_status) = &update.status {
                state.status = (*new_status).clone();
            }
            state.updated_at = time;
        } else {
            let Some(status) = &update.status else {
                panic!("initial update for '{name}' is missing a status");
            };
            let state = ControllerState {
                active: update.active,
                next_run,
                updated_at: time,
                status: status.clone(),
                failures: 0,
            };
            self.states.insert(name.to_owned(), state);
        }
    }

    fn apply_updates(&mut self, updates: &BTreeMap<String, ControllerUpdate<C::Status>>) {
        let time = self.control_plane.time;
        for (name, update) in updates.iter() {
            self.apply_state_update(name.as_str(), update);
        }
    }
}

macro_rules! assert_update_snapshot {
    ($snapshot_name:expr, $info:expr, $value:expr) => {
        insta::with_settings!({ info => $info }, {
            insta::assert_json_snapshot!($snapshot_name, $value);
        })
    }
}
pub(crate) use assert_update_snapshot;

macro_rules! assert_observed_publication_snapshot {
    ($snapshot_name:expr, $pub_info:expr, $updates:expr) => {
        // let mut settings = insta::Settings::clone_current();
        // let info = $harness.last_pub_info();
        // settings.set_info(info);
        // let guard = settings.bind_to_scope();
        insta::with_settings!({ info => $pub_info }, {
            insta::assert_json_snapshot!($snapshot_name, $updates);
        })

        // insta::assert_json_snapshot!($snapshot_name, $updates, {
        //     ".*.next_run" => redact_next_run(),
        // });
        // std::mem::drop(guard);
    };
}
pub(crate) use assert_observed_publication_snapshot;

fn pub_id(counter: u8) -> Id {
    Id::new([counter, 0, 0, 0, 0, 0, 0, 0])
}

pub struct MockControlPlane {
    live: tables::Catalog,
    inferred_schemas: tables::InferredSchemas,

    publications: Vec<TestPublication>,
    time: DateTime<Utc>,
    pub_counter: u8,
}

impl MockControlPlane {
    pub fn new(time: DateTime<Utc>) -> MockControlPlane {
        MockControlPlane {
            live: Default::default(),
            inferred_schemas: Default::default(),
            publications: Default::default(),
            time,
            pub_counter: 0,
        }
    }

    fn update_live_specs(&mut self, mut draft: tables::Catalog, pub_id: Id) {
        let live = &mut self.live;
        for capture in draft.captures.iter_mut() {
            capture.last_pub_id = Some(pub_id);
            let drafted = capture.drafted.take();
            if drafted.is_some() {
                capture.live_spec = drafted;
            }
        }
        for collection in draft.collections.iter_mut() {
            collection.last_pub_id = Some(pub_id);
            let drafted = collection.drafted.take();
            if drafted.is_some() {
                collection.live_spec = drafted;
            }
        }
        for materialization in draft.materializations.iter_mut() {
            materialization.last_pub_id = Some(pub_id);
            let drafted = materialization.drafted.take();
            if drafted.is_some() {
                materialization.live_spec = drafted;
            }
        }
        for test in draft.tests.iter_mut() {
            test.last_pub_id = Some(pub_id);
            let drafted = test.drafted.take();
            if drafted.is_some() {
                test.live_spec = drafted;
            }
        }

        let tables::Catalog {
            captures,
            collections,
            materializations,
            tests,
        } = draft;
        live.captures.upsert_all(captures, |prev, next| {
            next.id = prev.id;
        });
        live.collections.upsert_all(collections, |prev, next| {
            next.id = prev.id;
            next.inferred_schema_md5 = prev.inferred_schema_md5.clone();
        });
        live.materializations
            .upsert_all(materializations, |prev, next| {
                next.id = prev.id;
            });
        live.tests.upsert_all(tests, |prev, next| {
            next.id = prev.id;
        });
    }

    fn next_pub(&mut self) -> (Id, DateTime<Utc>) {
        self.pub_counter += 1;
        self.time += chrono::Duration::minutes(1);

        (Id::new([self.pub_counter, 0, 0, 0, 0, 0, 0, 0]), self.time)
    }
}

#[async_trait::async_trait]
impl ControlPlane for MockControlPlane {
    fn current_time(&self) -> DateTime<Utc> {
        self.time
    }

    async fn get_live_specs(&mut self, names: BTreeSet<String>) -> anyhow::Result<tables::Catalog> {
        Ok(self.live.get_named(&names))
    }

    async fn get_inferred_schemas(
        &mut self,
        collections: BTreeSet<String>,
    ) -> anyhow::Result<tables::InferredSchemas> {
        let schemas = tables::inner_join(self.inferred_schemas.iter(), collections.iter())
            .map(|(s, _)| s.clone())
            .collect();
        Ok(schemas)
    }

    async fn create_publication(&mut self, draft: tables::Catalog) -> anyhow::Result<tables::Id> {
        // Assert that the draft is structurally valid
        for row in draft.captures.iter() {
            assert_valid_draft(row);
        }
        for row in draft.collections.iter() {
            assert_valid_draft(row);
        }
        for row in draft.materializations.iter() {
            assert_valid_draft(row);
        }
        for row in draft.tests.iter() {
            assert_valid_draft(row);
        }

        self.pub_counter += 1;
        let id = pub_id(self.pub_counter);
        self.publications.push(TestPublication {
            draft,
            status: publications::JobStatus::Success {
                linked_materialization_publications: Vec::new(),
            },
            control_plane: Some((id, self.current_time())),
        });
        Ok(id)
    }
}

fn assert_valid_draft<S, R: tables::SpecRow<S>>(row: &R) {
    assert!(
        row.get_draft_spec().is_some(),
        "missing draft spec in: {row:?}"
    );
    assert_eq!(
        row.get_expect_pub_id(),
        row.get_last_pub_id(),
        "expect_pub_id != last_pub_id in: {row:?}"
    );
    assert!(row.get_action().is_some(), "missing action in: {row:?}");
}
