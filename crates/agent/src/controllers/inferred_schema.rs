use std::collections::{BTreeMap, BTreeSet};

use crate::controllers::publication_status::{PublicationHistory, PublicationStatus};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use super::{
    jittered_next_run, ControlJob, ControlPlane, ControllerState, ControllerUpdate, NextRun,
    PublicationResult,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InferredSchemaStatus {
    pub schema_last_updated: DateTime<Utc>,
    pub schema_md5: Option<String>,
    pub consumers: BTreeMap<String, ConsumerStatus>,
    pub publications: PublicationHistory,
}

impl InferredSchemaStatus {
    fn next_run(&self, now: DateTime<Utc>) -> NextRun {
        // The idea here is to check frequently if there isn't an inferred schema at all yet,
        // so we can quickly start materializing some data. But after it works at least once,
        // then we want to use a longer duration in order to coalesce more schema updates into
        // each publication to prevent undue churn.
        // TODO: we might want to account for `last_backfill` times here
        let min_backoff_minutes = if self.schema_md5.is_none() || self.consumers_need_updated() {
            1i64
        } else {
            10i64
        };

        let after_minutes = now
            .signed_duration_since(self.schema_last_updated)
            .num_minutes()
            .max(min_backoff_minutes)
            .min(90);
        NextRun::after_minutes(after_minutes as u32).with_jitter_percent(25)
    }

    fn initial(
        collection_name: &str,
        schema_md5: Option<String>,
        publication: &PublicationResult,
    ) -> InferredSchemaStatus {
        let mut consumers = BTreeMap::new();
        let pub_status = PublicationStatus::observed(publication);
        for (consumer_name, _, _) in publication.consumers_of(collection_name) {
            consumers.insert(
                consumer_name.to_string(),
                ConsumerStatus::new(schema_md5.clone(), pub_status.clone()),
            );
        }
        InferredSchemaStatus {
            schema_last_updated: publication.completed_at,
            schema_md5,
            consumers,
            publications: PublicationHistory::initial(pub_status),
        }
    }

    fn consumers_need_updated(&self) -> bool {
        self.consumers
            .values()
            .any(|c| c.applied_schema_md5 != self.schema_md5)
    }

    fn is_publication_pending(&self) -> bool {
        self.publications.pending.is_some()
    }

    /// Updates the status of all consumers of the collection to reflect that they're using the latest inferred schema.
    /// This works because if the collection itself is published, then all consumers of it must also be included in the
    /// publication.
    fn on_successful_collection_publication(
        &self,
        collection_name: &str,
        schema_md5: Option<String>,
        publication: &PublicationResult,
    ) -> Self {
        let mut new_status = self.clone();
        new_status.schema_md5 = schema_md5.clone();

        let pub_status = PublicationStatus::observed(publication);
        let _completed = new_status.publications.observe(pub_status.clone());

        for (consumer_name, _, _) in publication.consumers_of(collection_name) {
            // Is there already a status for this consumer of the collection?
            // If not, then it means the consumer just now started reading the collection,
            // so we'll need to initialize a new status to track it.
            if let Some(consumer_status) = new_status.consumers.get_mut(consumer_name) {
                // TODO: check if spec is being deleted or added
                consumer_status.publication_complete(pub_status.clone(), schema_md5.clone());
            } else {
                new_status.consumers.insert(
                    consumer_name.to_string(),
                    ConsumerStatus::new(schema_md5.clone(), pub_status.clone()),
                );
            }
        }
        // Remove any consumer statuses for specs that were deleted by this publication
        for del in publication.draft.deletions.iter() {
            new_status.consumers.remove(&del.catalog_name);
        }
        new_status
    }

    fn on_failed_publication(
        &self,
        collection_name: &str,
        publication: &PublicationResult,
    ) -> Self {
        let mut new_status = self.clone();
        let schema_md5 = get_inferred_schema_md5(collection_name, publication);
        // We can still update the last known schema md5, if known.
        if schema_md5.is_some() && schema_md5 != new_status.schema_md5 {
            new_status.schema_md5 = schema_md5.clone();
            new_status.schema_last_updated = publication.completed_at;
        }

        let completed = new_status
            .publications
            .observe(PublicationStatus::observed(publication))
            .expect("publication must be pending");

        if let Some(affected_consumers) =
            completed.is_incompatible_collection_error(collection_name)
        {
            //
            for affected in affected_consumers {
                if let Some(consumer_status) = new_status.consumers.get_mut(&affected.name) {
                    consumer_status.needs_backfill = true;
                } else {
                    // Generally, we should always have an initialized ConsumerState already if we're seeing
                    // a failed publication involving it. But it's possible that this is the first we're seeing
                    // this consumer, given that it may have been created prior to this controller, so initialize
                    // a new state for it here.
                    let mut cs = ConsumerStatus::new(schema_md5.clone(), completed.clone());
                    cs.needs_backfill = true;
                    new_status.consumers.insert(affected.name.clone(), cs);
                }
            }
        }
        new_status
    }
}

fn get_inferred_schema_md5(collection: &str, publication: &PublicationResult) -> Option<String> {
    publication
        .inferred_schemas
        .iter()
        .find(|p| p.collection_name == collection)
        .map(|s| s.md5.clone())
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ConsumerStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_backfill: Option<PublicationStatus>,
    pub applied_schema_md5: Option<String>,
    #[serde(skip_serializing_if = "is_false")]
    pub needs_backfill: bool,
    pub last_publication: PublicationStatus,
}

impl ConsumerStatus {
    fn new(
        applied_schema_md5: Option<String>,
        last_publication: PublicationStatus,
    ) -> ConsumerStatus {
        ConsumerStatus {
            applied_schema_md5,
            last_publication,
            last_backfill: None,
            needs_backfill: false,
        }
    }

    fn publication_complete(&mut self, pub_status: PublicationStatus, schema_md5: Option<String>) {
        if let Some(bf_pub) = self.last_backfill.as_mut() {
            if bf_pub.id == pub_status.id {
                bf_pub.completed = pub_status.completed;
                bf_pub.result = pub_status.result.clone();
            }
        }
        if pub_status.is_success() {
            self.applied_schema_md5 = schema_md5;
            self.needs_backfill = false;
        }
        self.last_publication = pub_status;
    }
}

pub struct InferredSchemaController;

#[async_trait::async_trait]
impl ControlJob for InferredSchemaController {
    type Status = InferredSchemaStatus;

    fn controller_name(&self) -> String {
        String::from("InferredSchemaPublisher")
    }

    fn observe_publication(
        &self,
        current_states: &BTreeMap<String, ControllerState<Self::Status>>,
        publication: &PublicationResult,
    ) -> BTreeMap<String, ControllerUpdate<Self::Status>> {
        if publication.publication_status.is_success() {
            on_successful_publication(current_states, publication)
        } else {
            on_failed_publication(current_states, publication)
        }
    }

    async fn update(
        &self,
        catalog_name: String,
        current_state: ControllerState<Self::Status>,
        control_plane: &mut dyn ControlPlane,
    ) -> anyhow::Result<ControllerUpdate<Self::Status>> {
        if current_state.status.is_publication_pending() {
            // We're still waiting on a publication to complete, so just keep waiting.
            // Note that we don't set next_run or update the status.
            return Ok(current_state.to_update());
        }

        let maybe_schema_md5 = control_plane
            .get_inferred_schema(&catalog_name)
            .await?
            .map(|s| s.md5);

        if current_state.status.schema_md5 != maybe_schema_md5
            || current_state.status.consumers_need_updated()
        {
            let mut new_status = current_state.status.clone();
            if new_status.schema_md5 != maybe_schema_md5 {
                new_status.schema_md5 = maybe_schema_md5;
                new_status.schema_last_updated = control_plane.current_time();
            }
            create_publication(catalog_name, &mut new_status, control_plane).await?;

            Ok(current_state.to_update().with_status(new_status))
        } else {
            // Everything seems up to date, so just schedule the next check-up
            let next_run = current_state.status.next_run(control_plane.current_time());
            Ok(current_state.to_update().with_next_run(next_run))
        }
    }
}

async fn create_publication(
    collection_name: String,
    new_status: &mut InferredSchemaStatus,
    control_plane: &mut dyn ControlPlane,
) -> anyhow::Result<()> {
    let live = control_plane.get_live_spec(&collection_name).await?;
    let mut draft = live.into_draft();

    // See if there's any consumers that need to have their backfill counters incremented, and do that now.
    for (consumer, _) in new_status.consumers.iter().filter(|c| c.1.needs_backfill) {
        let mut consumer_spec = control_plane.get_live_spec(consumer).await?.into_draft();
        for mat in consumer_spec.materializations.iter_mut() {
            for binding in mat
                .spec
                .bindings
                .iter_mut()
                .filter(|b| !b.disable && b.source.collection().as_str() == collection_name)
            {
                tracing::debug!(%collection_name, %consumer, prev_backfill = %binding.backfill, "incrementing materialization backfill counter");
                binding.backfill += 1;
            }
        }
        for coll in consumer_spec
            .collections
            .iter_mut()
            .filter(|c| c.spec.derive.is_some())
        {
            for binding in coll
                .spec
                .derive
                .as_mut()
                .unwrap()
                .transforms
                .iter_mut()
                .filter(|b| !b.disable && b.source.collection().as_str() == collection_name)
            {
                tracing::debug!(%collection_name, %consumer, prev_backfill = %binding.backfill, "incrementing derivation backfill counter");
                binding.backfill += 1;
            }
        }
        draft.merge(consumer_spec)
    }

    let pub_id = control_plane.create_publication(draft).await?;
    new_status.publications.pending = Some(PublicationStatus::created(
        pub_id,
        control_plane.current_time(),
    ));
    Ok(())
}

fn on_successful_publication(
    current_states: &BTreeMap<String, ControllerState<InferredSchemaStatus>>,
    publication: &PublicationResult,
) -> BTreeMap<String, ControllerUpdate<InferredSchemaStatus>> {
    let mut drafted_collections = BTreeSet::new();
    let mut updates = BTreeMap::new();
    // If the collection was drafted, then we can assume that all consumers of the collection
    // have had their inferred schemas updated.
    for draft_collection in publication.draft.collections.iter() {
        drafted_collections.insert(draft_collection.catalog_name.as_str());
        let inferred_schema_md5 = publication
            .inferred_schemas
            .iter()
            .find(|s| s.collection_name == draft_collection.catalog_name)
            .map(|s| s.md5.clone());
        let current_state = current_states.get(draft_collection.catalog_name.as_str());
        let desired_active = uses_inferred_schema(&draft_collection.spec);

        let maybe_update = match (current_state, desired_active) {
            (Some(state), true) => {
                let next_status = state.status.on_successful_collection_publication(
                    &draft_collection.catalog_name,
                    inferred_schema_md5,
                    publication,
                );
                let mut update = state.to_update().with_active(true);
                if !next_status.is_publication_pending() {
                    let next_run = next_status.next_run(publication.completed_at);
                    update = update.with_next_run(next_run);
                }
                Some(update.with_status(next_status))
            }
            (None, true) => {
                let init_status = InferredSchemaStatus::initial(
                    &draft_collection.catalog_name,
                    inferred_schema_md5,
                    publication,
                );

                Some(ControllerUpdate {
                    active: true,
                    next_run: Some(init_status.next_run(publication.completed_at)),
                    status: Some(init_status),
                })
            }
            (Some(state), false) if state.active => {
                // Deactivate the state in response to a publicaction that removes use of the inferred schema
                Some(state.to_update().with_active(false))
            }
            _ => None,
        };
        if let Some(update) = maybe_update {
            updates.insert(draft_collection.catalog_name.clone(), update);
        }
    }

    for live_collection in publication
        .live
        .collections
        .iter()
        .filter(|c| uses_inferred_schema(&c.spec))
        .filter(|c| !drafted_collections.contains(c.catalog_name.as_str()))
    {
        // The inferred schema may have still been updated if the collection was pulled into the
        // publication via spec expansion. Record the update, since it may obviate the need for another
        // publish.
        let inferred_schema_md5 = publication
            .inferred_schemas
            .iter()
            .find(|s| s.collection_name == live_collection.catalog_name)
            .map(|s| s.md5.clone());
        let current_state = current_states.get(live_collection.catalog_name.as_str());
        let desired_active = uses_inferred_schema(&live_collection.spec);

        if let Some(state) = current_state {
            let new_status = state.status.on_successful_collection_publication(
                &live_collection.catalog_name,
                inferred_schema_md5,
                publication,
            );
            let next_run = new_status.next_run(publication.completed_at);
            let update = state
                .to_update()
                .with_active(true)
                .with_status(new_status)
                .with_next_run(next_run);
            updates.insert(live_collection.catalog_name.clone(), update);
        }
    }

    updates
}

fn on_failed_publication(
    current_states: &BTreeMap<String, ControllerState<InferredSchemaStatus>>,
    publication: &PublicationResult,
) -> BTreeMap<String, ControllerUpdate<InferredSchemaStatus>> {
    let mut updates = BTreeMap::new();

    for (collection_name, state) in current_states.iter().filter(|(_, v)| v.active) {
        if state
            .status
            .publications
            .is_pending(publication.publication_id)
        {
            let next_status = state
                .status
                .on_failed_publication(collection_name, publication);
            let next_run = next_status.next_run(publication.completed_at);
            let update = state
                .to_update()
                .with_status(next_status)
                .with_next_run(next_run);
            updates.insert(collection_name.clone(), update);
        }
    }

    updates
}

fn uses_inferred_schema(collection: &models::CollectionDef) -> bool {
    collection
        .read_schema
        .as_ref()
        .map(models::Schema::references_inferred_schema)
        .unwrap_or(false)
}

#[cfg(test)]
mod test {
    use crate::controllers::test_util::{self, Harness, TestPublication};
    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn happy_path_test() {
        let mut harness = Harness::new(InferredSchemaController);

        let initial_draft = test_util::draft_of(json!({
            "captures": {
                "a/cap": mock_capture_spec(&["acmeCo/a"]),
            },
            "collections": {
                "a/a": mock_collection_spec(true),
                "a/d": mock_derivation_spec(true, &["a/a"]),
            },
            "materializations": {
                "a/m1": mock_materialization_spec(&["a/a", "a/d"]),
                "a/m2": mock_materialization_spec(&["a/a"]),
                "a/m3": mock_materialization_spec(&["a/d"]),
            }
        }));

        let (info, updates) = harness.observe_publication(TestPublication::of_draft(initial_draft));
        // Expect to see updates for both "a/a" and "a/d" that initialize new controller states
        // for those collections.
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("initial-updates", &updates);
        });

        // The inferred schema of a/a gets updated.
        harness.update_inferred_schema("a/a", 1);

        // Technically, either a/a or a/d could be run next in the real world, since they both have
        // identical next_run values. Time is deterministic in the test environment, though, and the
        // harness always selects controllers in lexicographical order.
        let (info, update, mut publications) = harness.next_run_update();
        assert_eq!(info.catalog_name, "a/a");
        // Expect to see that a publication of a/a was created in response to the schema being updated
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("update-after-a-schema-updated-1", (update, &publications));
        });

        // Now the controller observes the successful completion of that publication
        let publication = publications.pop().unwrap();
        let (info, updates) = harness.observe_publication(publication);

        // Expect to see the successful publication in the history of a/a, and for the inferred schema
        // md5 to be updated for all consumers of a/a (but not of a/d, which doesn't yet have an
        // inferred schema)
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("observe-pub-a-1-completed", &updates);
        });
        // the status of a/a should indicate that all consumers are now up-to-date
        assert!(!updates
            .get("a/a")
            .unwrap()
            .status
            .as_ref()
            .unwrap()
            .consumers_need_updated());

        // Now the inferred schema for a/d gets updated
        harness.update_inferred_schema("a/d", 1);

        let (info, update, mut publications) = harness.next_run_update();

        assert_eq!(info.catalog_name, "a/d");
        // Expect to see that a publication of a/d was created in response to the schema being updated
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("update-after-d-schema-updated-1", (update, &publications));
        });

        let (info, updates) = harness.observe_publication(publications.pop().unwrap());

        // Expect to see the successful publication in the history of a/d, and for the inferred schema
        // md5 to be updated for all consumers of a/d (but not of a/a, whose inferred schema is unchanged)
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("observe-pub-d-1-completed", &updates);
        });

        // Assert that the next update runs don't publish anthing, since the inferred schemas haven't been updated
        let (info, update, publications) = harness.next_run_update();
        assert_eq!(info.catalog_name, "a/a");
        assert!(publications.is_empty());
        assert!(update.next_run.is_some());

        let (info, update, publications) = harness.next_run_update();
        assert_eq!(info.catalog_name, "a/d");
        assert!(publications.is_empty());
        assert!(update.next_run.is_some());
    }

    fn mock_derivation_spec(use_inferred: bool, sources: &[&str]) -> Value {
        let mut collection = mock_collection_spec(use_inferred);

        let transforms = sources
            .iter()
            .map(|name| {
                json!({
                    "name": name, "source": name, "lambda": "shelect shtar"
                })
            })
            .collect::<Vec<_>>();
        let derive = json!({
            "using": {
                "sqlite": {}
            },
            "transforms": transforms,
        });
        collection
            .as_object_mut()
            .unwrap()
            .insert("derive".to_owned(), derive);
        collection
    }

    fn mock_collection_spec(uses_inferred: bool) -> Value {
        let read_schema = if uses_inferred {
            json!({
                "allOf": [
                    { "$ref": "flow://inferred-schema" },
                    { "$ref": "flow://write-schema" },
                ]
            })
        } else {
            json!({"type": "object"})
        };
        json!({
            "key": ["/id"],
            "writeSchema": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"}
                }
            },
            "readSchema": read_schema,
        })
    }

    fn mock_capture_spec(bound_collections: &[&str]) -> Value {
        let bindings = bound_collections
            .into_iter()
            .map(|collection| {
                json!({
                    "resource": { "thing": collection },
                    "target": collection,
                })
            })
            .collect();
        mock_connector_task(bindings)
    }

    fn mock_materialization_spec(bound_collections: &[&str]) -> Value {
        let bindings = bound_collections
            .into_iter()
            .map(|collection| {
                json!({
                    "resource": { "thing": collection },
                    "source": collection,
                })
            })
            .collect();
        mock_connector_task(bindings)
    }

    fn mock_connector_task(bindings: Vec<Value>) -> Value {
        serde_json::json!({
            "endpoint": {
                "connector": {
                    "image": "foo:test",
                    "config": {},
                }
            },
            "bindings": bindings,
        })
    }
}
