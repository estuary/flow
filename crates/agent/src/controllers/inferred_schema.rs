use std::collections::BTreeMap;

use crate::controllers::publication_status::{PublicationHistory, PublicationStatus};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tables::SpecRow;

use super::{
    ControlJob, ControlPlane, ControllerState, ControllerUpdate, NextRun, PublicationResult,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InferredSchemaStatus {
    pub schema_last_updated: DateTime<Utc>,
    pub schema_md5: Option<String>,
    pub publications: PublicationHistory,
}

impl InferredSchemaStatus {
    fn next_run(&self, now: DateTime<Utc>) -> NextRun {
        // The idea here is to check frequently if there isn't an inferred schema at all yet,
        // so we can quickly start materializing some data. But after it works at least once,
        // then we want to use a longer duration in order to coalesce more schema updates into
        // each publication to prevent undue churn.
        // TODO: we might want to account for `last_backfill` times here
        let min_backoff_minutes = if self.schema_md5.is_none() {
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

    fn is_publication_pending(&self) -> bool {
        self.publications.pending.is_some()
    }

    /// Updates the status of all consumers of the collection to reflect that they're using the latest inferred schema.
    /// This works because if the collection itself is published, then all consumers of it must also be included in the
    /// publication.
    fn on_successful_collection_publication(
        &self,
        _collection_name: &str,
        schema_md5: Option<String>,
        publication: &PublicationResult,
    ) -> Self {
        let mut new_status = self.clone();
        new_status.schema_md5 = schema_md5.clone();
        new_status.schema_last_updated = publication.completed_at;

        let pub_status = PublicationStatus::observed(publication);
        let _completed = new_status.publications.observe(pub_status.clone());

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

        let _ = new_status
            .publications
            .observe(PublicationStatus::observed(publication))
            .expect("publication must be pending");

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

        let maybe_schema_md5 = dbg!(control_plane
            .get_inferred_schema(&catalog_name)
            .await?
            .map(|s| s.md5));

        if current_state.status.schema_md5 != maybe_schema_md5 {
            let mut new_status = current_state.status.clone();
            create_publication(catalog_name, &mut new_status, control_plane).await?;

            Ok(current_state.to_update().with_status(new_status))
        } else {
            eprintln!("wtf everything is up to date");
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
    let mut catalog = control_plane.get_live_spec(&collection_name).await?;
    for collection in catalog.collections.iter_mut() {
        eprintln!("collection: {collection:?}");
        collection.draft_update();
    }

    let pub_id = control_plane.create_publication(catalog).await?;
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
    let mut updates = BTreeMap::new();

    let drafted_collections = publication
        .catalog
        .collections
        .iter()
        .filter(|c| c.drafted.is_some());
    for (collection, maybe_inferred_schema) in
        tables::left_outer_join(drafted_collections, publication.inferred_schemas.iter())
    {
        let current_state = current_states.get(collection.get_name());
        let desired_active = uses_inferred_schema(collection.drafted.as_ref().unwrap());

        let maybe_update = match (current_state, desired_active) {
            (Some(state), true) => {
                let next_status = state.status.on_successful_collection_publication(
                    collection.get_name(),
                    maybe_inferred_schema.map(|s| s.md5.clone()),
                    publication,
                );
                let next_run = next_status.next_run(publication.completed_at);
                Some(
                    state
                        .to_update()
                        .with_status(next_status)
                        .with_next_run(next_run),
                )
            }
            // State is currently active, but the collection no longer uses schema inference, so we should deactivate.
            (Some(state), false) if state.active => Some(state.to_update().with_active(false)),
            (Some(_), false) => None, // inactive state remains inactive
            // Need to initialize a new state for this collection
            (None, true) => {
                let next_status = InferredSchemaStatus {
                    schema_md5: maybe_inferred_schema.map(|s| s.md5.clone()),
                    schema_last_updated: publication.completed_at,
                    publications: PublicationHistory::initial(PublicationStatus::observed(
                        publication,
                    )),
                };
                let next_run = next_status.next_run(publication.completed_at);
                Some(ControllerUpdate::new(next_status).with_next_run(next_run))
            }
            (None, false) => None,
        };

        if let Some(update) = maybe_update {
            updates.insert(collection.get_name().to_string(), update);
        }
    }

    // TODO: should we do anything for live collections that were not drafted, but pulled in via expansion?

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
