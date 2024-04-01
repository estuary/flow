use std::collections::{BTreeMap, BTreeSet};

use crate::controllers::publication_status::{PublicationHistory, PublicationStatus};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tables::AnySpec;
use tables::EitherOrBoth;
use tables::SpecExt;

use super::{
    jittered_next_run, ControlJob, ControlPlane, ControllerState, ControllerUpdate, NextRun,
    PublicationResult,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CollectionStatus {
    inferred_schema_md5: Option<String>,
    md5_last_updated: DateTime<Utc>,
    #[serde(skip_serializing_if = "is_false")]
    needs_backfill: bool,
}

impl CollectionStatus {
    fn observe_schema(&mut self, schema_md5: Option<String>, time: DateTime<Utc>) {
        if self.inferred_schema_md5 != schema_md5 {
            self.inferred_schema_md5 = schema_md5;
            self.md5_last_updated = time;
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InferredSchemaStatus {
    pub collections: BTreeMap<String, CollectionStatus>,
    pub publications: PublicationHistory,
}

impl InferredSchemaStatus {
    fn get_inferred_schema_source_collections(&self) -> BTreeSet<String> {
        self.collections.keys().cloned().collect()
    }

    fn next_run(&self, now: DateTime<Utc>) -> NextRun {
        // The idea here is to check frequently if there isn't an inferred schema at all yet,
        // so we can quickly start materializing some data. But after it works at least once,
        // then we want to use a longer duration in order to coalesce more schema updates into
        // each publication to prevent undue churn.
        // TODO: we might want to account for `last_backfill` times here
        let any_missing = self
            .collections
            .values()
            .any(|c| c.inferred_schema_md5.is_none());
        let min_backoff_minutes = if any_missing { 1i64 } else { 10i64 };

        let schema_last_updated = self
            .collections
            .values()
            .map(|c| c.md5_last_updated)
            .max()
            .unwrap_or(now);
        let after_minutes = now
            .signed_duration_since(schema_last_updated)
            .num_minutes()
            .max(min_backoff_minutes)
            .min(90);
        NextRun::after_minutes(after_minutes as u32)
    }

    fn initial(
        drafted_spec: AnySpec<'_>,
        inferred_info: &BTreeMap<&str, Option<&str>>,
        pub_status: PublicationStatus,
    ) -> InferredSchemaStatus {
        let pub_time = pub_status.completed.unwrap();
        let mut collections = BTreeMap::new();
        for source in drafted_spec.reads_from() {
            let Some(maybe_md5) = inferred_info.get(source.as_str()) else {
                continue;
            };
            let status = CollectionStatus {
                inferred_schema_md5: maybe_md5.map(|h| h.to_string()),
                md5_last_updated: pub_time,
                needs_backfill: false,
            };
            collections.insert(source, status);
        }
        let publications = PublicationHistory::initial(pub_status);

        InferredSchemaStatus {
            collections,
            publications,
        }
    }

    fn observe_successful_publication(
        &mut self,
        spec: AnySpec<'_>,
        inferred_info: &BTreeMap<&str, Option<&str>>,
        pub_status: PublicationStatus,
    ) {
        let pub_time = pub_status.completed.unwrap();
        let reads_from = spec.reads_from();
        // Delete any state for collections that are no longer being read
        self.collections.retain(|c, _| reads_from.contains(c));
        for source in reads_from {
            let Some(maybe_md5) = inferred_info.get(source.as_str()) else {
                // Delete state for collections that no longer use the inferred schema
                self.collections.remove(&source);
                continue;
            };

            if let Some(current) = self.collections.get_mut(&source) {
                if current.inferred_schema_md5.as_deref() != *maybe_md5 {
                    current.inferred_schema_md5 = maybe_md5.map(|h| h.to_string());
                    current.md5_last_updated = pub_time;
                }
            }
        }
        self.publications.observe(pub_status.clone());
    }

    fn is_publication_pending(&self) -> bool {
        self.publications.pending.is_some()
    }

    fn on_failed_publication(
        &self,
        collection_name: &str,
        publication: &PublicationResult,
    ) -> Self {
        let mut new_status = self.clone();

        let _completed = new_status
            .publications
            .observe(PublicationStatus::observed(publication))
            .expect("publication must be pending");

        new_status
    }
}

// fn get_inferred_schema_md5(collection: &str, publication: &PublicationResult) -> Option<String> {
//     publication
//         .inferred_schemas
//         .iter()
//         .find(|p| p.collection_name == collection)
//         .map(|s| s.md5.clone())
// }

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Debug)]
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

        let current_time = control_plane.current_time();
        let sources = current_state
            .status
            .get_inferred_schema_source_collections();
        let inferred_schemas = control_plane.get_inferred_schemas(sources).await?;

        let needs_publish = tables::left_outer_join(
            current_state.status.collections.iter(),
            inferred_schemas.iter(),
        )
        .any(|((_, source_status), inferred_schema)| {
            source_status.inferred_schema_md5.as_deref() != inferred_schema.map(|s| s.md5.as_str())
        });

        if needs_publish {
            let mut new_status = current_state.status.clone();
            // TODO: check to see if we need to increment backfill counters
            let spec = control_plane.get_live_spec(&catalog_name).await?;
            let pub_id = control_plane.create_publication(spec.into_draft()).await?;

            new_status.publications.pending =
                Some(PublicationStatus::created(pub_id, current_time));
            // Don't set next_run because we'll wait until we observe the result of the publication.
            Ok(current_state.to_update().with_status(new_status))
        } else {
            let update = current_state
                .to_update()
                .with_next_run(current_state.status.next_run(current_time));
            Ok(update)
        }
    }
}

fn handle_successful_publish(
    consumer_name: &str,
    spec: AnySpec<'_>,
    existing: Option<&ControllerState<InferredSchemaStatus>>,
    inferred_infos: &BTreeMap<&str, Option<&str>>,
    pub_status: PublicationStatus,
    updates: &mut BTreeMap<String, ControllerUpdate<InferredSchemaStatus>>,
) {
    if let Some(existing_state) = existing {
        let mut status = existing_state.status.clone();
        status.observe_successful_publication(spec, &inferred_infos, pub_status.clone());
        let mut update = existing_state.to_update().with_active(spec.is_enabled());
        if spec.is_enabled() && !status.is_publication_pending() {
            update = update.with_next_run(status.next_run(pub_status.completed.unwrap()));
        }
        updates.insert(consumer_name.to_owned(), update.with_status(status));
    } else {
        let status = InferredSchemaStatus::initial(spec, inferred_infos, pub_status.clone());
        updates.insert(
            consumer_name.to_string(),
            ControllerUpdate {
                active: spec.is_enabled(),
                next_run: Some(status.next_run(pub_status.completed.unwrap())),
                status: Some(status),
            },
        );
    }
}

fn on_successful_publication(
    current_states: &BTreeMap<String, ControllerState<InferredSchemaStatus>>,
    publication: &PublicationResult,
) -> BTreeMap<String, ControllerUpdate<InferredSchemaStatus>> {
    let pub_status = PublicationStatus::observed(publication);
    let mut updates = BTreeMap::new();

    // Map of every collection that uses the inferred schema, to the md5 hash of the inferred schema.
    // Collections that don't use the inferred schema will not be present here.
    let mut collections_using_inferred = tables::left_outer_join(
        publication
            .live
            .collections
            .iter()
            .filter(|c| uses_inferred_schema(&c.spec)),
        publication.inferred_schemas.iter(),
    )
    .map(|(c, s)| {
        (
            c.catalog_name.as_str(),
            s.as_ref().map(|sch| sch.md5.as_str()),
        )
    })
    .collect::<BTreeMap<_, _>>();
    for drafted in publication.draft.collections.iter() {
        if uses_inferred_schema(&drafted.spec) {
            // A newly published collection that uses schema inference will not have an inferred schema yet.
            if !collections_using_inferred.contains_key(drafted.catalog_name.as_str()) {
                collections_using_inferred.insert(&drafted.catalog_name, None);
            }
        } else {
            // If a publication removed the use of the inferred schema, then we no longer need to track it.
            collections_using_inferred.remove(drafted.catalog_name.as_str());
        }
    }

    for eob in tables::full_outer_join(
        publication.draft.collections.iter(),
        publication.live.collections.iter(),
    ) {
        let (name, spec) = match eob {
            EitherOrBoth::Left(new_spec) if new_spec.spec.derive.is_some() => (
                new_spec.catalog_name.as_str(),
                AnySpec::from(&new_spec.spec),
            ),
            EitherOrBoth::Right(live) if live.spec.derive.is_some() => {
                (live.catalog_name.as_str(), AnySpec::from(&live.spec))
            }
            EitherOrBoth::Both(drafted, _) if drafted.spec.derive.is_some() => {
                (drafted.catalog_name.as_str(), AnySpec::from(&drafted.spec))
            }
            _ => {
                continue;
            }
        };
        let state = current_states.get(name);
        handle_successful_publish(
            name,
            spec,
            state,
            &collections_using_inferred,
            pub_status.clone(),
            &mut updates,
        );
    }
    for eob in tables::full_outer_join(
        publication.draft.materializations.iter(),
        publication.live.materializations.iter(),
    ) {
        let (name, spec) = match eob {
            EitherOrBoth::Left(new_spec) => (
                new_spec.catalog_name.as_str(),
                AnySpec::from(&new_spec.spec),
            ),
            EitherOrBoth::Right(live) => (live.catalog_name.as_str(), AnySpec::from(&live.spec)),
            EitherOrBoth::Both(drafted, _) => {
                (drafted.catalog_name.as_str(), AnySpec::from(&drafted.spec))
            }
            _ => {
                continue;
            }
        };
        let state = current_states.get(name);
        handle_successful_publish(
            name,
            spec,
            state,
            &collections_using_inferred,
            pub_status.clone(),
            &mut updates,
        );
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

        // Technically, either a/d, a/m1, or a/m2 could be run next in the real world, since they all have
        // identical next_run values. Time is deterministic in the test environment, though, and the
        // harness always selects controllers in lexicographical order.
        let (info, update, mut publications) = harness.next_run_update();
        assert_eq!(info.catalog_name, "a/d");
        // Expect to see that a publication of a/d was created in response to the a/a schema being updated
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("update-after-a-schema-updated-1", (update, &publications));
        });

        // Now the controller observes the successful completion of that publication
        let publication = publications.pop().unwrap();
        let (info, updates) = harness.observe_publication(publication);
        // Expect to see the successful publication in the history of a/d, and for the inferred schema
        // md5 to be updated for a/a
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("observe-pub-a-1-completed", &updates);
        });

        let (info, update, mut publications) = harness.next_run_update();
        assert_eq!(info.catalog_name, "a/m2");
        // Expect to see that a publication of a/m2 was created in response to the a/a schema being updated
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("update-m2-after-a-schema-updated-1", (update, &publications));
        });

        let (info, updates) = harness.observe_publication(publications.pop().unwrap());
        // Expect to see the successful publication in the history of a/d, and for the inferred schema
        // md5 to be updated for all consumers of a/d (but not of a/a, whose inferred schema is unchanged)
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("observe-m2-pub-a-1-completed", &updates);
        });

        // Now the inferred schema for a/d gets updated
        harness.update_inferred_schema("a/d", 1);

        // The next update run should create a publication of a/m1 now that the a/d schema was updated
        let (info, update, mut publications) = harness.next_run_update();
        assert_eq!(info.catalog_name, "a/m1");
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("update-m1-after-d-schema-updated-1", (update, &publications));
        });
        let (info, updates) = harness.observe_publication(publications.pop().unwrap());
        // Expect to see the successful publication in the history of a/m1, and for the inferred schema
        // md5 to be updated for the source a/d
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("observe-m1-pub-d-1-completed", &updates);
        });

        // a/m3 should be published to reflect the updated a/d schema
        let (info, update, mut publications) = harness.next_run_update();
        assert_eq!(info.catalog_name, "a/m3");
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("update-m3-after-d-schema-updated-1", (update, &publications));
        });
        let (info, updates) = harness.observe_publication(publications.pop().unwrap());
        // Expect to see the successful publication in the history of a/m1, and for the inferred schema
        // md5 to be updated for the source a/d
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("observe-m3-pub-d-1-completed", &updates);
        });

        // Scheduled update runs should not create any publications because none of the inferred
        // schemas have been updated.
        for _ in 0..9 {
            let (info, update, pubs) = harness.next_run_update();
            assert!(
                pubs.is_empty(),
                "expected empty publications, got: {pubs:?}, info: {info:?}"
            );
            assert!(
                update.next_run.is_some(),
                "expected Some next_run, got: {update:?}, info: {info:?}"
            );
        }

        harness.update_inferred_schema("a/a", 2);

        // a/m2 update and observe publication
        let (info, update, mut publications) = harness.next_run_update();
        assert_eq!("a/m2", info.catalog_name);
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("update-m2-after-a-schema-updated-2", (update, &publications));
        });
        let (info, updates) = harness.observe_publication(publications.pop().unwrap());
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("observe-m2-pub-a-2-completed", &updates);
        });

        // a/d update and observe publication
        let (info, update, mut publications) = harness.next_run_update();
        assert_eq!("a/d", info.catalog_name);
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("update-d-after-a-schema-updated-2", (update, &publications));
        });
        let (info, updates) = harness.observe_publication(publications.pop().unwrap());
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("observe-d-pub-a-2-completed", &updates);
        });

        // a/m1 update and observe publication
        let (info, update, mut publications) = harness.next_run_update();
        assert_eq!("a/m1", info.catalog_name);
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("update-m1-after-a-schema-updated-2", (update, &publications));
        });
        let (info, updates) = harness.observe_publication(publications.pop().unwrap());
        insta::with_settings!({ info => &info }, {
            insta::assert_json_snapshot!("observe-m1-pub-a-2-completed", &updates);
        });

        // assert!(publications.is_empty());
        // assert!(update.next_run.is_some());

        // let (info, update, publications) = harness.next_run_update();
        // assert_eq!(info.catalog_name, "a/d");
        // assert!(publications.is_empty());
        // assert!(update.next_run.is_some());
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
