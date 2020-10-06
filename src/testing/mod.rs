use crate::catalog::{
    self,
    specs::{TestStep, TestStepVerify},
};
use crate::derive;
use crate::doc::{Diff, FailedValidation, Pointer, SchemaIndex};
use crate::runtime::{self, cluster};
use estuary_json::Location;
use estuary_protocol::flow::{self, ingest_request};
use estuary_protocol::protocol::{Label, LabelSelector, LabelSet, ReadRequest};
use futures::StreamExt;
use itertools::Itertools;
use serde_json::Value;
use std::collections::BTreeMap;

mod driver;
use driver::Action;
pub use driver::Transform;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("database error")]
    Rusqlite(#[from] rusqlite::Error),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Flow cluster error")]
    Cluster(#[from] cluster::Error),
    #[error("catalog database error")]
    Catalog(#[from] catalog::Error),
    #[error("JSON decoding error")]
    Json(#[from] serde_json::Error),
    #[error("gRPC stream error")]
    TonicStatus(#[from] tonic::Status),

    #[error("invalid document UUID: {value:?}")]
    InvalidUuid { value: Option<serde_json::Value> },
    #[error("document validation error: {}",
        serde_json::to_string_pretty(.0).unwrap())]
    Validation(FailedValidation),
    #[error("detected differences while verifying collection {:?}: {}",
        .collection, serde_json::to_string_pretty(.diffs).unwrap())]
    Verify {
        collection: String,
        diffs: Vec<Diff>,
    },
}

pub struct Collection {
    collection_name: String,
    key: Vec<Pointer>,
    schema: url::Url,
    uuid_ptr: Pointer,
}

impl Collection {
    pub fn load_all(db: &rusqlite::Connection) -> Result<Vec<Collection>, rusqlite::Error> {
        db.prepare(
            "SELECT
            collection_name,
            key_json,
            schema_uri,
            '/_meta/uuid' AS uuid_ptr
            FROM collections;",
        )?
        .query_map(rusqlite::NO_PARAMS, |r| {
            let collection_name = r.get::<_, String>(0)?;
            let key = r.get::<_, Value>(1)?;
            let schema = r.get::<_, url::Url>(2)?;
            let uuid_ptr = r.get::<_, String>(3)?.into();

            let key: Vec<String> = serde_json::from_value(key).unwrap();
            let key = key.iter().map(Into::into).collect::<Vec<_>>();

            Ok(Collection {
                collection_name,
                key,
                schema,
                uuid_ptr,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
    }

    /// Load transitive dependencies of collections, returned as a sorted Vec of (derived-collection, source-collection).
    pub fn load_transitive_dependencies(
        db: &rusqlite::Connection,
    ) -> Result<Vec<(String, String)>, rusqlite::Error> {
        db.prepare(
            "SELECT
            derivation_name,
            source_name
            FROM collection_transitive_dependencies
            ORDER BY derivation_name, source_name ASC;",
        )?
        .query_map(rusqlite::NO_PARAMS, |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()
    }
}

pub struct Context {
    pub cluster: runtime::Cluster,
    pub collections: Vec<Collection>,
    pub collection_dependencies: Vec<(String, String)>,
    pub schema_index: &'static SchemaIndex<'static>,
    pub transforms: Vec<Transform>,
}

impl Context {
    /// Run a test case to completion.
    /// TODO(johnny): This is a hair-ball that needs to be teased apart.
    pub async fn run_test_case(&self, steps: &Vec<TestStep>) -> Result<(), Error> {
        // Gather journal offsets for each journal which will be verified by this test, before we begin.
        let mut verify_from = BTreeMap::new();

        for step in steps {
            if let TestStep::Verify(spec) = step {
                let selector = verify_journal_selector(spec);
                let journals = self.cluster.list_journals(Some(selector)).await?;

                for journal in journals.journals {
                    let journal = journal.spec.unwrap().name;

                    // TODO(johnny): Make this a transactional write-barrier.
                    let mut stream = self
                        .cluster
                        .read(ReadRequest {
                            header: None,
                            journal: journal.clone(),
                            offset: -1,
                            block: false,
                            do_not_proxy: false,
                            metadata_only: true,
                            end_offset: 0,
                        })
                        .await?;

                    if let Some(chunk) = stream.next().await {
                        let chunk = chunk?;
                        verify_from.insert(journal, chunk.write_head);
                    }
                }
            }
        }
        log::info!("collected verify_from offsets {:?}", verify_from);

        let mut driver =
            driver::Driver::new(&self.transforms, &self.collection_dependencies, steps);

        while let Some(action) = driver.next() {
            match action {
                Action::Ingest(spec) => {
                    log::info!("action: ingest {:?}", spec.collection);

                    let mut docs_json_lines = Vec::new();
                    for doc in &spec.documents {
                        serde_json::to_writer(&mut docs_json_lines, doc).unwrap();
                        docs_json_lines.push(b'\n');
                    }

                    let request = flow::IngestRequest {
                        collections: vec![ingest_request::Collection {
                            name: spec.collection.clone(),
                            docs_json_lines,
                        }],
                    };
                    let response = self
                        .cluster
                        .ingest_client()
                        .await?
                        .ingest(request)
                        .await?
                        .into_inner();

                    driver.completed_ingest(spec, response);
                }
                Action::Advance(add_clock_delta_seconds) => {
                    log::info!("action: advance time {:?}", add_clock_delta_seconds);
                    self.cluster
                        .advance_time(flow::AdvanceTimeRequest {
                            add_clock_delta_seconds,
                        })
                        .await?;
                }
                Action::Verify(spec) => {
                    log::info!("action: verify {:?}", spec);

                    let mut content = Vec::new();

                    let selector = verify_journal_selector(spec);
                    let journals = self.cluster.list_journals(Some(selector)).await?;

                    for journal in journals.journals {
                        let journal = journal.spec.unwrap().name;
                        let offset = *verify_from.get(&journal).unwrap_or(&0);
                        let end_offset = *driver.journal_heads().get(&journal).unwrap_or(&0);

                        log::info!(
                            "action: verify => fetching {:?} range {}:{}",
                            &journal,
                            offset,
                            end_offset
                        );

                        let mut stream = self
                            .cluster
                            .read(ReadRequest {
                                header: None,
                                journal,
                                offset,
                                end_offset,
                                block: true,
                                do_not_proxy: false,
                                metadata_only: false,
                            })
                            .await?;

                        while let Some(chunk) = stream.next().await {
                            let chunk = chunk?;
                            content.extend(chunk.content.into_iter());
                        }
                    }

                    log::info!(
                        "read verify content:\n{}",
                        String::from_utf8_lossy(&content)
                    );

                    let collection = self
                        .collections
                        .iter()
                        .find(|c| c.collection_name == spec.collection)
                        .unwrap();

                    let mut combiner = derive::combiner::Combiner::new(
                        self.schema_index,
                        &collection.schema,
                        collection.key.clone().into(),
                    );

                    for doc in serde_json::Deserializer::from_slice(&content).into_iter::<Value>() {
                        let doc = doc?;

                        // Inspect the document's UUID to determine if this is a transaction acknowledgment
                        // (which should be skipped while combining).
                        let uuid = derive::extract_uuid_parts(&doc, &collection.uuid_ptr)
                            .ok_or_else(|| Error::InvalidUuid {
                                value: collection.uuid_ptr.query(&doc).cloned(),
                            })?;

                        if uuid.producer_and_flags & FLAGS_ACK_TXN != 0 {
                            continue;
                        }
                        combiner.combine(doc).map_err(|e| Error::Validation(e))?;
                    }

                    let root = Location::Root;
                    let mut diffs = Vec::new();

                    for (index, eob) in combiner
                        .into_entries("")
                        .zip_longest(spec.documents.iter())
                        .enumerate()
                    {
                        Diff::diff(
                            eob.as_ref().left(),
                            eob.as_ref().right().cloned(),
                            &root.push_item(index),
                            &mut diffs,
                        );
                    }

                    if !diffs.is_empty() {
                        return Err(Error::Verify {
                            collection: spec.collection.clone(),
                            diffs,
                        });
                    }
                }
                Action::Stat(pending) => {
                    for pending in pending {
                        let selector = Some(shard_selector(&pending.derivation));
                        let shards = self.cluster.list_shards(selector).await?;

                        for shard in shards.shards {
                            let shard = shard.spec.unwrap().id;
                            log::info!("action: stat shard {:?}", shard);

                            let response =
                                self.cluster.stat_shard(pending.stat_request(shard)).await?;
                            driver.completed_stat(&pending, response);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

// Builds a LabelSelector which matches journals to be read by this TestStepVerify.
fn verify_journal_selector(spec: &TestStepVerify) -> LabelSelector {
    let mut include = vec![Label {
        name: "estuary.dev/collection".to_owned(),
        value: spec.collection.to_owned(),
    }];
    let mut exclude = Vec::new();

    if let Some(sel) = &spec.partitions {
        push_partitions(&sel.include, &mut include);
        push_partitions(&sel.exclude, &mut exclude);
    }
    LabelSelector {
        include: Some(LabelSet { labels: include }),
        exclude: Some(LabelSet { labels: exclude }),
    }
}

// Build a LabelSelector which matches shards of this derivation.
fn shard_selector(derivation: &str) -> LabelSelector {
    LabelSelector {
        include: Some(LabelSet {
            labels: vec![Label {
                name: "estuary.dev/derivation".to_owned(),
                value: derivation.to_owned(),
            }],
        }),
        exclude: None,
    }
}

// Flatten partition selector fields into a Vec<Label>.
// Value::String is percent encoded, and all other Values use literal JSON strings
// to derive label values.
// ***This MUST match the Go-side behavior in Field_Value.EncodePartition!***
fn push_partitions(fields: &BTreeMap<String, Vec<Value>>, out: &mut Vec<Label>) {
    for (field, value) in fields {
        for value in value {
            let value = match value {
                Value::String(s) => {
                    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC)
                        .to_string()
                }
                _ => serde_json::to_string(value).unwrap(),
            };
            out.push(Label {
                name: format!("estuary.dev/field/{}", field),
                value,
            });
        }
    }
}

const FLAGS_ACK_TXN: u64 = 0x2;
