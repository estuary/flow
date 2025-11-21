use super::Read;
use futures::{StreamExt, stream::BoxStream};
use proto_flow::flow;
use proto_gazette::consumer;
use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, BufReader};

// StreamingReader reads fixture documents line-by-line from a file.
// Each line is either:
// - A document: ["collection/name", {...document...}]
// - An ack marker: {"ack": true}
//
// The ack marker denotes a transaction boundary. All documents between
// two ack markers (or between the start and first ack) belong to the same transaction.
#[derive(Clone)]
pub struct StreamingReader {
    pub path: std::path::PathBuf,
}

impl super::Reader for StreamingReader {
    type Stream = BoxStream<'static, anyhow::Result<Read>>;

    fn start_for_derivation(
        self,
        derivation: &flow::CollectionSpec,
        resume: consumer::Checkpoint,
    ) -> Self::Stream {
        let transforms = &derivation.derivation.as_ref().unwrap().transforms;

        let index = transforms
            .iter()
            .enumerate()
            .map(|(index, t)| {
                let collection = t.collection.as_ref().unwrap();
                (
                    collection.name.clone(),
                    (index, json::Pointer::from_str(&collection.uuid_ptr)),
                )
            })
            .fold(
                HashMap::<String, Vec<(usize, json::Pointer)>>::new(),
                |mut acc, item| {
                    if let Some(existing) = acc.get_mut(&item.0) {
                        existing.push(item.1);
                    } else {
                        acc.insert(item.0, vec![item.1]);
                    }

                    acc
                },
            );

        self.start(index, resume)
    }

    fn start_for_materialization(
        self,
        materialization: &flow::MaterializationSpec,
        resume: consumer::Checkpoint,
    ) -> Self::Stream {
        let index = materialization
            .bindings
            .iter()
            .enumerate()
            .map(|(index, t)| {
                let collection = t.collection.as_ref().unwrap();
                (
                    collection.name.clone(),
                    (index, json::Pointer::from_str(&collection.uuid_ptr)),
                )
            })
            .fold(
                HashMap::<String, Vec<(usize, json::Pointer)>>::new(),
                |mut acc, item| {
                    if let Some(existing) = acc.get_mut(&item.0) {
                        existing.push(item.1);
                    } else {
                        acc.insert(item.0, vec![item.1]);
                    }

                    acc
                },
            );

        self.start(index, resume)
    }
}

impl StreamingReader {
    fn start(
        self,
        index: HashMap<String, Vec<(usize, json::Pointer)>>,
        resume: consumer::Checkpoint,
    ) -> BoxStream<'static, anyhow::Result<Read>> {
        let skip = resume
            .sources
            .get("fixture")
            .as_ref()
            .map(|source| source.read_through as usize)
            .unwrap_or_default();

        let producer = crate::uuid::Producer([7, 19, 83, 3, 3, 17]);
        let path = self.path.clone();

        coroutines::coroutine(move |mut co| async move {
            let file = match tokio::fs::File::open(&path).await {
                Ok(file) => file,
                Err(err) => {
                    let err = anyhow::Error::from(err)
                        .context(format!("couldn't open streaming fixture file: {:?}", path));
                    () = co.yield_(Err(err)).await;
                    return;
                }
            };

            let reader = BufReader::new(file);
            let mut lines = reader.lines();

            let mut txn: usize = 0;
            let mut offset: usize = 0;
            let mut line_number: usize = 0;

            // Skip transactions we've already processed
            let mut skipped = 0;
            while skipped < skip {
                line_number += 1;
                let line = match lines.next_line().await {
                    Ok(Some(line)) => line,
                    Ok(None) => return, // Reached end of file
                    Err(err) => {
                        () = co.yield_(Err(anyhow::Error::from(err))).await;
                        return;
                    }
                };

                match is_ack_line(&line) {
                    Ok(true) => skipped += 1,
                    Ok(false) => {}
                    Err(err) => {
                        () = co.yield_(Err(err)).await;
                        return;
                    }
                }
            }

            loop {
                line_number += 1;
                let line = match lines.next_line().await {
                    Ok(Some(line)) => line,
                    Ok(None) => break, // End of file
                    Err(err) => {
                        () = co.yield_(Err(anyhow::Error::from(err))).await;
                        return;
                    }
                };
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                // Check if this is an ack marker
                let is_ack = match is_ack_line(&line) {
                    Ok(is_ack) => is_ack,
                    Err(err) => {
                        () = co.yield_(Err(err)).await;
                        return;
                    }
                };

                if is_ack {
                    tracing::info!(line_number, txn, "detected ack, emitting checkpoint");
                    // Emit a checkpoint for the completed transaction
                    () = co
                        .yield_(Ok(Read::Checkpoint(consumer::Checkpoint {
                            sources: [(
                                "fixture".to_string(),
                                consumer::checkpoint::Source {
                                    read_through: 1 + txn as i64,
                                    producers: Vec::new(),
                                },
                            )]
                            .into(),
                            ack_intents: Default::default(),
                        })))
                        .await;

                    txn += 1;
                    offset = 0;
                    continue;
                }

                // Parse as document: [collection, doc]
                let (collection, mut doc): (models::Collection, serde_json::Value) =
                    match serde_json::from_str(&line) {
                        Ok(parsed) => parsed,
                        Err(err) => {
                            let err = anyhow::Error::from(err).context(format!(
                                "couldn't parse fixture line {} as [collection, document]: '{}'",
                                line_number, line
                            ));
                            () = co.yield_(Err(err)).await;
                            return;
                        }
                    };

                let Some(bindings) = index.get(collection.as_str()) else {
                    offset += 1;
                    continue;
                };

                for (binding, ptr) in bindings {
                    // Add a UUID fixture with a synthetic publication time.
                    let seconds = 3600 * txn + offset; // Synthetic timestamp of the document.
                    let uuid = crate::uuid::build(
                        producer,
                        crate::uuid::Clock::from_unix(seconds as u64, 0),
                        crate::uuid::Flags(0),
                    );

                    *json::ptr::create_value(ptr, &mut doc).expect("able to create fixture UUID") =
                        serde_json::json!(uuid.as_hyphenated());

                    () = co
                        .yield_(Ok(Read::Document {
                            binding: *binding as u32,
                            doc: doc.to_string().into(),
                        }))
                        .await;
                }

                offset += 1;

                if line_number % 1000000 == 0 {
                    tracing::info!(
                        line_number,
                        txn,
                        offset,
                        "processed streaming fixture lines"
                    );
                }
            }

            // If there are any remaining documents without a final ack,
            // emit a checkpoint for the last transaction
            if offset > 0 {
                () = co
                    .yield_(Ok(Read::Checkpoint(consumer::Checkpoint {
                        sources: [(
                            "fixture".to_string(),
                            consumer::checkpoint::Source {
                                read_through: 1 + txn as i64,
                                producers: Vec::new(),
                            },
                        )]
                        .into(),
                        ack_intents: Default::default(),
                    })))
                    .await;
            }
        })
        .boxed()
    }
}

// Helper function to check if a line is an ack marker
fn is_ack_line(line: &str) -> anyhow::Result<bool> {
    // Try to parse as JSON object with "ack" field
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(line) {
        if let Some(obj) = val.as_object() {
            if let Some(ack) = obj.get("ack") {
                return Ok(ack.as_bool().unwrap_or(false));
            }
        }
    }
    Ok(false)
}
