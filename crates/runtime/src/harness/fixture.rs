use super::Read;
use futures::{stream::BoxStream, StreamExt};
use proto_flow::flow;
use proto_gazette::consumer;
use std::collections::HashMap;

// Fixture is a Vec of transaction fixtures, where each transaction fixture
// is a Vec of ordered (collection-name, document) instances.
pub type Fixture = Vec<Vec<(models::Collection, serde_json::Value)>>;

#[derive(Clone)]
pub struct Reader(pub Fixture);

impl super::Reader for Reader {
    type Stream = futures::stream::BoxStream<'static, anyhow::Result<Read>>;

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
                    (index, doc::Pointer::from_str(&collection.uuid_ptr)),
                )
            })
            .fold(
                HashMap::<String, Vec<(usize, doc::Pointer)>>::new(),
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
                    (index, doc::Pointer::from_str(&collection.uuid_ptr)),
                )
            })
            .fold(
                HashMap::<String, Vec<(usize, doc::Pointer)>>::new(),
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

impl Reader {
    fn start(
        self,
        index: HashMap<String, Vec<(usize, doc::Pointer)>>,
        resume: consumer::Checkpoint,
    ) -> BoxStream<'static, anyhow::Result<Read>> {
        let skip = resume
            .sources
            .get("fixture")
            .as_ref()
            .map(|source| source.read_through as usize)
            .unwrap_or_default();

        let producer = crate::uuid::Producer([7, 19, 83, 3, 3, 17]);

        coroutines::coroutine(move |mut co| async move {
            for (txn, docs) in self.0.into_iter().enumerate().skip(skip) {
                for (offset, (collection, mut doc)) in docs.into_iter().enumerate() {
                    let Some(bindings) = index.get(collection.as_str()) else {
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

                        *ptr.create_value(&mut doc)
                            .expect("able to create fixture UUID") =
                            serde_json::json!(uuid.as_hyphenated());

                        () = co
                            .yield_(Ok(Read::Document {
                                binding: *binding as u32,
                                doc: doc.to_string().into(),
                            }))
                            .await;
                    }
                }

                // Yield a synthetic Checkpoint which embeds the transaction offset.
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
