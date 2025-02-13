use anyhow::Context;
use prost::Message;
use proto_flow::{
    flow,
    materialize::{request, Request},
    runtime_checkpoint, RuntimeCheckpoint,
};
use std::collections::BTreeMap;
use std::io::Write;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct MaterializeFixture {
    /// Path or URL to a Flow specification file, having a single materialization.
    #[clap(long)]
    source: String,
    // Path to a materialization fixture.
    #[clap(long)]
    fixture: String,
}

#[derive(serde::Deserialize)]
struct Fixture {
    #[serde(default)]
    checkpoint: serde_json::Value,
    transactions: Vec<BTreeMap<String, Vec<(bool, serde_json::Value)>>>,
}

pub async fn do_materialize_fixture(
    _ctx: &mut crate::CliContext,
    MaterializeFixture { source, fixture }: &MaterializeFixture,
) -> anyhow::Result<()> {
    let spec: bytes::Bytes = std::fs::read(source).context("failed to read spec")?.into();
    let mut spec: flow::MaterializationSpec =
        Message::decode(spec).context("failed to parse MaterializationSpec")?;

    // Unwrap the connector configuration before passing it on.
    let models::ConnectorConfig { image: _, config } =
        serde_json::from_str(&spec.config_json).expect("materialization spec is a connector");
    spec.config_json = config.to_string();

    let fixtures = std::fs::read(fixture).context("failed to read fixture")?;
    let Fixture {
        checkpoint,
        transactions,
    } = serde_yaml::from_slice(&fixtures).context("failed to parse Fixture")?;

    let mut out = std::io::BufWriter::new(std::io::stdout());
    let mut emit = |request: Request| {
        serde_json::to_writer(&mut out, &request).unwrap();
        out.write(&['\n' as u8]).unwrap();
    };

    emit(Request {
        apply: Some(request::Apply {
            materialization: Some(spec.clone()),
            version: "test".to_string(),
            last_materialization: None,
            last_version: String::new(),
            state_json: String::new(),
        }),
        ..Default::default()
    });
    emit(Request {
        open: Some(request::Open {
            materialization: Some(spec.clone()),
            range: Some(flow::RangeSpec {
                key_begin: 0,
                key_end: u32::MAX,
                r_clock_begin: 0,
                r_clock_end: u32::MAX,
            }),
            state_json: checkpoint.to_string(),
            version: "test".to_string(),
        }),
        ..Default::default()
    });
    emit(Request {
        acknowledge: Some(request::Acknowledge {}),
        ..Default::default()
    });

    let mut buf = bytes::BytesMut::new();
    let buf = &mut buf;

    for (round, transaction) in transactions.into_iter().enumerate() {
        let mut loads = Vec::new();
        let mut stores = Vec::new();

        for (fixture_collection, docs) in transaction {
            for (binding_index, binding) in spec.bindings.iter().enumerate() {
                let flow::materialization_spec::Binding {
                    collection,
                    field_selection,
                    delta_updates,
                    ..
                } = binding;

                let flow::CollectionSpec {
                    name: this_collection,
                    projections,
                    ..
                } = collection.as_ref().unwrap();

                let flow::FieldSelection { keys, values, .. } = field_selection.as_ref().unwrap();

                if fixture_collection != *this_collection {
                    continue;
                }

                let key_ex = extractors::for_fields(keys, projections, &doc::SerPolicy::noop())?;
                let values_ex =
                    extractors::for_fields(values, projections, &doc::SerPolicy::noop())?;

                for (exists, doc) in &docs {
                    if !delta_updates {
                        loads.push(Request {
                            load: Some(request::Load {
                                binding: binding_index as u32,
                                key_packed: doc::Extractor::extract_all(doc, &key_ex, buf),
                                ..Default::default()
                            }),
                            ..Default::default()
                        });
                    }
                    stores.push(Request {
                        store: Some(request::Store {
                            binding: binding_index as u32,
                            key_packed: doc::Extractor::extract_all(doc, &key_ex, buf),
                            values_packed: doc::Extractor::extract_all(doc, &values_ex, buf),
                            doc_json: doc.to_string(),
                            exists: *exists && !delta_updates,
                            ..Default::default()
                        }),
                        ..Default::default()
                    });
                }
            }
        }

        for load in loads {
            emit(load)
        }
        emit(Request {
            flush: Some(request::Flush {}),
            ..Default::default()
        });
        for store in stores {
            emit(store)
        }
        emit(Request {
            start_commit: Some(request::StartCommit {
                runtime_checkpoint: Some(RuntimeCheckpoint {
                    sources: [(
                        "a/read/journal;suffix".to_string(),
                        runtime_checkpoint::Source {
                            read_through: round as i64,
                            ..Default::default()
                        },
                    )]
                    .into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        emit(Request {
            acknowledge: Some(request::Acknowledge {}),
            ..Default::default()
        });
    }

    out.flush()?;
    Ok(())
}
