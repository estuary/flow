use crate::connector::docker_run;
use crate::{connector::docker_run_stream, local_specs};
use anyhow::Context;
use futures::channel::mpsc::Sender;
use futures::{channel, stream, SinkExt, StreamExt};
use proto_flow::flow::{CollectionSpec, ConnectorState};
use proto_flow::{
    capture::{request, Request},
    flow::RangeSpec,
};
use serde::Deserialize;
use serde_json::json;
use serde_json::value::RawValue;
use std::sync::{Arc, Mutex};

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Capture {
    /// Source flow catalog to run
    source: String,

    /// Print the reduced checkpoint of the connector as it gets updated
    #[clap(long, action)]
    print_checkpoint: bool,
}

#[derive(Deserialize)]
struct ConnectorConfig {
    image: String,
    config: Box<RawValue>,
}

#[derive(Debug)]
enum Command {
    Drain,
    Combine(String),
}

pub async fn do_capture(
    ctx: &mut crate::CliContext,
    Capture {
        source,
        print_checkpoint,
    }: &Capture,
) -> anyhow::Result<()> {
    let client = ctx.controlplane_client().await?;
    let (_sources, mut validations) = local_specs::load_and_validate_full(client, &source).await?;

    let capture = validations
        .built_captures
        .first_mut()
        .expect("must have a capture");

    let cfg: ConnectorConfig = serde_json::from_str(&capture.spec.config_json)?;
    capture.spec.config_json = cfg.config.to_string();

    let apply = Request {
        apply: Some(request::Apply {
            capture: Some(capture.spec.clone()),
            version: "0".to_string(),
            dry_run: false,
        }),
        ..Default::default()
    };

    let range = RangeSpec {
        key_begin: 0,
        key_end: u32::MAX,
        r_clock_begin: 0,
        r_clock_end: u32::MAX,
    };

    let open = Request {
        open: Some(request::Open {
            capture: Some(capture.spec.clone()),
            version: "0".to_string(),
            range: Some(range),
            state_json: "{}".to_string(),
        }),
        ..Default::default()
    };

    let apply_output = docker_run(&cfg.image, apply)
        .await
        .context("connector apply")?;

    let apply_action = apply_output
        .applied
        .expect("applied rpc")
        .action_description;
    eprintln!("Apply RPC Response: {apply_action}");

    let bindings = capture.spec.bindings.clone();
    let mut channels: Vec<Arc<Mutex<Sender<Command>>>> = Vec::new();

    for binding in bindings.clone().into_iter() {
        let (send, mut recv) = channel::mpsc::channel::<Command>(1);

        channels.push(Arc::new(Mutex::new(send)));

        let CollectionSpec {
            key,
            name,
            projections,
            write_schema_json,
            ..
        } = binding.collection.context("missing collection")?;

        let write_schema_json = doc::validation::build_bundle(&write_schema_json)
            .context("collection write_schema_json is not a JSON schema")?;
        let validator =
            doc::Validator::new(write_schema_json).context("could not build a schema validator")?;

        let combiner = doc::Combiner::new(
            extractors::for_key(&key, &projections)?.into(),
            None,
            tempfile::tempfile().expect("opening temporary spill file"),
            validator,
        )
        .expect("create combiner");

        tokio::spawn(async move {
            let mut state = State {
                combiner,
                collection_name: name,
            };
            loop {
                let command = match recv.next().await {
                    Some(value) => value,
                    None => return (),
                };

                match command {
                    Command::Combine(doc) => {
                        state.combine_right(&doc).unwrap();
                    }
                    Command::Drain => {
                        let mut out = Vec::with_capacity(32);
                        state = state.drain_chunk(&mut out).expect("failed to drain chunk");
                        let collection_name = &state.collection_name;

                        out.iter().for_each(|v| {
                            println!("{collection_name} {v}");
                        });
                    }
                }
            }
        });
    }

    let (req_send, req_recv) = channel::mpsc::channel::<Request>(1);
    let req_send_arc = Arc::new(Mutex::new(req_send));

    let in_stream = stream::unfold(req_recv, |mut recv| async move {
        match recv.next().await {
            Some(req) => Some((req, recv)),
            _ => None,
        }
    });
    let mut out_stream = docker_run_stream(
        &cfg.image,
        Box::pin(stream::once(async { open }).chain(in_stream)),
    )
    .await
    .context("connector output stream")?;
    let checkpoint = Arc::new(Mutex::new(json!({})));
    let explicit_acknowledgements = Arc::new(Mutex::new(false));
    eprintln!("Documents");
    loop {
        let item = match out_stream.next().await {
            Some(Ok(value)) => value,
            Some(Err(e)) => return Err(e),
            None => return Ok(()),
        };

        let mut explicit_ack = explicit_acknowledgements.lock().unwrap();
        if let Some(opened) = item.opened {
            *explicit_ack = opened.explicit_acknowledgements;
        }

        if let Some(captured) = item.captured {
            let doc = captured.doc_json;

            let sender_mutex = &channels[captured.binding as usize];
            let mut sender = sender_mutex.lock().unwrap();
            sender.send(Command::Combine(doc.clone())).await?;
        }

        if let Some(ConnectorState {
            updated_json,
            merge_patch,
        }) = item.checkpoint.and_then(|c| c.state)
        {
            let mut cp = checkpoint.lock().unwrap();
            let update = serde_json::from_str(&updated_json)?;
            if merge_patch {
                if *print_checkpoint {
                    eprintln!(
                        "Merge Patch for Checkpoint: {}",
                        serde_json::to_string_pretty(&update)?
                    );
                }
                json_patch::merge(&mut cp, &update);
            } else {
                *cp = update;
            }

            if *print_checkpoint {
                eprintln!("Checkpoint: {}", serde_json::to_string_pretty(&*cp)?);
            }

            for channel in channels.iter() {
                let mut sender = channel.lock().unwrap();
                sender.send(Command::Drain).await?;
            }

            if *explicit_ack {
                // Send acknowledge to connector
                let mut ack_channel = req_send_arc.lock().unwrap();
                ack_channel
                    .send(Request {
                        acknowledge: Some(request::Acknowledge { checkpoints: 1 }),
                        ..Default::default()
                    })
                    .await
                    .context("failed to send ack")?;
            }
        }
    }
}

pub struct State {
    // Combiner of published documents.
    combiner: doc::Combiner,

    collection_name: String,
}

impl State {
    pub fn combine_right(&mut self, doc_json: &str) -> anyhow::Result<()> {
        let memtable = match &mut self.combiner {
            doc::Combiner::Accumulator(accumulator) => accumulator.memtable()?,
            _ => panic!("implementation error: combiner is draining, not accumulating"),
        };
        let alloc = memtable.alloc();

        let mut deser = serde_json::Deserializer::from_str(doc_json);
        let doc = doc::HeapNode::from_serde(&mut deser, alloc)
            .with_context(|| format!("couldn't parse published document as JSON: {}", doc_json))?;

        memtable.add(doc, false)?;

        Ok(())
    }

    pub fn drain_chunk(mut self, out: &mut Vec<String>) -> Result<Self, doc::combine::Error> {
        let mut drainer = match self.combiner {
            doc::Combiner::Accumulator(accumulator) => accumulator.into_drainer()?,
            doc::Combiner::Drainer(d) => d,
        };
        let more = drainer.drain_while(|doc, _fully_reduced| {
            let doc_json = serde_json::to_string(&doc).expect("document serialization cannot fail");
            out.push(doc_json);

            Ok::<bool, doc::combine::Error>(true)
        })?;

        if more {
            self.combiner = doc::Combiner::Drainer(drainer);
        } else {
            self.combiner = doc::Combiner::Accumulator(drainer.into_new_accumulator()?);
        }

        Ok(self)
    }
}
