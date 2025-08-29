use crate::{
    collection::read::ReadBounds,
    ops::{OpsCollection, TaskSelector},
};
use anyhow::Context;
use std::io::Write;

#[derive(clap::Args, Debug)]
pub struct Stats {
    #[clap(flatten)]
    pub task: TaskSelector,

    #[clap(flatten)]
    pub bounds: ReadBounds,

    /// Read raw data from stats journals, including possibly uncommitted or rolled back transactions.
    /// This flag is currently required, but will be made optional in the future as we add support for
    /// committed reads, which will become the default.
    #[clap(long)]
    pub uncommitted: bool,

    /// Aggregate stats using the specified time window: 1s, 1m, 10m, 1h, 1d
    #[clap(long, value_parser = parse_aggregate_window)]
    pub aggregate: Option<AggregateWindow>,
}

#[derive(Debug, Clone, Copy)]
pub enum AggregateWindow {
    OneSecond,
    OneMinute,
    TenMinutes,
    OneHour,
    OneDay,
}

fn parse_aggregate_window(s: &str) -> anyhow::Result<AggregateWindow> {
    match s {
        "1s" => Ok(AggregateWindow::OneSecond),
        "1m" => Ok(AggregateWindow::OneMinute),
        "10m" => Ok(AggregateWindow::TenMinutes),
        "1h" => Ok(AggregateWindow::OneHour),
        "1d" => Ok(AggregateWindow::OneDay),
        _ => anyhow::bail!(
            "Invalid aggregate window: {}. Valid values are: 1s, 1m, 10m, 1h, 1d",
            s
        ),
    }
}

impl Stats {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        if self.aggregate.is_none() {
            // No aggregation, use the original implementation
            return crate::ops::read_task_ops_journal(
                &ctx.client,
                &self.task.task,
                OpsCollection::Stats,
                &self.bounds,
            )
            .await;
        }

        // Custom implementation with aggregation
        let (_shard_id_prefix, _ops_logs_journal, ops_stats_journal, _shard_client, journal_client) =
            flow_client::fetch_user_task_authorization(&ctx.client, &self.task.task).await?;

        let accumulator = self.init_accumulator(&ctx.client).await?;
        self.read_and_aggregate_stats(accumulator, journal_client, &ops_stats_journal)
            .await
    }

    async fn init_accumulator(
        &self,
        client: &crate::Client,
    ) -> anyhow::Result<doc::combine::Accumulator> {
        // Fetch the stats collection spec from control plane so we can get the bundled schema.
        // TODO(phil): Determine the actual stats collection for the specific task instead of assuming they're all the same.
        const STATS_COLLECTION: &str = "ops/tasks/public/gcp-us-central1-c2/stats";

        #[derive(serde::Deserialize)]
        struct LiveSpec {
            spec: models::CollectionDef,
        }

        let live_specs: Vec<LiveSpec> = crate::api_exec(
            client
                .from("live_specs")
                .select("spec")
                .eq("catalog_name", STATS_COLLECTION)
                .limit(1),
        )
        .await?;

        let live_spec = live_specs
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("Stats collection {} not found", STATS_COLLECTION))?;

        let schema_bytes = live_spec
            .spec
            .schema
            .ok_or_else(|| anyhow::anyhow!("ops collection def is missing schema"))?;

        // Initialize combiner with stats schema
        let validator = doc::validation::build_bundle(schema_bytes.get().as_bytes())?;
        let uri = validator.curi.clone();
        let spec = doc::combine::Spec::with_one_binding(
            true, // full reductions
            vec![doc::Extractor::new("/ts", &doc::SerPolicy::noop())],
            "stats",
            Some(uri),
            doc::Validator::new(validator)?,
        );

        let accumulator = doc::combine::Accumulator::new(
            spec,
            tempfile::tempfile().context("opening tempfile")?,
        )?;
        Ok(accumulator)
    }

    async fn read_and_aggregate_stats(
        &self,
        accumulator: doc::combine::Accumulator,
        journal_client: gazette::journal::Client,
        journal_name: &str,
    ) -> anyhow::Result<()> {
        use futures::StreamExt;
        use gazette::journal::ReadJsonLine;
        use proto_gazette::broker;

        let aggregate_window = self
            .aggregate
            .expect("aggregate window should be set when this method is called");

        // Set up journal reading
        let begin_mod_time = if let Some(since) = self.bounds.since {
            let start_time = time::OffsetDateTime::now_utc() - *since;
            (start_time - time::OffsetDateTime::UNIX_EPOCH).as_seconds_f64() as i64
        } else {
            0
        };

        let mut lines = journal_client.read_json_lines(
            broker::ReadRequest {
                journal: journal_name.to_string(),
                offset: 0,
                block: self.bounds.follow,
                begin_mod_time,
                ..Default::default()
            },
            1,
        );

        let mut current_window: Option<String> = None;
        let open_secs_extractor = doc::Extractor::new("/openSecondsTotal", &doc::SerPolicy::noop());

        let mut accumulator = Some(accumulator);

        while let Some(line) = lines.next().await {
            match line {
                Ok(ReadJsonLine::Meta(broker::ReadResponse {
                    fragment,
                    write_head,
                    ..
                })) => {
                    tracing::debug!(?fragment, %write_head, "journal metadata");
                }
                Ok(ReadJsonLine::Doc {
                    root,
                    next_offset: _,
                }) => {
                    let doc = root.get();

                    // Check if document has openSecondsTotal property
                    if open_secs_extractor.query(doc).is_err() {
                        continue;
                    }

                    let mut doc_val = serde_json::to_value(doc::SerPolicy::noop().on(doc)).unwrap();
                    // Get and truncate timestamp
                    let Some(ts_value) = doc_val.pointer("/ts").and_then(|p| p.as_str()) else {
                        continue;
                    };

                    let truncated_ts = self.truncate_timestamp(ts_value, aggregate_window)?;

                    // Check if we've moved to a new time window
                    if let Some(ref current) = current_window {
                        if truncated_ts != *current {
                            // Drain current accumulator and output results
                            let mut drainer = accumulator
                                .take()
                                .expect("accumulator must be Some")
                                .into_drainer()?;
                            self.drain_and_output(&mut drainer)?;
                            current_window = Some(truncated_ts.clone());
                            accumulator = Some(drainer.into_new_accumulator()?);
                        }
                    } else {
                        current_window = Some(truncated_ts.clone());
                    }

                    doc_val["ts"] = serde_json::Value::String(truncated_ts);

                    // Add to combiner
                    let memtable = accumulator
                        .as_mut()
                        .expect("accumulator must be Some")
                        .memtable()?;
                    let heap_doc = doc::HeapNode::from_node(&doc_val, memtable.alloc());
                    memtable.add(0, heap_doc, false)?;
                }
                Err(gazette::RetryError {
                    inner: err,
                    attempt,
                }) => match err {
                    err if err.is_transient() => {
                        tracing::warn!(?err, %attempt, "error reading stats journal (will retry)");
                    }
                    gazette::Error::BrokerStatus(broker::Status::Suspended)
                        if self.bounds.follow =>
                    {
                        tracing::debug!(?err, %attempt, "journal is suspended (will retry)");
                    }
                    gazette::Error::BrokerStatus(
                        status @ broker::Status::OffsetNotYetAvailable
                        | status @ broker::Status::Suspended,
                    ) => {
                        tracing::debug!(?status, "stopping read at end of journal content");
                        break;
                    }
                    err => anyhow::bail!(err),
                },
            }
        }

        // Drain any remaining data
        if current_window.is_some() {
            let mut drainer = accumulator
                .take()
                .expect("accumulator must be Some")
                .into_drainer()?;
            self.drain_and_output(&mut drainer)?;
        }

        Ok(())
    }

    fn truncate_timestamp(&self, ts: &str, window: AggregateWindow) -> anyhow::Result<String> {
        use time::format_description::well_known::Rfc3339;
        use time::OffsetDateTime;

        let dt = OffsetDateTime::parse(ts, &Rfc3339)?;

        let truncated = match window {
            AggregateWindow::OneSecond => dt.replace_nanosecond(0)?,
            AggregateWindow::OneMinute => dt.replace_second(0)?.replace_nanosecond(0)?,
            AggregateWindow::TenMinutes => {
                let minute = dt.minute();
                let truncated_minute = (minute / 10) * 10;
                dt.replace_minute(truncated_minute)?
                    .replace_second(0)?
                    .replace_nanosecond(0)?
            }
            AggregateWindow::OneHour => dt
                .replace_minute(0)?
                .replace_second(0)?
                .replace_nanosecond(0)?,
            AggregateWindow::OneDay => dt.replace_time(time::Time::MIDNIGHT),
        };

        Ok(truncated.format(&Rfc3339)?)
    }

    fn drain_and_output(&self, drainer: &mut doc::combine::Drainer) -> anyhow::Result<()> {
        let policy = doc::SerPolicy::noop();
        let sout = std::io::stdout();
        let mut stdout = sout.lock();

        while let Some(drained) = drainer.next() {
            let drained = drained?;
            let mut v = match drained.root {
                doc::OwnedNode::Archived(owned_archived_node) => {
                    serde_json::to_vec(&policy.on(owned_archived_node.get()))?
                }
                doc::OwnedNode::Heap(owned_heap_node) => {
                    serde_json::to_vec(&policy.on(owned_heap_node.get()))?
                }
            };
            v.push(b'\n');
            stdout.write_all(&v)?;
        }
        stdout.flush()?;

        Ok(())
    }
}
