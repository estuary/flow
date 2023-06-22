use anyhow::Context;
use bytes::Bytes;
use doc::{inference::Shape, SchemaIndexBuilder, FailedValidation};
use futures::{TryStreamExt, StreamExt};
use journal_client::{broker, read::uncommitted::{ReadUntil, ReadStart, JournalRead, ExponentialBackoff, Reader}, list::list_journals, AuthHeader};
use json::schema::{build::build_schema, types};
use models::{
    Capture, CaptureBinding, CaptureDef, CaptureEndpoint, Catalog, Collection, CollectionDef,
    CompositeKey, ConnectorConfig, JsonPointer, Schema, ShardTemplate,
};
use proto_flow::{
    capture::{request, Request},
    flow::capture_spec::ConnectorType,
    ops::Log
};
use bytelines::AsyncByteLines;
use proto_grpc::broker::journal_client::JournalClient;
use schema_inference::{json_decoder::JsonCodec, inference::infer_shape, shape, server::InferenceError, schema::SchemaBuilder};
use serde_json::{json, value::RawValue, Value};
use tokio::io::{BufReader, AsyncReadExt};
use tokio_util::{compat::FuturesAsyncReadCompatExt, io::{ReaderStream, StreamReader}, codec::FramedRead};
use tonic::{codegen::InterceptedService, transport::Channel};
use std::{collections::BTreeMap, io::ErrorKind, time::{Duration, Instant}};
use url::Url;

use crate::{connector::docker_run, catalog::{fetch_live_specs, List, SpecTypeSelector, NameSelector, collect_specs}, dataplane::{self, fetch_data_plane_access_token}};
use crate::local_specs;
use anyhow::anyhow;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct SuggestSchema {
    /// Collection name
    #[clap(long)]
    collection: String,

    /// Task name
    #[clap(long)]
    task: String,
}

pub async fn do_suggest_schema(
    ctx: &mut crate::CliContext,
    SuggestSchema {
        collection,
        task,
    }: &SuggestSchema,
) -> anyhow::Result<()> {
    let client = ctx.controlplane_client().await?;
    // Retrieve identified live specifications.
    let live_specs = fetch_live_specs(
        client.clone(),
        &List {
            flows: true,
            name_selector: NameSelector {
                name: vec![collection.clone()],
                prefix: Vec::new()
            },
            type_selector: SpecTypeSelector {
                collections: Some(true),
                captures: Some(false),
                materializations: Some(false),
                tests: Some(false),
            },
            deleted: false, // deleted specs have nothing to pull
        },
        vec![
            "catalog_name",
            "id",
            "updated_at",
            "last_pub_user_email",
            "last_pub_user_full_name",
            "last_pub_user_id",
            "spec_type",
            "spec",
        ],
    )
    .await?;

    let (key, collection_def) = collect_specs(live_specs)?.collections.pop_first().ok_or(anyhow!("could not find collection"))?;

    let mut data_plane_client =
        dataplane::journal_client_for(client, vec![collection.clone()]).await?;

    // Reader for the collection itself
    let selector = broker::LabelSelector {
        include: Some(broker::LabelSet { labels: vec![broker::Label{
            name: labels::COLLECTION.to_string(),
            value: collection.clone(),
        }]}),
        exclude: None,
    };
    let reader = journal_reader(&mut data_plane_client, &selector).await?;

    // Reader for the ops log of the task
    let ops_collection = "ops.us-central1.v1/logs".to_string();
    let selector = broker::LabelSelector {
        include: Some(broker::LabelSet {
            labels: vec![
                broker::Label {
                    name: labels::COLLECTION.to_string(),
                    value: ops_collection.clone(),
                }, broker::Label {
                    name: format!("{}kind", labels::FIELD_PREFIX),
                    value: "capture".to_string(),
                }, broker::Label {
                    name: format!("{}name", labels::FIELD_PREFIX),
                    value: urlencoding::encode(task).into_owned(),
                }]
        }),
        exclude: None,
    };

    let client = ctx.controlplane_client().await?;
    let mut data_plane_client =
        dataplane::journal_client_for(client, vec![ops_collection]).await?;
    let log_reader = journal_reader(&mut data_plane_client, &selector).await?;
    let mut log_stream = AsyncByteLines::new(BufReader::new(log_reader.compat())).into_stream();
    let mut log_invalid_documents = log_stream.try_filter_map(|log| async move {
        let parsed: Log = serde_json::from_slice(&log)?;
        if parsed.message != "document failed validation against its collection JSON Schema" {
            return Ok(None);
        }

        let error_string = &parsed.fields_json_map.get("error").ok_or(std::io::Error::new(ErrorKind::Other, "could not get 'error' field of ops log"))?;
        let mut err: Vec<serde_json::Value> = serde_json::from_str(error_string)?;
        let err_object = err.pop().ok_or(std::io::Error::new(ErrorKind::Other, "could not get second element of 'error' field in ops log"))?;
        let failed_validation: FailedValidation = serde_json::from_value(err_object)?;

        let buf: Bytes = serde_json::to_vec(&failed_validation.document)?.into();
        return Ok(Some(buf));
    });

    let mut log_invalid_documents_reader = StreamReader::new(Box::pin(log_invalid_documents));

    let codec = JsonCodec::new(); // do we want to limit length here? LinesCodec::new_with_max_length(...) does this
    let mut doc_bytes_stream = FramedRead::new(FuturesAsyncReadCompatExt::compat(reader).chain(log_invalid_documents_reader), codec);

    let schema_model = collection_def.schema.unwrap();

    let mut accumulator = raw_schema_to_shape(&schema_model)?;
    let original_schema_final = SchemaBuilder::new(accumulator.clone()).root_schema();

    loop {
        match doc_bytes_stream.next().await {
            Some(Ok(doc_val)) => {
                let parsed: Value = doc_val;
                // There should probably be a higher-level API for this in `journal-client`

                if parsed.pointer("/_meta/ack").is_none() {
                    let inferred_shape = infer_shape(&parsed);

                    accumulator = shape::merge(accumulator, inferred_shape)
                }
            }
            Some(Err(e)) => return Err(InferenceError::from(e).into()),
            None => break
        }
    }

    let new_schema = serde_json::to_value(&SchemaBuilder::new(accumulator).root_schema())?;

    std::fs::write("original.schema.json", serde_json::to_string_pretty(&original_schema_final)?)?;
    std::fs::write("new.schema.json", serde_json::to_string_pretty(&new_schema)?)?;

    eprintln!("Wrote original.schema.json and new.schema.json.");

    std::process::Command::new("git")
        .args(["diff", "--no-index", "original.schema.json", "new.schema.json"])
        .status()
        .expect("git diff failed");

    Ok(())
}

fn raw_schema_to_shape(schema: &Schema) -> anyhow::Result<Shape> {
    let value = serde_json::to_value(&schema)?;
    let mut index = SchemaIndexBuilder::new();
    let curi = Url::parse("https://example/schema").unwrap();
    let root = build_schema(curi, &value)?;
    index.add(&root).unwrap();
    index.verify_references().unwrap();
    let index = index.into_index();

    return Ok(Shape::infer(&root, &index))
}

async fn journal_reader(
    mut data_plane_client: &mut JournalClient<InterceptedService<Channel, AuthHeader>>,
    selector: &broker::LabelSelector
) -> anyhow::Result<Reader<ExponentialBackoff>> {
    tracing::debug!(?selector, "journal label selector");

    let mut journals = list_journals(&mut data_plane_client, &selector)
        .await
        .context("listing journals for collection read")?;
    tracing::debug!(journal_count = journals.len(), selector = ?selector, "listed journals");
    let maybe_journal = journals.pop();
    if !journals.is_empty() {
        // TODO: implement a sequencer and allow reading from multiple journals
        anyhow::bail!("flowctl is not yet able to read from partitioned collections (coming soon)");
    }

    let journal = maybe_journal.ok_or_else(|| {
        anyhow::anyhow!(
            "collection '{:#?}' does not exist or has never been written to (it has no journals)",
            selector
        )
    })?;

    let read = JournalRead::new(journal.name.clone())
        .starting_at(ReadStart::Offset(0))
        .read_until(ReadUntil::WriteHead);

    tracing::debug!(journal = %journal.name, "starting read of journal");

    // It would seem unusual for a CLI to retry indefinitely, so limit the number of retries.
    let backoff = ExponentialBackoff::new(5);
    let reader = Reader::start_read(data_plane_client.clone(), read, backoff);

    return Ok(reader);
}
