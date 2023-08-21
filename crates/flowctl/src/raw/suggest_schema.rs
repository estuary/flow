use crate::{
    catalog::{collect_specs, fetch_live_specs, List, LiveSpecRow, NameSelector, SpecTypeSelector},
    collection::read::{journal_reader, ReadArgs},
    ops,
};
use anyhow::anyhow;
use bytelines::AsyncByteLines;
use doc::{shape::schema::to_schema, FailedValidation, SchemaIndexBuilder, Shape};
use futures::{Stream, StreamExt, TryStreamExt};
use json::schema::build::build_schema;
use models::Schema;
use proto_flow::ops::Log;
use schema_inference::json_decoder::JsonCodec;
use std::{io::ErrorKind, pin::Pin};
use tokio::io::BufReader;
use tokio_util::{codec::FramedRead, compat::FuturesAsyncReadCompatExt};
use url::Url;

/// With some of our captures, we have an existing document schema for their collections, but we
/// frequently run into issues with these document schemas: they are sometimes completely wrong
/// about type of a field, or sometimes they are too narrow about the type (e.g. a value is marked
/// as "integer", but it is actually "number"), other times they are missing some fields that are
/// being captured into the collection.
///
/// This tool is built for the purpose of helping with updating the schema of these collections, by
/// reading data from a collection, and the corresponding task ops logs (specifically document schema
/// violation logs), and running schema inference on all documents of the collection as well as the
/// documents that violated the existing schema, to come up with a new schema that will allow those
/// documents to pass validation. The schema inference run starts with the existing task schema as
/// its starting point, and widens that schema to allow the invalid documents to pass validation.
///
/// The reason for including all documents from the collection is that the task's existing schema
/// may be missing some fields, and we want to also be able to extend the existing schema with
/// these missing fields.
///
/// The outputs of this command are two files: the original schema of the collection, and the new,
/// suggested schema of the collection. There is also a diff run on the two files automatically by
/// the command to help recognise the differences between them. This command is meant to be used by
/// a user to come up with a good JSON Merge Patch for the collection.
#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct SuggestSchema {
    #[clap(flatten)]
    args: ReadArgs,

    /// Task name to read ops logs from
    #[clap(long)]
    task: String,
}

pub async fn do_suggest_schema(
    ctx: &mut crate::CliContext,
    SuggestSchema { args, task }: &SuggestSchema,
) -> anyhow::Result<()> {
    let client = ctx.controlplane_client().await?;
    // Retrieve identified live specifications.
    let live_specs = fetch_live_specs::<LiveSpecRow>(
        client.clone(),
        &List {
            flows: true,
            name_selector: NameSelector {
                name: vec![args.selector.collection.clone()],
                prefix: Vec::new(),
            },
            type_selector: SpecTypeSelector {
                collections: Some(true),
                ..Default::default()
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

    let (_, collection_def) = collect_specs(live_specs)?
        .collections
        .pop_first()
        .ok_or(anyhow!("could not find collection"))?;

    if args.bounds.follow {
        anyhow::bail!("--follow is not supported by this command");
    }

    // Reader for the collection itself
    let reader = match journal_reader(ctx, &args).await {
        Ok(r) => Some(r),
        Err(e)
            if e.to_string()
                .contains("does not exist or has never been written to") =>
        {
            None
        }
        Err(e) => anyhow::bail!(e),
    };

    // Read log lines from the logs collection and filter "failed validation" documents
    let read_args = ops::read_args(task, ops::OpsCollection::Logs, &args.bounds, true);
    let log_reader = journal_reader(ctx, &read_args).await?;
    let log_stream = AsyncByteLines::new(BufReader::new(log_reader.compat())).into_stream();
    let log_invalid_documents = log_stream.try_filter_map(|log| async move {
        let parsed: Log = serde_json::from_slice(&log)?;
        if parsed.message != "document failed validation against its collection JSON Schema" {
            return Ok(None);
        }

        let error_string = &parsed
            .fields_json_map
            .get("error")
            .ok_or(to_io_error("could not get 'error' field of ops log"))?;
        let mut err: Vec<serde_json::Value> = serde_json::from_str(error_string)?;
        let err_object = err.pop().ok_or(to_io_error(
            "could not get second element of 'error' field in ops log",
        ))?;
        let failed_validation: FailedValidation = serde_json::from_value(err_object)?;

        return Ok(Some(failed_validation.document));
    });

    let codec = JsonCodec::new(); // do we want to limit length here? LinesCodec::new_with_max_length(...) does this

    // Chain together the collection document reader and the log_invalid_documents stream so we can
    // run schema-inference on both
    let mut docs_stream: Pin<Box<dyn Stream<Item = Result<serde_json::Value, std::io::Error>>>> =
        if let Some(reader) = reader {
            Box::pin(
                FramedRead::new(FuturesAsyncReadCompatExt::compat(reader), codec)
                    .map_err(to_io_error)
                    .chain(log_invalid_documents),
            )
        } else {
            Box::pin(log_invalid_documents)
        };

    // The original collection schema to be used as the starting point of schema-inference
    let schema_model = collection_def
        .schema
        .as_ref()
        .or(collection_def.read_schema.as_ref())
        .expect("collection must define either schema or readSchema");
    // The inferred shape, we start by using the existing schema of the collection
    let mut inferred_shape = raw_schema_to_shape(&schema_model)?;

    // Create a JSONSchema object from the original schema so we can use it to run a diff later
    // The reason for this is that this allows us to have JSONSchema outputs that have a similar
    // structure, allowing us to do a more intuitive diff
    let original_jsonschema = to_schema(inferred_shape.clone());

    loop {
        match docs_stream.next().await {
            Some(Ok(parsed)) => {
                if parsed.pointer("/_meta/ack").is_some() {
                    continue;
                }
                inferred_shape.widen(&parsed);
            }
            Some(Err(e)) => return Err(e.into()),
            None => break,
        }
    }

    // Build a new JSONSchema from the updated inferred shape
    let new_jsonschema = to_schema(inferred_shape);

    let collection_name = args.selector.collection.split("/").last().unwrap();

    let original_schema_file_name = format!("{collection_name}.original.schema.json");
    std::fs::write(
        &original_schema_file_name,
        serde_json::to_string_pretty(&original_jsonschema)?,
    )?;

    let new_schema_file_name = format!("{collection_name}.new.schema.json");
    std::fs::write(
        &new_schema_file_name,
        serde_json::to_string_pretty(&new_jsonschema)?,
    )?;

    eprintln!("Wrote {original_schema_file_name} and {new_schema_file_name}.");

    // git diff is much better at diffing JSON structures, it is pretty smart to show the diff in a
    // way that is human-readable and understandable, and doesn't mess up the JSON structure.
    // the --no-index option allows us to use git diff without being in a git repository
    std::process::Command::new("git")
        .args([
            "diff",
            "--no-index",
            &original_schema_file_name,
            &new_schema_file_name,
        ])
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

    return Ok(Shape::infer(&root, &index));
}

fn to_io_error<T: ToString>(message: T) -> std::io::Error {
    std::io::Error::new(ErrorKind::Other, message.to_string())
}
