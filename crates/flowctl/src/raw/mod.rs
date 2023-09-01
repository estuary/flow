use crate::local_specs;
use anyhow::Context;
use doc::combine;
use std::{
    io::{self, Write},
    path::PathBuf,
};

mod capture;
mod discover;
mod materialize_fixture;
mod suggest_schema;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Advanced {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    /// Issue a custom table select request to the API.
    ///
    /// Requests are issued to a specific --table and support optional
    /// query parameters that can identify columns to return, apply
    /// filters, and more.
    /// Consult the PostgREST documentation for detailed usage:
    /// https://postgrest.org/
    ///
    /// Pass query arguments as multiple `-q key=value` arguments.
    /// For example: `-q select=col1,col2 -q col3=eq.MyValue`
    Get(Get),
    /// Issue a custom RPC request to the API.
    ///
    /// Requests are issued to a specific --function and require a
    /// request --body. As with `get`, you may pass optional query
    /// parameters.
    Rpc(Rpc),
    /// Issue a custom table update request to the API.
    Update(Update),
    /// Perform a configured build of catalog sources.
    Build(Build),
    /// Bundle catalog sources into a flattened and inlined catalog.
    Bundle(Bundle),
    /// Combine over an input stream of documents and write the output.
    Combine(Combine),
    /// Generate a materialization fixture.
    MaterializeFixture(materialize_fixture::MaterializeFixture),
    /// Discover a connector and write catalog files
    Discover(discover::Discover),
    /// Run a capture connector and combine its documents
    Capture(capture::Capture),
    /// Suggest a schema that would alleviate document schema violations of a specific collection
    SuggestSchema(suggest_schema::SuggestSchema),
    /// Emit the Flow specification JSON-Schema.
    JsonSchema,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Get {
    /// Table to select from.
    #[clap(long)]
    table: String,
    /// Optional query parameters.
    #[clap(long, parse(try_from_str = parse_key_val), number_of_values = 1)]
    query: Vec<(String, String)>,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Update {
    /// Table to update.
    #[clap(long)]
    table: String,
    /// Optional query parameters.
    #[clap(long, parse(try_from_str = parse_key_val), number_of_values = 1)]
    query: Vec<(String, String)>,
    /// Serialized JSON argument of the request.
    #[clap(long)]
    body: String,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Rpc {
    /// RPC function to invoke.
    #[clap(long)]
    function: String,
    /// Optional query parameters.
    #[clap(long, parse(try_from_str = parse_key_val), number_of_values = 1)]
    query: Vec<(String, String)>,
    /// Serialized JSON argument of the request.
    #[clap(long)]
    body: String,
}

#[derive(Debug, Clone, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Build {
    #[clap(long)]
    db_path: PathBuf,
    #[clap(long)]
    build_id: String,
    #[clap(long, default_value = "")]
    connector_network: String,
    #[clap(long, default_value = "/")]
    file_root: String,
    #[clap(long)]
    source: String,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Bundle {
    /// Path or URL to a Flow specification file to bundle.
    #[clap(long)]
    source: String,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Combine {
    /// Path or URL to a Flow specification file.
    #[clap(long)]
    source: String,
    /// Name of a collection in the Flow specification file.
    #[clap(long)]
    collection: String,
}

impl Advanced {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        match &self.cmd {
            Command::Get(get) => do_get(ctx, get).await,
            Command::Update(update) => do_update(ctx, update).await,
            Command::Rpc(rpc) => do_rpc(ctx, rpc).await,
            Command::Build(build) => do_build(ctx, build).await,
            Command::Bundle(bundle) => do_bundle(ctx, bundle).await,
            Command::Combine(combine) => do_combine(ctx, combine).await,
            Command::MaterializeFixture(fixture) => {
                materialize_fixture::do_materialize_fixture(ctx, fixture).await
            }
            Command::Discover(args) => discover::do_discover(ctx, args).await,
            Command::Capture(args) => capture::do_capture(ctx, args).await,
            Command::SuggestSchema(args) => suggest_schema::do_suggest_schema(ctx, args).await,
            Command::JsonSchema => {
                let schema = models::Catalog::root_json_schema();
                Ok(serde_json::to_writer_pretty(std::io::stdout(), &schema)?)
            }
        }
    }
}

async fn do_get(ctx: &mut crate::CliContext, Get { table, query }: &Get) -> anyhow::Result<()> {
    let client = ctx.controlplane_client().await?;
    let req = client.from(table).build().query(query);
    tracing::debug!(?req, "built request to execute");

    println!("{}", req.send().await?.text().await?);
    Ok(())
}

async fn do_update(
    ctx: &mut crate::CliContext,
    Update { table, query, body }: &Update,
) -> anyhow::Result<()> {
    let client = ctx.controlplane_client().await?;
    let req = client.from(table).update(body).build().query(query);
    tracing::debug!(?req, "built request to execute");

    println!("{}", req.send().await?.text().await?);
    Ok(())
}

async fn do_rpc(
    ctx: &mut crate::CliContext,
    Rpc {
        function,
        query,
        body,
    }: &Rpc,
) -> anyhow::Result<()> {
    let client = ctx.controlplane_client().await?;
    let req = client.rpc(function, body).build().query(query);
    tracing::debug!(?req, "built request to execute");

    println!("{}", req.send().await?.text().await?);
    Ok(())
}

async fn do_build(ctx: &mut crate::CliContext, build: &Build) -> anyhow::Result<()> {
    let client = ctx.controlplane_client().await?;

    let Build {
        db_path,
        build_id,
        connector_network,
        file_root,
        source,
    } = build.clone();

    let source_url = build::arg_source_to_url(&source, false)?;
    let project_root = build::project_root(&source_url);

    let build_result = build::managed_build(
        build_id.clone(),
        connector_network.clone(),
        Box::new(local_specs::Resolver { client }),
        file_root.clone().into(),
        ops::tracing_log_handler,
        project_root.clone(),
        source_url,
    )
    .await;

    // build_api::Config is a legacy metadata structure which we still
    // expect and validate when we open up a DB from Go.
    let build_config = proto_flow::flow::build_api::Config {
        build_db: db_path.to_string_lossy().to_string(),
        build_id,
        source,
        source_type: proto_flow::flow::ContentType::Catalog as i32,
        ..Default::default()
    };

    build::persist(build_config, &db_path, &build_result)?;

    Ok(())
}

async fn do_bundle(ctx: &mut crate::CliContext, Bundle { source }: &Bundle) -> anyhow::Result<()> {
    let (sources, _) =
        local_specs::load_and_validate(ctx.controlplane_client().await?, source).await?;
    serde_json::to_writer_pretty(io::stdout(), &local_specs::into_catalog(sources))?;
    Ok(())
}

async fn do_combine(
    ctx: &mut crate::CliContext,
    Combine { source, collection }: &Combine,
) -> anyhow::Result<()> {
    let (_sources, validations) =
        local_specs::load_and_validate(ctx.controlplane_client().await?, source).await?;

    let collection = match validations
        .built_collections
        .binary_search_by_key(&collection.as_str(), |c| c.collection.as_str())
    {
        Ok(index) => &validations.built_collections[index],
        Err(_) => anyhow::bail!("collection {collection} not found"),
    };

    let schema = if collection.spec.read_schema_json.is_empty() {
        doc::validation::build_bundle(&collection.spec.write_schema_json).unwrap()
    } else {
        doc::validation::build_bundle(&collection.spec.read_schema_json).unwrap()
    };

    let mut accumulator = combine::Accumulator::new(
        extractors::for_key(&collection.spec.key, &collection.spec.projections)?.into(),
        None,
        tempfile::tempfile().context("opening tempfile")?,
        doc::Validator::new(schema).unwrap(),
    )?;

    let mut in_docs = 0usize;
    let mut in_bytes = 0usize;
    let mut out_docs = 0usize;
    // We don't track out_bytes because it's awkward to do so
    // and the user can trivially measure for themselves.

    for line in io::stdin().lines() {
        let line = line?;

        let memtable = accumulator.memtable()?;
        let rhs = doc::HeapNode::from_serde(
            &mut serde_json::Deserializer::from_str(&line),
            memtable.alloc(),
        )?;

        in_docs += 1;
        in_bytes += line.len() + 1;
        memtable.add(rhs, false)?;
    }

    let mut out = io::BufWriter::new(io::stdout().lock());

    let mut drainer = accumulator.into_drainer()?;
    assert_eq!(
        false,
        drainer.drain_while(|node, _fully_reduced| {
            serde_json::to_writer(&mut out, &node).context("writing document to stdout")?;
            out.write(b"\n")?;
            out_docs += 1;
            Ok::<_, anyhow::Error>(true)
        })?,
        "implementation error: drain_while exited with remaining items to drain"
    );
    out.flush()?;

    tracing::info!(
        input_docs = in_docs,
        input_bytes = in_bytes,
        output_docs = out_docs,
        "completed combine"
    );

    Ok(())
}

fn parse_key_val<T, U>(s: &str) -> anyhow::Result<(T, U)>
where
    T: std::str::FromStr,
    T::Err: Into<anyhow::Error>,
    U: std::str::FromStr,
    U::Err: Into<anyhow::Error>,
{
    let pos = match s.find('=') {
        Some(pos) => pos,
        None => anyhow::bail!("invalid KEY=value: no `=` found in `{s}`"),
    };
    Ok((
        s[..pos].parse().map_err(Into::into)?,
        s[pos + 1..].parse().map_err(Into::into)?,
    ))
}
