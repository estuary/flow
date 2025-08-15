use crate::{
    collection::read::ReadBounds,
    local_specs,
    ops::{OpsCollection, TaskSelector},
};
use anyhow::Context;
use cleanup_ops_journals::CleanupOpsJournals;
use doc::combine;
use std::{
    io::{self, Write},
    path::PathBuf,
};
use tables::CatalogResolver;

mod cleanup_ops_journals;
mod discover;
mod materialize_fixture;
mod oauth;
mod shards;
mod spec;

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
    /// Get the spec output of a connector
    Spec(spec::Spec),
    /// Test a connector's OAuth config
    Oauth(oauth::Oauth),
    /// Emit the Flow specification JSON-Schema.
    JsonSchema,
    /// Read stats collection documents
    Stats(Stats),
    /// Stream logs associated with the given bearer token.
    BearerLogs(BearerLogs),
    /// Print information about the shards for a given task
    ListShards(TaskSelector),
    /// Print environment variables for working with a given data-plane
    /// and prefix using Gazette's `gazctl`.
    GazctlEnv(GazctlEnv),

    CleanupOpsJournals(CleanupOpsJournals),
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Get {
    /// Table to select from.
    #[clap(long)]
    table: String,
    /// Optional query parameters.
    #[clap(long, value_parser = parse_key_val::<String, String>, number_of_values = 1)]
    query: Vec<(String, String)>,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Update {
    /// Table to update.
    #[clap(long)]
    table: String,
    /// Optional query parameters.
    #[clap(long, value_parser = parse_key_val::<String, String>, number_of_values = 1)]
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
    #[clap(long, value_parser = parse_key_val::<String, String>, number_of_values = 1)]
    query: Vec<(String, String)>,
    /// Serialized JSON argument of the request.
    #[clap(long)]
    body: String,
}

#[derive(Debug, Clone, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Build {
    /// Path of database to build.
    #[clap(long)]
    db_path: PathBuf,
    /// Publication ID of this build.
    #[clap(long, default_value = "ff:ff:ff:ff:ff:ff:ff:ff")]
    pub_id: models::Id,
    /// Build ID of this build.
    #[clap(long)]
    build_id: models::Id,
    /// Docker network to use for connectors.
    #[clap(long, default_value = "")]
    connector_network: String,
    /// File root which jails local file:// resources.
    #[clap(long, default_value = "/")]
    file_root: String,
    /// Source file or URL from which to load the draft catalog.
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
}

impl Stats {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        crate::ops::read_task_ops_journal(
            &ctx.client,
            &self.task.task,
            OpsCollection::Stats,
            &self.bounds,
        )
        .await
    }
}

#[derive(clap::Args, Debug)]
pub struct BearerLogs {
    /// Bearer logs token.
    #[clap(long)]
    pub token: uuid::Uuid,
    /// Start reading from this far in the past.
    #[clap(long, default_value = "1h")]
    pub since: Option<humantime::Duration>,
}

impl BearerLogs {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let bound = match self.since {
            None => None,
            Some(since) => {
                let since: std::time::Duration = since.into();
                Some(crate::Timestamp::from_unix_timestamp(
                    (std::time::SystemTime::now() - since)
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64,
                )?)
            }
        };

        crate::poll::stream_logs(&ctx.client, &self.token.to_string(), bound).await
    }
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
            Command::Spec(args) => spec::do_spec(ctx, args).await,
            Command::Oauth(args) => oauth::do_oauth(ctx, args).await,
            Command::JsonSchema => {
                let schema = models::Catalog::root_json_schema();
                Ok(serde_json::to_writer_pretty(std::io::stdout(), &schema)?)
            }
            Command::Stats(stats) => stats.run(ctx).await,
            Command::BearerLogs(bearer_logs) => bearer_logs.run(ctx).await,
            Command::ListShards(selector) => shards::do_list_shards(ctx, selector).await,
            Command::GazctlEnv(gazctl_env) => gazctl_env.run(ctx).await,
            Command::CleanupOpsJournals(cleanup) => cleanup.cleanup_ops_journals(ctx).await,
        }
    }
}

async fn do_get(ctx: &mut crate::CliContext, Get { table, query }: &Get) -> anyhow::Result<()> {
    let req = ctx.client.from(table).build().query(query);
    tracing::debug!(?req, "built request to execute");

    println!("{}", req.send().await?.text().await?);
    Ok(())
}

async fn do_update(
    ctx: &mut crate::CliContext,
    Update { table, query, body }: &Update,
) -> anyhow::Result<()> {
    let req = ctx.client.from(table).update(body).build().query(query);
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
    let req = ctx.client.rpc(function, body.clone()).build().query(query);
    tracing::debug!(?req, "built request to execute");

    println!("{}", req.send().await?.text().await?);
    Ok(())
}

async fn do_build(ctx: &mut crate::CliContext, build: &Build) -> anyhow::Result<()> {
    let resolver = local_specs::Resolver {
        client: ctx.client.clone(),
    };

    let Build {
        db_path,
        pub_id,
        build_id,
        connector_network,
        file_root,
        source,
    } = build.clone();

    let source_url = build::arg_source_to_url(&source, false)?;
    let project_root = build::project_root(&source_url);

    let draft = build::load(&source_url, std::path::Path::new(&file_root)).await;
    let draft = local_specs::surface_errors(draft.into_result())?;

    let live = resolver.resolve(draft.all_catalog_names()).await;
    let live = local_specs::surface_errors(live.into_result())?;

    let output = build::validate(
        pub_id,
        build_id,
        true, // Allow local connectors.
        &connector_network,
        ops::tracing_log_handler,
        false, // Don't no-op captures.
        false, // Don't no-op derivations.
        false, // Don't no-op materializations.
        &project_root,
        draft,
        live,
    )
    .await;

    // build_api::Config is a legacy metadata structure which we still
    // expect and validate when we open up a DB from Go.
    let build_config = proto_flow::flow::build_api::Config {
        build_db: db_path.to_string_lossy().to_string(),
        build_id: build_id.to_string(),
        source,
        source_type: proto_flow::flow::ContentType::Catalog as i32,
        ..Default::default()
    };

    build::persist(build_config, &db_path, &output)?;

    Ok(())
}

async fn do_bundle(_ctx: &mut crate::CliContext, Bundle { source }: &Bundle) -> anyhow::Result<()> {
    let source = build::arg_source_to_url(source, false)?;
    let mut draft = local_specs::surface_errors(local_specs::load(&source).await.into_result())?;
    ::sources::inline_draft_catalog(&mut draft);
    serde_json::to_writer_pretty(io::stdout(), &local_specs::into_catalog(draft))?;
    Ok(())
}

async fn do_combine(
    ctx: &mut crate::CliContext,
    Combine { source, collection }: &Combine,
) -> anyhow::Result<()> {
    let (_sources, validations) = local_specs::load_and_validate(&ctx.client, source).await?;

    let collection = match validations
        .built_collections
        .binary_search_by_key(&collection.as_str(), |c| c.collection.as_str())
    {
        Ok(index) => &validations.built_collections[index],
        Err(_) => anyhow::bail!("collection {collection} not found"),
    };
    let spec = collection.spec.as_ref().expect("not a deletion");

    let schema = if spec.read_schema_json.is_empty() {
        doc::validation::build_bundle(&spec.write_schema_json).unwrap()
    } else {
        doc::validation::build_bundle(&spec.read_schema_json).unwrap()
    };

    let mut accumulator = combine::Accumulator::new(
        combine::Spec::with_one_binding(
            true, // Full reductions. Make this an option?
            extractors::for_key(&spec.key, &spec.projections, &doc::SerPolicy::noop())?,
            "source",
            None,
            doc::Validator::new(schema).unwrap(),
        ),
        tempfile::tempfile().context("opening tempfile")?,
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
        memtable.add(0, rhs, false)?;
    }

    let mut out = io::BufWriter::new(io::stdout().lock());

    let mut drainer = accumulator.into_drainer()?;
    while let Some(drained) = drainer.next() {
        let drained = drained?;

        serde_json::to_writer(&mut out, &doc::SerPolicy::noop().on_owned(&drained.root))
            .context("writing document to stdout")?;
        out.write(b"\n")?;
        out_docs += 1;
    }
    out.flush()?;

    tracing::info!(
        input_docs = in_docs,
        input_bytes = in_bytes,
        output_docs = out_docs,
        "completed combine"
    );

    Ok(())
}

#[derive(clap::Args, Debug)]
pub struct GazctlEnv {
    /// Name of the data-plane to work with.
    #[clap(long)]
    pub data_plane: Option<String>,
    /// Journal and shard prefix to request authorization for.
    #[clap(long)]
    pub prefix: Option<String>,
    /// Flow catalog name to resolve data plane and use as prefix.
    #[clap(long)]
    pub name: Option<String>,
    #[clap(long)]
    pub admin: bool,
}

impl GazctlEnv {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        // Validate argument combinations
        let (data_plane_name, prefix) = match (&self.data_plane, &self.prefix, &self.name) {
            (Some(dp), Some(pf), None) => {
                // Traditional mode: --data-plane and --prefix
                (dp.clone(), pf.clone())
            }
            (None, None, Some(name)) => {
                // New mode: --name (resolve data plane from catalog name)
                let data_plane_name = self.resolve_data_plane_from_catalog_name(ctx, name).await?;
                (data_plane_name, format!("{name}/"))
            }
            _ => {
                anyhow::bail!(
                    "Must provide either (--data-plane and --prefix) or --name, but not both"
                );
            }
        };

        let models::authorizations::UserPrefixAuthorization {
            broker_address,
            broker_token,
            reactor_address,
            reactor_token,
            retry_millis: _,
        } = flow_client::fetch_user_prefix_authorization(
            &ctx.client,
            models::authorizations::UserPrefixAuthorizationRequest {
                capability: if self.admin {
                    models::Capability::Admin
                } else {
                    models::Capability::Read
                },
                data_plane: models::Name::new(&data_plane_name),
                prefix: models::Prefix::new(&prefix),
                started_unix: 0,
            },
        )
        .await?;

        println!("export BROKER_ADDRESS={broker_address}");
        println!("export BROKER_AUTH_TOKEN={broker_token}");
        println!("export CONSUMER_ADDRESS={reactor_address}");
        println!("export CONSUMER_AUTH_TOKEN={reactor_token}");

        Ok(())
    }

    async fn resolve_data_plane_from_catalog_name(
        &self,
        ctx: &mut crate::CliContext,
        catalog_name: &str,
    ) -> anyhow::Result<String> {
        #[derive(serde::Deserialize)]
        struct LiveSpecResult {
            data_plane_name: Option<String>,
        }

        let results: Vec<LiveSpecResult> = crate::api_exec(
            ctx.client
                .from("live_specs_ext")
                .select("data_plane_name")
                .eq("catalog_name", catalog_name)
                .limit(1),
        )
        .await?;

        match results.first() {
            Some(LiveSpecResult {
                data_plane_name: Some(dp_name),
            }) => Ok(dp_name.clone()),
            Some(LiveSpecResult {
                data_plane_name: None,
            }) => {
                anyhow::bail!(
                    "Catalog spec '{}' exists but has no data plane assigned",
                    catalog_name
                )
            }
            None => {
                anyhow::bail!("Catalog spec '{}' not found", catalog_name)
            }
        }
    }
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
