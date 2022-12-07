use crate::source;
use anyhow::Context;
use doc::combine;
use std::io::{self, Write};

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
    /// Bundle catalog sources into a flattened and inlined catalog.
    Bundle(source::SourceArgs),
    /// Unbundle a bundled catalog into files in a local directory.
    Unbundle(Unbundle),
    /// Combine over an input stream of documents and write the output.
    Combine(Combine),
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

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Combine {
    /// Name of a collection in the Flow specification file.
    #[clap(long)]
    collection: String,

    #[clap(flatten)]
    source_args: source::SourceArgs,
}

#[derive(Debug, clap::Args)]
pub struct Unbundle {
    #[clap(flatten)]
    local_specs: source::LocalSpecsArgs,

    // Note: Given that this command accepts a _bundled_ catalog, it didn't seem necessary to support URLs, but there's definitely
    // still some opportunity to leverage common code for reading this, which likely _would_ support URLs. Just didn't seem
    // important enough to do that right now.
    /// Source of the _bundled_ catalog. The default ("-") will read the catalog from stdin. URLs are not supported.
    #[clap(long, default_value = "-")]
    source: String,
}

impl Advanced {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> Result<(), anyhow::Error> {
        match &self.cmd {
            Command::Get(get) => do_get(ctx, get).await,
            Command::Update(update) => do_update(ctx, update).await,
            Command::Rpc(rpc) => do_rpc(ctx, rpc).await,
            Command::Bundle(bundle) => do_bundle(ctx, bundle).await,
            Command::Unbundle(unbundle) => do_unbundle(ctx, unbundle).await,
            Command::Combine(combine) => do_combine(ctx, combine).await,
        }
    }
}

async fn do_get(ctx: &mut crate::CliContext, Get { table, query }: &Get) -> anyhow::Result<()> {
    let req = ctx.controlplane_client()?.from(table).build().query(query);
    tracing::debug!(?req, "built request to execute");

    println!("{}", req.send().await?.text().await?);
    Ok(())
}

async fn do_update(
    ctx: &mut crate::CliContext,
    Update { table, query, body }: &Update,
) -> anyhow::Result<()> {
    let req = ctx
        .controlplane_client()?
        .from(table)
        .update(body)
        .build()
        .query(query);
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
    let req = ctx
        .controlplane_client()?
        .rpc(function, body)
        .build()
        .query(query);
    tracing::debug!(?req, "built request to execute");

    println!("{}", req.send().await?.text().await?);
    Ok(())
}

async fn do_bundle(
    _ctx: &mut crate::CliContext,
    sources: &source::SourceArgs,
) -> anyhow::Result<()> {
    let source_files = sources.resolve_sources().await?;
    let catalog = crate::source::bundle(source_files.iter().map(String::as_str)).await?;
    serde_json::to_writer_pretty(io::stdout(), &catalog)?;
    Ok(())
}

async fn do_unbundle(_ctx: &mut crate::CliContext, args: &Unbundle) -> anyhow::Result<()> {
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    let mut bundled_content = Vec::with_capacity(8 * 1024);
    match args.source.as_str() {
        "-" => {
            tokio::io::stdin()
                .read_to_end(&mut bundled_content)
                .await
                .context("reading stdin")?;
        }
        path => {
            let mut file = File::open(path).await.context("opening source file")?;
            file.read_to_end(&mut bundled_content)
                .await
                .context("reading source file")?;
        }
    }

    let parsed = sources::parse_catalog_spec(&bundled_content)?;
    source::write_local_specs(parsed, &args.local_specs).await?;
    Ok(())
}

async fn do_combine(
    _ctx: &mut crate::CliContext,
    Combine {
        source_args,
        collection,
    }: &Combine,
) -> anyhow::Result<()> {
    let collection = models::Collection::new(collection);

    let sources = source_args.resolve_sources().await?;
    let catalog = crate::source::bundle(sources).await?;

    let Some(models::CollectionDef{schema, read_schema, key, .. }) = catalog.collections.get(&collection) else {
        anyhow::bail!("did not find collection {collection:?} in the source specification");
    };
    let schema = schema.as_ref().or(read_schema.as_ref()).unwrap();

    let schema = doc::validation::build_schema(
        url::Url::parse("https://example/schema").unwrap(),
        &serde_json::to_value(schema).unwrap(),
    )?;

    let mut accumulator = combine::Accumulator::new(
        key.iter().map(|ptr| doc::Pointer::from_str(ptr)).collect(),
        None,
        tempfile::tempfile().context("opening tempfile")?,
        doc::Validator::new(schema)?,
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
