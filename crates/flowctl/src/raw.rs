use super::config;

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
    /// requesty --body. As with `get`, you may pass optional query
    /// parameters.
    Rpc(Rpc),
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Get {
    /// Table to select from.
    #[clap(long)]
    table: String,
    /// Optional query parameters.
    #[clap(short = 'q', parse(try_from_str = parse_key_val), number_of_values = 1)]
    query: Vec<(String, String)>,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Rpc {
    /// RPC function to invoke.
    #[clap(long)]
    function: String,
    /// Optional query parameters.
    #[clap(short = 'q', parse(try_from_str = parse_key_val), number_of_values = 1)]
    query: Vec<(String, String)>,
    /// Serialized JSON arguments of the request.
    #[clap(long)]
    body: String,
}

impl Advanced {
    pub async fn run(&self, cfg: &mut config::Config) -> Result<(), anyhow::Error> {
        match &self.cmd {
            Command::Get(get) => do_get(&cfg, get).await,
            Command::Rpc(rpc) => do_rpc(&cfg, rpc).await,
        }
    }
}

async fn do_get(cfg: &config::Config, Get { table, query }: &Get) -> anyhow::Result<()> {
    let req = cfg.client()?.from(table).build().query(query);
    tracing::debug!(?req, "built request to execute");

    println!("{}", req.send().await?.text().await?);
    Ok(())
}

async fn do_rpc(
    cfg: &config::Config,
    Rpc {
        function,
        query,
        body,
    }: &Rpc,
) -> anyhow::Result<()> {
    let req = cfg.client()?.rpc(function, body).build().query(query);
    tracing::debug!(?req, "built request to execute");

    println!("{}", req.send().await?.text().await?);
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
