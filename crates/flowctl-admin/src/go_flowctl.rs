use flow_cli_common::ExternalArgs;

/// The name of the go-based flowctl binary. This must be on the PATH.
pub const GO_FLOWCTL: &str = "flowctl-go";

/// External subcommands that are provided by the flowctl Go-based binary. All arguments are simply
/// forwarded verbatim.
#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum FlowctlGoSubcommand {
    /// Low-level APIs for automation
    Api(ExternalArgs),
    /// Check a Flow catalog for errors
    Check(ExternalArgs),
    /// Build a catalog and deploy it to a data plane
    Deploy(ExternalArgs),
    /// Discover available captures of an endpoint
    Discover(ExternalArgs),
    /// Interact with broker journals
    Journals(ExternalArgs),
    /// Print the catalog JSON schema
    JsonSchema(ExternalArgs),
    /// Print combined configuration and exit
    PrintConfig(ExternalArgs),
    /// Serve a component of Flow
    Serve(ExternalArgs),
    /// Interact with consumer shards
    Shards(ExternalArgs),
    /// Run an ephemeral, temporary local data plane
    TempDataPlane(ExternalArgs),
    /// Locally test a Flow catalog
    Test(ExternalArgs),
}

impl FlowctlGoSubcommand {
    pub fn into_flowctl_args(self) -> Vec<String> {
        use FlowctlGoSubcommand::*;
        let (subcommand, ExternalArgs { mut args }) = match self {
            Api(a) => ("api", a),
            Check(a) => ("check", a),
            Deploy(a) => ("deploy", a),
            Discover(a) => ("discover", a),
            Journals(a) => ("journals", a),
            JsonSchema(a) => ("json-schema", a),
            PrintConfig(a) => ("print-config", a),
            Serve(a) => ("serve", a),
            Shards(a) => ("shards", a),
            TempDataPlane(a) => ("temp-data-plane", a),
            Test(a) => ("test", a),
        };
        args.insert(0, subcommand.to_owned());
        args
    }
}
