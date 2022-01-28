#[derive(Debug, clap::Args)]
pub struct CombineArgs {
    /// it's the fooiest
    #[clap(long)]
    foo: String,
}
