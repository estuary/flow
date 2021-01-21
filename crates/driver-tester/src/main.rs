use driver_tester::{run_tests, Args};
use structopt::StructOpt;
use tracing_subscriber::EnvFilter;

const LOG_VAR: &str = "FLOW_LOG";

#[tokio::main]
async fn main() {
    let args = Args::from_args();
    let verbosity = args.verbose;
    let log_filter = std::env::var(LOG_VAR).ok().unwrap_or_else(|| {
        let s = match verbosity {
            i32::MIN..=-1 => "off",
            // default filter
            0 => "warn",
            1 => "warn,driver_tester::tests=info",
            2 => "info,driver_tester::tests=debug",
            3..=i32::MAX => "driver_tester=trace,debug",
        };
        s.to_string()
    });
    let filter = EnvFilter::try_new(log_filter).expect("invalid FLOW_LOG value");

    // TODO: maybe make this not pretty if isatty is false
    tracing_subscriber::fmt()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true)
        .pretty()
        .init();
    let report = run_tests(args).await;

    if verbosity > 1 {
        println!("{:#}", report);
    } else {
        println!("{}", report);
    }
    let code = if report.passed() { 0 } else { 1 };
    std::process::exit(code);
}
