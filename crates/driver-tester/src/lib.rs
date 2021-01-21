mod request;
mod test_doc;
mod tests;

use crate::tests::TestCase;
use anyhow::Context;
use protocol::materialize::driver_client;
use rand_chacha::ChaChaRng;
use std::fmt::{self, Debug, Display};
use std::time::Duration;

pub type DriverClientImpl = driver_client::DriverClient<tonic::transport::Channel>;

/// CANARY Tests Flow materialization drivers.
#[derive(Debug, structopt::StructOpt)]
pub struct Args {
    /// Make the output more verbose.
    #[structopt(short = "v", long, parse(from_occurrences))]
    pub verbose: i32,
    /// The URI of the materialization driver to test.
    #[structopt(long)]
    pub driver_uri: String,
    /// The endpoint URI of a compatible system to use during tests. This should not be a
    /// production system. The tests (and the driver under test) may inflict untold horrors on
    /// whatever this URI points to. It should be something like a docker container that can be
    /// easily thrown away and rebuilt. This will soon be replaced by a more generic `--endpoint`
    /// argument that accepts a json string with the connection information.
    #[structopt(long)]
    pub endpoint_uri: String,

    /// The table name to use during tests. This will soon be replaced by a more generic
    /// `--endpoint` argument that accepts a json string with the connection information.
    #[structopt(long)]
    pub table: String,

    /// The names of the tests to run. May be specified multiple times to run multiple tests. If no
    /// `--test` argument is provided, then all tests will be run. Of course there's only one test
    /// right now, anyway, so this option is rather silly.
    #[structopt(long = "test")]
    pub tests: Vec<TestCase>,
}

/// Holds the complete results from running a suite of tests.
pub struct TestReport {
    pub error: Option<anyhow::Error>,
    pub results: Vec<TestResult>,
}

impl TestReport {
    /// Returns true if all the tests passed
    pub fn passed(&self) -> bool {
        self.error.is_none() && self.results.iter().all(|r| r.error.is_none())
    }
}

impl Display for TestReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let n_tests = self.results.len();
        let s = if n_tests != 1 { "s" } else { "" };
        writeln!(f, "Finished {} test{}:\n", n_tests, s)?;

        for result in self.results.iter() {
            if f.alternate() {
                writeln!(f, "{:#}", result)?;
            } else {
                writeln!(f, "{}", result)?;
            }
        }

        if let Some(err) = self.error.as_ref() {
            if f.alternate() {
                writeln!(f, "\nError: {:#}", err)?;
            } else {
                writeln!(f, "\nError: {}", err)?;
            }
        }

        let end = if self.passed() { "PASSED" } else { "FAILED" };
        writeln!(f, "\n{}", end)
    }
}

/// The result of running a single test.
pub struct TestResult {
    /// The name of the test case.
    pub name: String,
    /// The time it took to run the test, excluding the setup function.
    pub duration: Duration,
    /// If the test failed, then this will hold the error. If `error` is `None`, then it indicates
    /// that the test passed.
    pub error: Option<anyhow::Error>,
}

impl Display for TestResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let pass = if self.error.is_none() { "PASS" } else { "FAIL" };
        let duration = self.duration.as_millis();
        write!(f, "{}: {} ({} ms)", self.name, pass, duration)?;
        if let Some(err) = self.error.as_ref() {
            if f.alternate() {
                writeln!(f, "\nError: {:#}", err)?;
            } else {
                writeln!(f, " Error: {}", err)?;
            }
        } else {
            f.write_str("\n")?;
        }
        Ok(())
    }
}

/// Fixture holds some basic information that will be used by all tests.
pub struct Fixture {
    /// The URI of the driver under test.
    pub driver_uri: String,
    /// The client to use for testing. All tests will use the same client, which will already be
    /// connected to the `driver_uri` before the test case begins.
    pub client: DriverClientImpl,
    /// The endpoint to use for tests. This gets passed to the driver as part of the StartSession
    /// messages. This field, along with `target` will soon be replaced by a single json `Value`,
    /// once the materialization protocol is updated for that.
    pub endpoint: String,
    /// The target to use for tests. This gets passed to the driver as part of the StartSession
    /// messages.
    pub target: String,
    /// The random number generator to be used for all random data generation during tests.
    /// This is pinned to a specific implementation so that we can get repeatable results by using
    /// the same seed. But the field is not public so tests don't accidentally rely on specific
    /// implementation details.
    rng: ChaChaRng,
}
impl Debug for Fixture {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Fixture")
            .field("driver_uri", &self.driver_uri)
            .field("endpoint", &self.endpoint)
            .field("target", &self.endpoint)
            .finish()
    }
}

impl Fixture {
    /// Returns a mutable reference to the random number generator, so tests can use it
    pub fn rng(&mut self) -> &mut impl rand::Rng {
        &mut self.rng
    }
}

/// Runs all tests using the provided arguments, and returns a TestReport with the results.
pub async fn run_tests(args: Args) -> TestReport {
    let Args {
        driver_uri,
        endpoint_uri,
        table,
        tests,
        ..
    } = args;

    let reporter = Reporter {
        verbosity: args.verbose,
    };

    let connect_result = DriverClientImpl::connect(driver_uri.clone())
        .await
        .context("Failed to connect to driver");

    let client = match connect_result {
        Ok(c) => c,
        Err(err) => {
            return TestReport {
                error: Some(err),
                results: Vec::new(),
            }
        }
    };
    let test_cases = if tests.is_empty() {
        self::tests::ALL_TESTS
    } else {
        tests.as_slice()
    };

    // TODO: Consider allowing the seed to be set in args, and randomly generate a new seed if not
    let seed: u64 = 12345678910;
    let rng = new_rng(seed);

    let mut fixture = Fixture {
        client,
        driver_uri,
        endpoint: endpoint_uri,
        target: table,
        rng,
    };

    let mut results = Vec::with_capacity(tests.len());
    for test in test_cases {
        reporter.starting_test(*test);
        let result = tests::run(*test, &mut fixture).await;
        reporter.test_finished(&result);
        results.push(result);
    }

    TestReport {
        error: None,
        results,
    }
}

fn new_rng(seed: u64) -> ChaChaRng {
    use rand::SeedableRng;

    tracing::debug!("using RNG seed: {}", seed);
    SeedableRng::seed_from_u64(seed)
}

/// Writes test output to stdout as they run.
pub struct Reporter {
    verbosity: i32,
}

impl Reporter {
    fn starting_test(&self, test: TestCase) {
        if self.verbosity >= 2 {
            println!("Starting: {}", test);
        }
    }

    fn test_finished(&self, result: &TestResult) {
        if self.verbosity >= 1 {
            let desc = if result.error.is_none() {
                "PASSED"
            } else {
                "FAILED"
            };
            println!(
                "Finished: {}: {} {} ms",
                result.name,
                desc,
                result.duration.as_millis()
            );
            if let Some(err) = result.error.as_ref() {
                println!("Failure: {:#}\n", err);
            }
        }
    }
}
