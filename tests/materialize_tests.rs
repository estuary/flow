use std::process::{Command, Output, Stdio};

const FLOWCTL: &str = env!("CARGO_BIN_EXE_flowctl");
const EXAMPLE_CATALOG: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/flow.yaml");

// We'll build the catalog once, and then re-use the same result for all materialization tests.
// This is just to save time, since the npm portion of catalog builds can take a while.
lazy_static::lazy_static! {
    static ref SETUP: Setup = build_example_catalog();
}

#[derive(Debug)]
struct Setup {
    catalog: tempfile::NamedTempFile,
    build_output: CommandResult,
}

impl Setup {
    fn catalog_path(&self) -> String {
        self.catalog.as_ref().display().to_string()
    }
}

fn build_example_catalog() -> Setup {
    let tempfile = tempfile::NamedTempFile::new().unwrap();
    let catalog_path = tempfile.as_ref().display().to_string();
    let build_output = run_flowctl(
        catalog_path.as_str(),
        &["build", "--source", EXAMPLE_CATALOG],
    );
    assert_eq!(
        0, build_output.exit_code,
        "Catalog build failed: {:#?}",
        build_output
    );
    Setup {
        catalog: tempfile,
        build_output,
    }
}

#[test]
fn materialize_dry_run_generates_output_for_all_fields() {
    let catalog_path = SETUP.catalog_path();

    assert_stdout_contains(
        "Materialization of Estuary collection 'stock/daily-stats'",
        0,
        catalog_path.as_str(),
        &[
            "materialize",
            "--target",
            "localSqlite",
            "--table-name",
            "test_table",
            "--collection",
            "stock/daily-stats",
            "--all-fields",
            "--dry-run",
        ],
    );
}

#[test]
fn materialize_dry_run_generates_ouput_for_specific_fields() {
    let catalog_path = SETUP.catalog_path();

    assert_stdout_contains(
        "Materialization of Estuary collection 'stock/daily-stats'",
        0,
        catalog_path.as_str(),
        &[
            "materialize",
            "--target",
            "localSqlite",
            "--table-name",
            "test_table",
            "--collection",
            "stock/daily-stats",
            "--dry-run",
            "--field",
            "date",
            "--field",
            "security",
            "--field",
            "my_special_column",
        ],
    );
}

#[test]
fn materialize_dry_run_emits_error_when_no_fields_are_specified_and_stdin_is_not_a_tty() {
    let catalog_path = SETUP.catalog_path();

    assert_stderr_contains(
        "no fields were specified in the arguments",
        1,
        catalog_path.as_str(),
        &[
            "materialize",
            "--target",
            "localSqlite",
            "--table-name",
            "test_table",
            "--collection",
            "stock/daily-stats",
            "--dry-run",
        ],
    );
}

#[test]
fn materialize_dry_run_emits_error_when_target_does_not_exist() {
    let catalog_path = SETUP.catalog_path();

    assert_stderr_contains(
        "unable to find a materialization --target with the given name",
        1,
        catalog_path.as_str(),
        &[
            "materialize",
            "--target",
            "aMissingTarget",
            "--table-name",
            "test_table",
            "--collection",
            "stock/daily-stats",
            "--dry-run",
            "--all-fields",
        ],
    );
}

fn assert_stderr_contains(
    expected: &str,
    expected_exit_code: i32,
    catalog_path: &str,
    args: &[&str],
) {
    let output = run_flowctl(catalog_path, args);
    assert_eq!(
        expected_exit_code, output.exit_code,
        "unexpected exit code from: {:?}, output: {:#?}",
        args, output
    );

    assert!(
        output.stderr.contains(expected),
        "Expected '{}' to be included in stderr output: {:#?}",
        expected,
        output
    );
}

fn assert_stdout_contains(
    expected_text: &str,
    expected_exit_code: i32,
    catalog_path: &str,
    args: &[&str],
) {
    let output = run_flowctl(catalog_path, args);
    assert_eq!(
        expected_exit_code, output.exit_code,
        "unexpected exit code from: {:?}, output: {:#?}",
        args, output
    );
    assert!(output.stdout.contains(expected_text));
}

#[derive(Debug, Clone, PartialEq)]
struct CommandResult {
    exit_code: i32,
    stdout: String,
    stderr: String,
}

fn run_flowctl(catalog_path: &str, args: &[&str]) -> CommandResult {
    let mut cmd = Command::new(FLOWCTL);
    cmd.args(args)
        .arg("--catalog")
        .arg(catalog_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stdin(Stdio::piped());

    let Output {
        status,
        stdout,
        stderr,
    } = cmd.output().expect("failed to execute flowctl command");
    let stdout = String::from_utf8_lossy(stdout.as_slice()).into_owned();
    let stderr = String::from_utf8_lossy(stderr.as_slice()).into_owned();
    CommandResult {
        exit_code: status.code().unwrap_or(std::i32::MAX),
        stdout,
        stderr,
    }
}
