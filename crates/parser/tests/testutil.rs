//! Common functions and types for writing end-to-end tests of the parser CLI.

// Functions in this file are only run in tests, which don't count for coverage.
// This prevents "unused function" warnings from being emitted.
#![allow(dead_code)]

use assert_cmd::cargo::CommandCargoExt;
use parser::{Input, ParseConfig};
use serde_json::Value;
use std::fs::File;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Resolve a test file path that works in both Cargo and Bazel contexts.
pub fn path(rel: impl AsRef<Path>) -> PathBuf {
    test_support::test_resource_path!(rel.as_ref())
}

pub fn input_for_file(rel_path: impl AsRef<Path>) -> Input {
    let file = File::open(path(rel_path)).expect("failed to open file");
    Input::File(file)
}

pub struct CommandResult {
    pub parsed: Vec<Value>,
    pub raw_stdout: String,
    pub exit_code: i32,
}

impl CommandResult {
    pub fn assert_success(&self, parsed_rows: usize) {
        assert_eq!(
            parsed_rows,
            self.parsed.len(),
            "expected to output {} records, but instead got {} records",
            parsed_rows,
            self.parsed.len()
        );
        assert_eq!(self.exit_code, 0, "expected parsing to succeed");
    }

    pub fn assert_failure(&self, parsed_rows: usize) {
        assert_eq!(
            parsed_rows,
            self.parsed.len(),
            "expected to output {} records, but instead got {} records",
            parsed_rows,
            self.parsed.len()
        );
        assert_eq!(self.exit_code, 1, "expected parsing to fail");
    }
}

pub fn run_test(config: &ParseConfig, input: Input) -> CommandResult {
    return run_parser(config, input, true);
}

/// Find the flow-parser binary in both Cargo and Bazel contexts.
fn find_flow_parser_bin() -> PathBuf {
    // In Bazel, check for runfiles location
    if let (Ok(bazel_srcdir), Ok(bazel_ws)) = (
        std::env::var("TEST_SRCDIR"),
        std::env::var("TEST_WORKSPACE"),
    ) {
        let bin_path = PathBuf::from(bazel_srcdir)
            .join(bazel_ws)
            .join("crates/parser/flow-parser");
        if bin_path.exists() {
            return bin_path;
        }
    }

    // Fall back to assert_cmd's cargo_bin for Cargo builds
    use std::process::Command;
    Command::cargo_bin("flow-parser")
        .expect("to find flow-parser binary")
        .get_program()
        .into()
}

pub fn run_parser(config: &ParseConfig, input: Input, debug: bool) -> CommandResult {
    use std::io::BufRead;
    use std::process::{Command, Stdio};

    let tmp = TempDir::new().unwrap();
    let cfg_path = tmp.path().join("config.json");
    let mut cfg_file = File::create(&cfg_path).unwrap();
    serde_json::to_writer_pretty(&mut cfg_file, config).expect("failed to write config");
    std::mem::drop(cfg_file);

    let debug_env = if debug {
        vec![("PARSER_LOG", "parser=debug")]
    } else {
        vec![]
    };

    let bin_path = find_flow_parser_bin();
    let mut process = Command::new(bin_path)
        .args(&["parse", "--config-file", cfg_path.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .envs(debug_env)
        .spawn()
        .expect("failed to spawn parser process");

    let mut input = input.into_buffered_stream(8192);
    let copy_result = std::io::copy(&mut input, &mut process.stdin.take().unwrap());
    let output = process
        .wait_with_output()
        .expect("failed to await completion of process");
    // Unwrap copy_result only after the process has completed, since wait_with_output is likely to
    // give us a more relevant error message.
    copy_result.expect("failed to copy input to stdin");

    // Code will be None if child exited due to a signal, so this is just to make debugging easier.
    let exit_code = output.status.code().unwrap_or_else(|| {
        println!("child process exited abnormally: {:?}", output.status);
        -1
    });
    let mut parsed = Vec::new();
    for line in output.stdout.lines() {
        if debug {
            println!("parser output line: {:?}", line);
        }
        parsed.push(
            serde_json::from_str(&line.unwrap()).expect("failed to deserialize parser output"),
        );
    }
    let raw_stdout = String::from_utf8_lossy(&output.stdout).into_owned();

    // Print stderr so that it will show up in the output if the test fails.
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    if debug {
        println!("parser stderr:\n{}", stderr);
    }

    CommandResult {
        parsed,
        exit_code,
        raw_stdout,
    }
}
