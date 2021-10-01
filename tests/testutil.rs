//! Common functions and types for writing end-to-end tests of the parser CLI.

// Functions in this file are only run in tests, which don't count for coverage.
// This prevents "unused function" warnings from being emitted.
#![allow(dead_code)]

use parser::{Input, ParseConfig};
use serde_json::Value;
use std::fs::File;
use std::path::Path;

use tempdir::TempDir;

pub fn input_for_file(rel_path: impl AsRef<Path>) -> Input {
    let file = File::open(rel_path).expect("failed to open file");
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

/// Returns the path to the parser executable, accounting for
fn parser_exe() -> &'static str {
    if cfg!(all(target_env = "musl", not(debug_assertions))) {
        "./target/x86_64-unknown-linux-musl/release/parser"
    } else if cfg!(all(target_env = "gnu", not(debug_assertions))) {
        "./target/release/parser"
    } else if cfg!(all(target_env = "gnu", debug_assertions)) {
        "./target/debug/parser"
    } else {
        unimplemented!("unsupported compilation configuration")
    }
}

pub fn run_test(config: &ParseConfig, input: Input) -> CommandResult {
    use std::io::BufRead;
    use std::process::{Command, Stdio};

    let tmp = TempDir::new("jsonl-parser-test").unwrap();
    let cfg_path = tmp.path().join("config.json");
    let mut cfg_file = File::create(&cfg_path).unwrap();
    serde_json::to_writer_pretty(&mut cfg_file, config).expect("failed to write config");
    std::mem::drop(cfg_file);

    let mut process = Command::new(parser_exe())
        .args(&["parse", "--config-file", cfg_path.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .env("PARSER_LOG", "parser=debug")
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
        println!("parser output line: {:?}", line);
        parsed.push(
            serde_json::from_str(&line.unwrap()).expect("failed to deserialize parser output"),
        );
    }
    let raw_stdout = String::from_utf8_lossy(&output.stdout).into_owned();

    // Print stderr so that it will show up in the output if the test fails.
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    println!("parser stderr:\n{}", stderr);

    CommandResult {
        parsed,
        exit_code,
        raw_stdout,
    }
}
