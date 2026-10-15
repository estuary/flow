//! Running the setup commands, with everything they say framed.
//!
//! `Command::status` would let a subprocess write to this process's stderr
//! untouched, and these commands do not all write tidy single lines: an `nft`
//! syntax error echoes the offending rule and underlines it with carets, both
//! indented. A leading space on stderr is what the launcher's log pump takes
//! for connector-init's marker, so a setup command that failed would pass for
//! it, and then be mistaken for that connector's first log record.

use std::io::Write;
use std::process::{Command, Output, Stdio};

/// Run a setup command to completion, framing whatever it wrote.
///
/// `stdin` is the bytes to feed it, for the commands that read one: `nft -f -`
/// takes the whole ruleset that way rather than through a file the VMM would
/// have to find somewhere writable.
pub fn run(program: &str, args: &[&str], stdin: Option<&[u8]>) -> anyhow::Result<()> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(match stdin {
            Some(_) => Stdio::piped(),
            None => Stdio::null(),
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| anyhow::anyhow!("running {program}: {e}"))?;

    if let Some(stdin) = stdin {
        child
            .stdin
            .take()
            .expect("stdin was piped")
            .write_all(stdin)
            .map_err(|e| anyhow::anyhow!("writing to {program}: {e}"))?;
    }
    let output = child
        .wait_with_output()
        .map_err(|e| anyhow::anyhow!("waiting for {program}: {e}"))?;

    eprint!("{}", diagnostics(&output));

    if !output.status.success() {
        anyhow::bail!("{program} {}: {}", args.join(" "), output.status);
    }
    Ok(())
}

/// Run a command for what it writes to stdout, framing only its stderr. For
/// listings the caller parses rather than shows.
pub fn output(program: &str, args: &[&str]) -> anyhow::Result<Vec<u8>> {
    let output = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| anyhow::anyhow!("running {program}: {e}"))?;

    if !output.stderr.is_empty() {
        eprint!(
            "{}",
            crate::framed(&String::from_utf8_lossy(&output.stderr))
        );
    }
    if !output.status.success() {
        anyhow::bail!("{program} {}: {}", args.join(" "), output.status);
    }
    Ok(output.stdout)
}

/// What the command said, framed, whether or not it succeeded: a warning from
/// a command that exited zero is still worth seeing.
///
/// Both streams, because a setup command writing to this process's stdout
/// would land in the middle of the guest's console.
fn diagnostics(output: &Output) -> String {
    let mut framed = String::new();

    for stream in [&output.stdout, &output.stderr] {
        if !stream.is_empty() {
            framed.push_str(&crate::framed(&String::from_utf8_lossy(stream)));
        }
    }
    framed
}

#[cfg(test)]
mod tests {
    /// The stderr of a real `nft` syntax error, which carries two indented
    /// lines: the offending rule, and the carets under it. Held verbatim
    /// rather than produced by running `nft`, which needs a netlink socket
    /// this test does not have.
    const NFT_SYNTAX_ERROR: &str = "\
ruleset:3:30-37: Error: syntax error, unexpected string, expecting priority
    type filter hook forward prioritY 0;
                             ^^^^^^^^
";

    #[test]
    fn indented_output_is_framed() {
        let output = std::process::Output {
            status: std::os::unix::process::ExitStatusExt::from_raw(1 << 8),
            stdout: b"a command that also wrote to stdout\n".to_vec(),
            stderr: NFT_SYNTAX_ERROR.as_bytes().to_vec(),
        };

        // What would have reached the launcher without framing. The first of
        // these is what its pump takes for connector-init's marker.
        assert_eq!(
            NFT_SYNTAX_ERROR
                .lines()
                .filter(|line| line.starts_with(' '))
                .count(),
            2,
        );

        let framed = super::diagnostics(&output);
        for line in framed.lines() {
            assert!(
                !line.starts_with(' '),
                "framed a line beginning with a space: {line:?}"
            );
        }
        insta::assert_snapshot!(framed);
    }

    #[test]
    fn a_failing_command_is_an_error() {
        let error = super::run("/bin/sh", &["-c", "exit 3"], None)
            .expect_err("a non-zero exit is a failure");

        insta::assert_snapshot!(format!("{error:#}"));
    }

    /// The path `nft -f -` takes: the ruleset arrives on stdin.
    #[test]
    fn stdin_reaches_the_command() {
        super::run(
            "/bin/sh",
            &["-c", "test \"$(cat)\" = 'table inet flow_egress {}'"],
            Some(b"table inet flow_egress {}"),
        )
        .expect("the command saw its stdin");
    }
}
