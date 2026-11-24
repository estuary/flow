//! DNS refusals which a VMM's resolver reports over a host vsock port.
//! `serve` writes each to the connector's logs while it serves a stream. Only
//! a VMM passes `--resolver-vsock-port`, so only it has them.

/// One refusal, as the resolver writes it: a JSON object per line. The
/// resolver's `Refusal` (`crates/connector-vmm`) states the same shape.
#[derive(serde::Deserialize)]
#[serde(tag = "refusal", rename_all = "camelCase")]
enum Refusal {
    Name { name: String },
    Address { name: String },
    Stopped { reported: usize },
}

/// The resolver's refusals, as lines.
pub type Reader = std::pin::Pin<Box<dyn tokio::io::AsyncBufRead + Send>>;

/// Dial the resolver's refusals at host vsock `port`. A failure leaves
/// refusals unreported, as they're feedback, and not enforcement.
pub async fn dial(port: u32) -> Option<Reader> {
    let addr = tokio_vsock::VsockAddr::new(tokio_vsock::VMADDR_CID_HOST, port);
    match tokio_vsock::VsockStream::connect(addr).await {
        Ok(stream) => Some(Box::pin(tokio::io::BufReader::new(stream))),
        Err(error) => {
            tracing::warn!(%error, port, "failed to dial the resolver's refusals");
            None
        }
    }
}

/// The message and field of the log which reports a refusal `line`.
pub(crate) fn render(line: &str) -> serde_json::Result<(String, (&'static str, String))> {
    Ok(match serde_json::from_str(line)? {
        Refusal::Name { name } => {
            let mut message = format!(
                "refused DNS name {name}, which this connector's egress does not permit; \
                 a task may permit it in egress.hosts"
            );
            // The resolver reports names lowercased, with dots within a label escaped.
            if matches!(name.as_str(), "pypi.org" | "files.pythonhosted.org") {
                message.push_str(
                    "; installing packages from PyPI typically requires both pypi.org \
                     and files.pythonhosted.org in egress.hosts",
                );
            }
            (message, ("name", name))
        }
        Refusal::Address { name } => (
            format!("refused DNS name {name}, which resolved to an address that is not public"),
            ("name", name),
        ),
        Refusal::Stopped { reported } => (
            "further refused DNS names are not reported".to_string(),
            ("reported", reported.to_string()),
        ),
    })
}

#[cfg(test)]
mod test {
    /// The lines of the resolver's own test of its refusals.
    #[test]
    fn renders_the_resolvers_refusals() {
        let lines = [
            r#"{"refusal":"name","name":"other.acmeco.example"}"#,
            r#"{"refusal":"name","name":"pypi.org"}"#,
            r#"{"refusal":"name","name":"files.pythonhosted.org"}"#,
            r#"{"refusal":"name","name":"test.pypi.org"}"#,
            r#"{"refusal":"address","name":"leak.acmeco.example"}"#,
            r#"{"refusal":"stopped","reported":32}"#,
            r#"{"refusal":"other","name":"acmeco.example"}"#,
        ];
        let rendered: Vec<String> = lines
            .iter()
            .map(|line| match super::render(line) {
                Ok((message, (key, value))) => format!("{message} ({key}={value})"),
                Err(error) => format!("error: {error}"),
            })
            .collect();

        insta::assert_snapshot!(rendered.join("\n"), @r"
        refused DNS name other.acmeco.example, which this connector's egress does not permit; a task may permit it in egress.hosts (name=other.acmeco.example)
        refused DNS name pypi.org, which this connector's egress does not permit; a task may permit it in egress.hosts; installing packages from PyPI typically requires both pypi.org and files.pythonhosted.org in egress.hosts (name=pypi.org)
        refused DNS name files.pythonhosted.org, which this connector's egress does not permit; a task may permit it in egress.hosts; installing packages from PyPI typically requires both pypi.org and files.pythonhosted.org in egress.hosts (name=files.pythonhosted.org)
        refused DNS name test.pypi.org, which this connector's egress does not permit; a task may permit it in egress.hosts (name=test.pypi.org)
        refused DNS name leak.acmeco.example, which resolved to an address that is not public (name=leak.acmeco.example)
        further refused DNS names are not reported (reported=32)
        error: unknown variant `other`, expected one of `name`, `address`, `stopped` at line 1 column 18
        ");
    }
}
