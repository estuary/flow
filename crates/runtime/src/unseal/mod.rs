use anyhow::Context;
use serde_json::value::RawValue;
use zeroize::Zeroizing;

/// Decrypt a `sops`-protected document using `sops` and application default credentials.
pub async fn decrypt_sops(config: &RawValue) -> anyhow::Result<Box<RawValue>> {
    let sops = locate_bin::locate("sops").context("failed to locate sops")?;

    #[derive(serde::Deserialize)]
    struct Document {
        #[serde(default)]
        sops: Option<Sops>,
    }
    #[derive(serde::Deserialize)]
    struct Sops {
        #[serde(default)]
        encrypted_suffix: Option<String>,
    }

    let doc: Document =
        serde_json::from_str(config.get()).context("decoding `sops` stanza of endpoint config")?;

    // If this isn't a `sops` document, then return a copy of it unmodified.
    let Some(Sops{encrypted_suffix}) = doc.sops else {
        return Ok(config.to_owned())
    };

    // Note that input_output() pre-allocates an output buffer as large as its input buffer,
    // and our decrypted result will never be larger than its input.
    let async_process::Output {
        stderr,
        stdout,
        status,
    } = async_process::input_output(
        async_process::Command::new(sops).args([
            "--decrypt",
            "--input-type",
            "json",
            "--output-type",
            "json",
            "/dev/stdin",
        ]),
        config.get().as_bytes(),
    )
    .await
    .context("failed to run sops")?;

    let stdout = Zeroizing::from(stdout);

    if !status.success() {
        anyhow::bail!(
            "decrypting sops document failed: {}",
            String::from_utf8_lossy(&stderr),
        );
    }

    // If there is no encrypted suffix, then we're all done.
    let Some(encrypted_suffix) = encrypted_suffix else {
        return Ok(serde_json::from_slice(&stdout).context("parsing `sops` output")?)
    };

    // We must re-write the document to remove the encrypted suffix.
    // Use `jq` to do the re-writing. This avoids allocating and parsing
    // values in our own heap, and is also succinct.
    // See: https://jqplay.org/s/sQunN3Qc4s
    let async_process::Output {
        stderr,
        stdout,
        status,
    } = async_process::input_output(
        async_process::Command::new("jq").args([
            // --compact-output disables jq's pretty-printer, which will otherwise introduce
            // unnecessary newlines/tabs in the output, which will cause the output to be
            // longer than the input.
            "--compact-output".to_string(),
            // --join-output puts jq into raw output mode, and additionally stops it from writing newlines
            // at the end of its output, which can otherwise cause the output to be longer
            // than the input.
            "--join-output".to_string(),
            format!("walk(if type == \"object\" then with_entries(. + {{key: .key | rtrimstr(\"{encrypted_suffix}\")}}) else . end)"),
        ]),
        &stdout,
    )
    .await
    .context("failed to run jq")?;

    let stdout = Zeroizing::from(stdout);

    if !status.success() {
        anyhow::bail!(
            "stripping encrypted suffix {encrypted_suffix} from document failed: {}",
            String::from_utf8_lossy(&stderr),
        );
    }

    Ok(serde_json::from_slice(&stdout).context("parsing stripped `jq` output")?)
}

#[cfg(test)]
mod test {
    use super::decrypt_sops;
    use futures::StreamExt;
    use serde_json::value::RawValue;

    #[tokio::test]
    async fn test_fixtures() {
        let configs: Vec<Box<RawValue>> = vec![
            serde_json::from_slice(include_bytes!("testdata/empty-input.json")).unwrap(),
            serde_json::from_slice(include_bytes!("testdata/hyphen-suffix.json")).unwrap(),
            serde_json::from_slice(include_bytes!("testdata/no-suffix.json")).unwrap(),
            serde_json::from_slice(include_bytes!("testdata/not-encrypted.json")).unwrap(),
            serde_json::from_slice(include_bytes!("testdata/under-suffix.json")).unwrap(),
        ];

        let outputs = futures::stream::iter(configs.into_iter())
            .map(|config| async move {
                serde_json::from_str(decrypt_sops(&config).await.unwrap().get()).unwrap()
            })
            .buffered(5)
            .collect::<Vec<serde_json::Value>>()
            .await;

        insta::assert_json_snapshot!(outputs, @r###"
        [
          {},
          {
            "notsops": "bar",
            "s2": "final secret",
            "stuff": {
              "array": [
                42,
                {
                  "frob": "bob",
                  "inner-sops": "nested secret"
                }
              ],
              "nullish": null,
              "other": true,
              "s1": "secret!"
            }
          },
          {
            "false": null,
            "foo": {
              "bar": 42,
              "some_sops": [
                3,
                "three"
              ]
            },
            "tru": true
          },
          {
            "false": null,
            "foo": {
              "bar": 42,
              "some_sops": [
                3,
                "three"
              ]
            },
            "tru": true
          },
          {
            "baz": {
              "array": [
                42,
                {
                  "true": false
                },
                {
                  "frob": "bob",
                  "inner_sops": 15
                }
              ],
              "nullish": null,
              "other": true,
              "s1": 42
            },
            "foo": "bar",
            "s2": "final secret!"
          }
        ]
        "###);
    }
}
