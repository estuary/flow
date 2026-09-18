use anyhow::Context;
use zeroize::Zeroizing;

pub mod overlay;
pub mod secrets;

/// Failure of [`resolve`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(
        "endpoint configuration has a top-level `sops` key and cannot also use a `secrets` stanza"
    )]
    SopsWithSecrets,
    #[error("decrypting `sops` configuration")]
    Sops(#[source] anyhow::Error),
    #[error("resolving `secrets` stanza")]
    Secrets(#[source] anyhow::Error),
}

/// Resolve a task's endpoint configuration into connector-bound plaintext.
///
/// - A `secrets` stanza over a plaintext document resolves through
///   [`secrets::resolve`], with `decrypt` supplying each named secret.
/// - A `sops` envelope decrypts through [`overlay::decrypt_with_overlay`],
///   validating any overlay against `config_schema`.
/// - A document which is neither passes through as a copy.
/// - A document which is *both* is rejected: `sops` seals the whole document,
///   and a stanza can only merge into plaintext.
pub async fn resolve<'a, S, Decrypt, Fut>(
    sealed: &models::RawValue,
    secrets: impl IntoIterator<Item = (&'a S, &'a S)>,
    config_schema: &[u8],
    decrypt: Decrypt,
) -> Result<models::RawValue, Error>
where
    S: AsRef<str> + ?Sized + 'a,
    Decrypt: Fn(&'a str) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<models::RawValue>>,
{
    // Peek rather than collect: a stanza's emptiness is all that's needed here,
    // and `secrets::resolve` consumes the remainder.
    let mut secrets = secrets.into_iter().peekable();

    match (secrets.peek().is_some(), sealed.is_sops()) {
        (true, true) => Err(Error::SopsWithSecrets),
        (false, true) => overlay::decrypt_with_overlay(sealed, config_schema)
            .await
            .map_err(Error::Sops),
        (true, false) => secrets::resolve(sealed, secrets, decrypt)
            .await
            .map_err(Error::Secrets),
        (false, false) => Ok(sealed.to_owned()),
    }
}

/// Decrypt a `sops`-protected document using `sops` and application default credentials.
pub async fn decrypt_sops(config: &models::RawValue) -> anyhow::Result<models::RawValue> {
    // Only objects can be `sops` documents.
    let dom = config.to_value();
    if !dom.is_object() {
        return Ok(config.to_owned());
    }

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
        serde_json::from_value(dom).context("decoding `sops` stanza of endpoint config")?;

    // If this isn't a `sops` document, then return a copy of it unmodified.
    let Some(Sops { encrypted_suffix }) = doc.sops else {
        return Ok(config.to_owned());
    };

    let sops = locate_bin::locate("sops").context("failed to locate sops")?;

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

    let mut stdout = Zeroizing::from(stdout);

    // `sops` emits JSON with newlines and tabs. Remove them to not break JSONL.
    stdout.retain(|c| *c != b'\n' && *c != b'\t');

    if !status.success() {
        anyhow::bail!(
            "decrypting sops document failed: {}",
            String::from_utf8_lossy(&stderr),
        );
    }

    // If there is no encrypted suffix, then we're all done.
    let Some(encrypted_suffix) = encrypted_suffix else {
        return Ok(serde_json::from_slice(&stdout).context("parsing `sops` output")?);
    };

    // We must re-write the document to remove the encrypted suffix.
    let mut dom: serde_json::Value =
        serde_json::from_slice(&stdout).context("parsing `sops` output")?;
    strip_encrypted_suffix(&mut dom, &encrypted_suffix);

    Ok(models::RawValue::from_value(&dom))
}

/// Remove `suffix` from the end of every object key within `dom`, at any depth.
/// Only a single trailing occurrence is removed, and an empty suffix is a no-op.
fn strip_encrypted_suffix(dom: &mut serde_json::Value, suffix: &str) {
    match dom {
        serde_json::Value::Object(map) => {
            *map = std::mem::take(map)
                .into_iter()
                .map(|(key, mut value)| {
                    strip_encrypted_suffix(&mut value, suffix);
                    let key = key.strip_suffix(suffix).map(str::to_string).unwrap_or(key);
                    (key, value)
                })
                .collect();
        }
        serde_json::Value::Array(items) => {
            for item in items {
                strip_encrypted_suffix(item, suffix);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod resolve_test {
    use super::{Error, resolve};
    use serde_json::json;

    /// Config schema of the fixtures, which have no `sops.overlay` to validate.
    const SCHEMA: &[u8] = b"{}";

    fn sealed(fixture: &[u8]) -> Box<models::RawValue> {
        serde_json::from_slice(fixture).unwrap()
    }

    fn stub(name: &str) -> std::future::Ready<anyhow::Result<models::RawValue>> {
        std::future::ready(Ok(models::RawValue::from_value(&json!(format!(
            "plaintext of {name}"
        )))))
    }

    #[tokio::test]
    async fn test_branches() {
        let plaintext = models::RawValue::from_value(&json!({"host": "db.acmeCo.test"}));
        let wrapped = sealed(include_bytes!("testdata/no-suffix.json"));
        let stanza: [(&str, &str); 1] = [("acmeCo/password", "/password")];

        // Plaintext with a stanza resolves each secret into the configuration.
        let out = resolve(&plaintext, stanza, SCHEMA, stub).await.unwrap();
        insta::assert_json_snapshot!(out.to_value(), @r###"
        {
          "host": "db.acmeCo.test",
          "password": "plaintext of acmeCo/password"
        }
        "###);

        // Plaintext without a stanza passes through.
        let out = resolve::<str, _, _>(&plaintext, [], SCHEMA, stub)
            .await
            .unwrap();
        insta::assert_json_snapshot!(out.to_value(), @r###"
        {
          "host": "db.acmeCo.test"
        }
        "###);

        // A `sops` envelope without a stanza decrypts.
        let out = resolve::<str, _, _>(&wrapped, [], SCHEMA, stub)
            .await
            .unwrap();
        insta::assert_json_snapshot!(out.to_value(), @r###"
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
        }
        "###);

        // A `sops` envelope with a stanza is rejected, and never decrypts.
        let err = resolve(&wrapped, stanza, SCHEMA, stub).await.unwrap_err();
        assert!(matches!(err, Error::SopsWithSecrets));
        insta::assert_snapshot!(
            err.to_string(),
            @"endpoint configuration has a top-level `sops` key and cannot also use a `secrets` stanza"
        );
    }

    #[tokio::test]
    async fn test_failed_decrypt_is_attributed_to_the_stanza() {
        let err = resolve(
            &models::RawValue::from_value(&json!({})),
            [("acmeCo/password", "/password")],
            SCHEMA,
            |_name| std::future::ready(Err(anyhow::anyhow!("service is down"))),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::Secrets(_)));
        insta::assert_snapshot!(
            format!("{:#}", anyhow::Error::from(err)),
            @"resolving `secrets` stanza: failed to resolve secret 'acmeCo/password', used at configuration location /password: service is down"
        );
    }
}

#[cfg(test)]
mod test {
    use super::decrypt_sops;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_fixtures() {
        let configs: Vec<Box<models::RawValue>> = vec![
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
