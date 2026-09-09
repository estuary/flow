use anyhow::Context;
use futures::{StreamExt, TryStreamExt};

/// Number of secrets decrypted concurrently. A task's stanza is small, and this
/// bounds what a pathological one could ask of the decryption service at once.
const FETCH_CONCURRENCY: usize = 8;

/// Resolve the `secrets` stanza of a task into its plaintext `config`.
///
/// `secrets` pairs the catalog name of a secret with the JSON pointer of
/// `config` where it is merged. `decrypt` resolves each name into its plaintext
/// value. Fetches are concurrent.
pub async fn resolve<'a, S, Decrypt, Fut>(
    config: &models::RawValue,
    secrets: impl IntoIterator<Item = (&'a S, &'a S)>,
    decrypt: Decrypt,
) -> anyhow::Result<models::RawValue>
where
    S: AsRef<str> + ?Sized + 'a,
    Decrypt: Fn(&'a str) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<models::RawValue>>,
{
    let decrypt = &decrypt;

    // Materialize the futures so the borrowed iterator closure does not cross
    // an await, allowing callers to spawn the resolution future as Send.
    let fetches: Vec<_> = secrets
        .into_iter()
        .map(|(name, pointer)| async move {
            let (name, pointer) = (name.as_ref(), json::Pointer::from_str(pointer.as_ref()));
            let resolved = decrypt(name).await.with_context(|| {
                format!(
                    "failed to resolve secret '{name}', used at configuration location {pointer}"
                )
            })?;
            anyhow::Ok((name, pointer, resolved))
        })
        .collect();

    let entries: Vec<(&str, json::Pointer, models::RawValue)> = futures::stream::iter(fetches)
        .buffered(FETCH_CONCURRENCY)
        .try_collect()
        .await?;

    let mut config = config.to_value();

    for (name, pointer, resolved) in entries {
        let target = json::ptr::create_value(&pointer, &mut config).with_context(|| {
            format!(
                "cannot apply secret '{name}' at configuration location '{pointer}': the pointer is incompatible with the existing document structure"
            )
        })?;
        json_patch::merge(target, &resolved.to_value());
    }

    Ok(models::RawValue::from_value(&config))
}

#[cfg(test)]
mod test {
    use super::resolve;
    use serde_json::json;

    /// Values which `stub` resolves, by secret name.
    fn fixture() -> serde_json::Value {
        json!({
            "acmeCo/password": "p4ssw0rd",
            "acmeCo/password-two": "p4ssw0rd",
            "acmeCo/password-three": "p4ssw0rd",
            "acmeCo/token": "t0ken",
            "acmeCo/credentials": {"user": "alice", "password": "p4ssw0rd"},
            "acmeCo/oauth-client": {"clientId": "client-id", "clientSecret": "client-secret", "method": "client"},
            "acmeCo/oauth-tokens": {"accessToken": "access-token", "refreshToken": "refresh-token", "method": "tokens"},
            "acmeCo/array": [1, 2, 3],
            "acmeCo/tombstone": null,
            "acmeCo/tombstone-two": null,
        })
    }

    /// Resolve `secrets` into `config` from the fixture.
    async fn run(
        config: serde_json::Value,
        secrets: &[(&str, &str)],
    ) -> anyhow::Result<serde_json::Value> {
        let fixture = fixture();

        let resolved = resolve(
            &models::RawValue::from_value(&config),
            secrets.iter().copied(),
            |name| {
                std::future::ready(match fixture.get(name) {
                    Some(value) => Ok(models::RawValue::from_value(value)),
                    None => Err(anyhow::anyhow!("secret does not exist")),
                })
            },
        )
        .await?;

        Ok(resolved.to_value())
    }

    /// Render an error with its full context chain, as the runtime logs it.
    fn err(err: anyhow::Error) -> String {
        format!("{:#}", err)
    }

    #[tokio::test]
    async fn missing_locations_follow_create_value_semantics() {
        // A numeric token creates an array when its parent is missing, and
        // extends an existing array with nulls as needed.
        let config = run(
            json!({
                "existingArray": [0],
                "existing": {"user": "bob", "host": "db.example.com"},
                "scalar": 42,
            }),
            &[
                ("acmeCo/credentials", "/new/2"),
                ("acmeCo/password", "/existing/user"),
                ("acmeCo/password-two", "/existingArray/3"),
                ("acmeCo/array", "/scalar"),
            ],
        )
        .await
        .unwrap();

        insta::assert_json_snapshot!(config, @r###"
        {
          "existing": {
            "host": "db.example.com",
            "user": "p4ssw0rd"
          },
          "existingArray": [
            0,
            null,
            null,
            "p4ssw0rd"
          ],
          "new": [
            null,
            null,
            {
              "password": "p4ssw0rd",
              "user": "alice"
            }
          ],
          "scalar": [
            1,
            2,
            3
          ]
        }
        "###);
    }

    #[tokio::test]
    async fn multiple_secrets_merge_at_one_location_in_secret_order() {
        let config = run(
            json!({}),
            &[
                ("acmeCo/oauth-client", "/credentials"),
                ("acmeCo/oauth-tokens", "/credentials"),
            ],
        )
        .await
        .unwrap();

        insta::assert_json_snapshot!(config, @r###"
        {
          "credentials": {
            "accessToken": "access-token",
            "clientId": "client-id",
            "clientSecret": "client-secret",
            "method": "tokens",
            "refreshToken": "refresh-token"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn null_value_replaces_its_location() {
        let config = run(
            json!({"remove": "gone", "keep": {"drop": 1, "stay": 2}}),
            &[
                ("acmeCo/tombstone", "/remove"),
                ("acmeCo/tombstone-two", "/keep/drop"),
            ],
        )
        .await
        .unwrap();

        insta::assert_json_snapshot!(config, @r###"
        {
          "keep": {
            "drop": null,
            "stay": 2
          },
          "remove": null
        }
        "###);
    }

    #[tokio::test]
    async fn pointer_tokens_follow_document_structure() {
        // Numeric tokens index arrays and name properties in objects, while
        // `~1` / `~0` unescape to '/' and '~'.
        let config = run(
            json!({"array": [1, {"existing": true}], "2": "old"}),
            &[
                ("acmeCo/password", "/2"),
                ("acmeCo/password-two", "/array/1/password"),
                ("acmeCo/password-three", "/a~1b/c~0d"),
            ],
        )
        .await
        .unwrap();

        insta::assert_json_snapshot!(config, @r###"
        {
          "2": "p4ssw0rd",
          "a/b": {
            "c~d": "p4ssw0rd"
          },
          "array": [
            1,
            {
              "existing": true,
              "password": "p4ssw0rd"
            }
          ]
        }
        "###);
    }

    #[tokio::test]
    async fn empty_pointer_merge_patches_the_root() {
        let config = run(
            json!({"user": "bob", "host": "db.example.com"}),
            &[("acmeCo/credentials", "")],
        )
        .await
        .unwrap();

        insta::assert_json_snapshot!(config, @r###"
        {
          "host": "db.example.com",
          "password": "p4ssw0rd",
          "user": "alice"
        }
        "###);

        let config = run(json!({}), &[("acmeCo/password", "")]).await.unwrap();
        assert_eq!(config, json!("p4ssw0rd"));
    }

    #[tokio::test]
    async fn errors() {
        // A dangling reference names the secret and its location.
        insta::assert_snapshot!(
            err(run(json!({}), &[("acmeCo/missing", "/a")])
                .await
                .unwrap_err()),
            @"failed to resolve secret 'acmeCo/missing', used at configuration location /a: secret does not exist"
        );

        // Existing arrays cannot be queried by property, and existing scalars
        // cannot gain children.
        for pointer in ["/array/property", "/scalar/property"] {
            let error = err(run(
                json!({"array": [0, 1], "scalar": 42}),
                &[("acmeCo/password", pointer)],
            )
            .await
            .unwrap_err());

            assert_eq!(
                error,
                format!(
                    "cannot apply secret 'acmeCo/password' at configuration location '{pointer}': the pointer is incompatible with the existing document structure"
                )
            );
        }
    }
}
