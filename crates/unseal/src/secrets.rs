use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use std::collections::BTreeMap;

/// Number of secrets decrypted concurrently. A task's stanza is small, and this
/// bounds what a pathological one could ask of the decryption service at once.
const FETCH_CONCURRENCY: usize = 8;

/// Resolve the `secrets` stanza of a task into its plaintext `config`.
///
/// `secrets` pairs a JSON pointer of `config` with the catalog name of the
/// secret which supplies it. `decrypt` resolves a name into its plaintext
/// value, and is called exactly once for each *distinct* name however many
/// locations it serves. Fetches are concurrent, and this routine holds no
/// plaintext beyond the configuration it returns.
///
/// Entries are sorted and applied in lexicographic pointer order, by
/// synthesizing a document from the pointer (`/a/b/c` with value `v` becomes
/// `{"a":{"b":{"c":v}}}`) and merging it into `config` as an RFC 7396 merge
/// patch. Everything else follows from the RFC: missing parents are created,
/// scalar parents are replaced, object values deep-merge, a `null` leaf deletes
/// its property, and a deeper pointer wins wherever two entries overlap.
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

    // Parse pointers before fetching anything, so a malformed stanza fails
    // without having disclosed plaintext.
    let mut entries: Vec<(&'a str, &'a str, Vec<String>)> = secrets
        .into_iter()
        .map(|(pointer, name)| {
            let (pointer, name) = (pointer.as_ref(), name.as_ref());
            Ok((pointer, name, parse_pointer(pointer)?))
        })
        .collect::<anyhow::Result<_>>()?;

    entries.sort_by_key(|(pointer, _name, _tokens)| *pointer);

    let mut distinct: BTreeMap<&'a str, Vec<&'a str>> = BTreeMap::new();
    for &(pointer, name, _) in &entries {
        distinct.entry(name).or_default().push(pointer);
    }

    // Materialize the futures so the borrowed iterator closure does not cross
    // an await, allowing callers to spawn the resolution future as Send.
    let fetches: Vec<_> = distinct
        .into_iter()
        .map(|(name, pointers)| async move {
            let value = decrypt(name).await.with_context(|| {
                format!(
                    "failed to resolve secret '{name}', used at configuration location(s) {}",
                    pointers.join(", ")
                )
            })?;
            anyhow::Ok((name, value.to_value()))
        })
        .collect();

    let values: BTreeMap<&'a str, serde_json::Value> = futures::stream::iter(fetches)
        .buffer_unordered(FETCH_CONCURRENCY)
        .try_collect()
        .await?;

    let mut config = config.to_value();

    for (_pointer, name, tokens) in entries {
        let mut patch = values
            .get(name)
            .expect("every distinct name was fetched")
            .clone();

        // The empty pointer merges at the configuration root, which RFC 7396
        // can only do with an object: any other value would replace the
        // configuration outright rather than merging into it.
        if tokens.is_empty() && !patch.is_object() {
            anyhow::bail!(
                "secret '{name}' merges at the root of the configuration, so it must be a JSON object"
            );
        }
        for token in tokens.iter().rev() {
            patch = serde_json::Value::Object([(token.clone(), patch)].into_iter().collect());
        }
        json_patch::merge(&mut config, &patch);
    }

    Ok(models::RawValue::from_value(&config))
}

/// Split a JSON pointer into its unescaped tokens.
///
/// Tokens are always object property names: `/2` addresses the property "2" and
/// never an array index, and `/-` is the literal property "-". Arrays are
/// atomic values -- to change one, target its parent property with a secret
/// whose value is the whole array.
///
/// `json::Pointer` is deliberately not used: its token model parses canonical
/// integer tokens as array indices -- the semantics rejected above -- and it
/// accepts pointers which lack a leading '/'.
fn parse_pointer(pointer: &str) -> anyhow::Result<Vec<String>> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    let Some(rest) = pointer.strip_prefix('/') else {
        anyhow::bail!(
            "configuration location '{pointer}' is not a JSON pointer: it must be empty, or begin with '/'"
        );
    };

    Ok(rest
        .split('/')
        .map(|token| token.replace("~1", "/").replace("~0", "~"))
        .collect())
}

#[cfg(test)]
mod test {
    use super::resolve;
    use serde_json::json;
    use std::collections::BTreeMap;

    /// Values which `stub` resolves, by secret name.
    fn fixture() -> serde_json::Value {
        json!({
            "acmeCo/password": "p4ssw0rd",
            "acmeCo/token": "t0ken",
            "acmeCo/credentials": {"user": "alice", "password": "p4ssw0rd"},
            "acmeCo/array": [1, 2, 3],
            "acmeCo/tombstone": null,
        })
    }

    /// Resolve `secrets` into `config` from the fixture, tallying how many
    /// times each name was decrypted so that de-duplication is observable.
    async fn run(
        config: serde_json::Value,
        secrets: &[(&str, &str)],
    ) -> anyhow::Result<(serde_json::Value, BTreeMap<String, usize>)> {
        let fixture = fixture();
        let calls = std::sync::Mutex::new(BTreeMap::new());

        let resolved = resolve(
            &models::RawValue::from_value(&config),
            secrets.iter().copied(),
            |name| {
                *calls.lock().unwrap().entry(name.to_string()).or_default() += 1;

                std::future::ready(match fixture.get(name) {
                    Some(value) => Ok(models::RawValue::from_value(value)),
                    None => Err(anyhow::anyhow!("secret does not exist")),
                })
            },
        )
        .await?;

        Ok((resolved.to_value(), calls.into_inner().unwrap()))
    }

    /// Render an error with its full context chain, as the runtime logs it.
    fn err(err: anyhow::Error) -> String {
        format!("{:#}", err)
    }

    #[tokio::test]
    async fn merges_create_parents_and_deeper_pointers_win() {
        // `/a` establishes an object which `/a/deep/leaf` then extends, and
        // `/existing/user` replaces just one property of an existing object.
        let (config, calls) = run(
            json!({
                "existing": {"user": "bob", "host": "db.example.com"},
                "scalar": 42,
            }),
            &[
                ("/a", "acmeCo/credentials"),
                ("/a/deep/leaf", "acmeCo/token"),
                ("/existing/user", "acmeCo/password"),
                ("/scalar", "acmeCo/array"),
            ],
        )
        .await
        .unwrap();

        insta::assert_json_snapshot!(config, @r###"
        {
          "a": {
            "deep": {
              "leaf": "t0ken"
            },
            "password": "p4ssw0rd",
            "user": "alice"
          },
          "existing": {
            "host": "db.example.com",
            "user": "p4ssw0rd"
          },
          "scalar": [
            1,
            2,
            3
          ]
        }
        "###);
        assert_eq!(calls["acmeCo/credentials"], 1);
    }

    #[tokio::test]
    async fn distinct_names_are_fetched_once_each() {
        let (config, calls) = run(
            json!({}),
            &[
                ("/one", "acmeCo/password"),
                ("/two", "acmeCo/password"),
                ("/three/nested", "acmeCo/password"),
                ("/four", "acmeCo/token"),
            ],
        )
        .await
        .unwrap();

        insta::assert_json_snapshot!(config, @r###"
        {
          "four": "t0ken",
          "one": "p4ssw0rd",
          "three": {
            "nested": "p4ssw0rd"
          },
          "two": "p4ssw0rd"
        }
        "###);
        insta::assert_debug_snapshot!(calls, @r###"
        {
            "acmeCo/password": 1,
            "acmeCo/token": 1,
        }
        "###);
    }

    #[tokio::test]
    async fn null_value_deletes_its_property() {
        // RFC 7396: a `null` leaf deletes, which is a hazard worth knowing for
        // authors of object-shaped secrets -- `/keep` loses `drop` the same way.
        let (config, _calls) = run(
            json!({"remove": "gone", "keep": {"drop": 1, "stay": 2}}),
            &[
                ("/remove", "acmeCo/tombstone"),
                ("/keep/drop", "acmeCo/tombstone"),
            ],
        )
        .await
        .unwrap();

        insta::assert_json_snapshot!(config, @r###"
        {
          "keep": {
            "stay": 2
          }
        }
        "###);
    }

    #[tokio::test]
    async fn pointer_tokens_are_always_property_names() {
        // A numeric token is the property "2" and not an array index, "-" is
        // the literal property, and `~1` / `~0` unescape to '/' and '~'.
        let (config, _calls) = run(
            json!({"array": [1, 2, 3]}),
            &[
                ("/2", "acmeCo/password"),
                ("/-", "acmeCo/password"),
                ("/a~1b/c~0d", "acmeCo/password"),
                ("/array", "acmeCo/array"),
            ],
        )
        .await
        .unwrap();

        insta::assert_json_snapshot!(config, @r###"
        {
          "-": "p4ssw0rd",
          "2": "p4ssw0rd",
          "a/b": {
            "c~d": "p4ssw0rd"
          },
          "array": [
            1,
            2,
            3
          ]
        }
        "###);
    }

    #[tokio::test]
    async fn empty_pointer_merges_an_object_at_the_root() {
        let (config, _calls) = run(
            json!({"user": "bob", "host": "db.example.com"}),
            &[("", "acmeCo/credentials")],
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
    }

    #[tokio::test]
    async fn errors() {
        // A non-object at the root would replace the configuration outright.
        insta::assert_snapshot!(
            err(run(json!({}), &[("", "acmeCo/password")]).await.unwrap_err()),
            @"secret 'acmeCo/password' merges at the root of the configuration, so it must be a JSON object"
        );

        // A dangling reference names the secret and every location it serves.
        insta::assert_snapshot!(
            err(run(
                json!({}),
                &[("/a", "acmeCo/missing"), ("/b/c", "acmeCo/missing")],
            )
            .await
            .unwrap_err()),
            @"failed to resolve secret 'acmeCo/missing', used at configuration location(s) /a, /b/c: secret does not exist"
        );

        // A location which isn't a JSON pointer fails before any fetch.
        insta::assert_snapshot!(
            err(run(json!({}), &[("not-a-pointer", "acmeCo/password")])
                .await
                .unwrap_err()),
            @"configuration location 'not-a-pointer' is not a JSON pointer: it must be empty, or begin with '/'"
        );
    }
}
