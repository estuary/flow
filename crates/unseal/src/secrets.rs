use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use std::collections::BTreeMap;

/// Number of secrets decrypted concurrently. A task's stanza is small, and this
/// bounds what a pathological one could ask of the decryption service at once.
const FETCH_CONCURRENCY: usize = 8;

/// Does `config` look like a `sops`-wrapped document?
///
/// This is the sniff which distinguishes a task's two configuration branches,
/// which are mutually exclusive: either the configuration is wrapped as a whole
/// (the legacy path), or it's plaintext and its secrets arrive through a
/// `secrets` stanza. `sops` is a reserved top-level property of the latter,
/// which publication enforces.
pub fn is_sops(config: &models::RawValue) -> bool {
    #[derive(serde::Deserialize)]
    struct Sniff {
        #[serde(default)]
        sops: Option<serde::de::IgnoredAny>,
    }

    // A configuration which isn't an object cannot be a `sops` document.
    serde_json::from_str::<Sniff>(config.get()).is_ok_and(|Sniff { sops }| sops.is_some())
}

/// Resolve the `secrets` stanza of a task into its plaintext `config`.
///
/// `secrets` maps a JSON pointer of `config` to the catalog name of the secret
/// which supplies it. `decrypt` resolves a name into its plaintext value, and
/// is called exactly once for each *distinct* name however many locations it
/// serves. Fetches are concurrent, and this routine holds no plaintext beyond
/// the configuration it returns.
///
/// Each entry is applied in lexicographic pointer order, by synthesizing a
/// document from the pointer (`/a/b/c` with value `v` becomes
/// `{"a":{"b":{"c":v}}}`) and merging it into `config` as an RFC 7396 merge
/// patch. Everything else follows from the RFC: missing parents are created,
/// scalar parents are replaced, object values deep-merge, a `null` leaf deletes
/// its property, and a deeper pointer wins wherever two entries overlap.
pub async fn resolve<Decrypt, Fut>(
    config: &models::RawValue,
    secrets: &BTreeMap<String, String>,
    decrypt: Decrypt,
) -> anyhow::Result<models::RawValue>
where
    Decrypt: Fn(String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<models::RawValue>>,
{
    // Parse pointers before fetching anything, so a malformed stanza fails
    // without having disclosed plaintext.
    let entries: Vec<(&str, &str, Vec<String>)> = secrets
        .iter()
        .map(|(pointer, name)| Ok((pointer.as_str(), name.as_str(), parse_pointer(pointer)?)))
        .collect::<anyhow::Result<_>>()?;

    let mut distinct: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (pointer, name, _tokens) in &entries {
        distinct.entry(name).or_default().push(pointer);
    }

    let decrypt = &decrypt;
    let values: BTreeMap<&str, serde_json::Value> =
        futures::stream::iter(distinct.iter().map(|(name, pointers)| async move {
            let value = decrypt(name.to_string()).await.with_context(|| {
                format!(
                    "failed to resolve secret '{name}', used at configuration location(s) {}",
                    pointers.join(", ")
                )
            })?;
            anyhow::Ok((*name, value.to_value()))
        }))
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
    use super::{is_sops, resolve};
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

        let secrets: BTreeMap<String, String> = secrets
            .iter()
            .map(|(pointer, name)| (pointer.to_string(), name.to_string()))
            .collect();

        let resolved = resolve(
            &models::RawValue::from_value(&config),
            &secrets,
            |name: String| {
                *calls.lock().unwrap().entry(name.clone()).or_default() += 1;

                std::future::ready(match fixture.get(&name) {
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

    #[test]
    fn sops_sniff() {
        let cases = [
            json!({"sops": {"mac": "..."}}),
            json!({"sops": null}),
            json!({"address": "db:5432"}),
            json!({}),
            json!("not an object"),
            json!(null),
        ];
        let sniffed: Vec<bool> = cases
            .iter()
            .map(|case| is_sops(&models::RawValue::from_value(case)))
            .collect();

        insta::assert_debug_snapshot!(sniffed, @r###"
        [
            true,
            false,
            false,
            false,
            false,
            false,
        ]
        "###);
    }
}
