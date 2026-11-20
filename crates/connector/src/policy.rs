//! Pure product-policy decisions applied at connector boundaries.

use std::collections::BTreeMap;

/// Policy established from an image reference before registry I/O.
#[derive(Debug, Clone)]
pub(crate) struct Image {
    /// Full image, including tag or digest (ghcr.io/estuary/source-foobar:v6)
    image: String,
    /// Image repository (ghcr.io/estuary/source-foobar)
    repository: String,
    /// Is this an Estuary first-party connector?
    first_party: bool,
}

/// Effective usage rate and a stable explanation of how it was selected.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UsageRate {
    pub value: f32,
    #[allow(dead_code)]
    source: &'static str,
}

impl Image {
    pub(crate) fn check(plane: crate::Plane, image: &str) -> anyhow::Result<Self> {
        let (repository, _) = models::split_image_tag(image);
        let first_party = repository
            .strip_prefix("ghcr.io/estuary/")
            .is_some_and(|name| !name.is_empty());

        if matches!(plane, crate::Plane::Public) && !first_party {
            anyhow::bail!(
                "connector image '{image}' is not allowed in public data planes: only Estuary-managed images are permitted"
            );
        }
        if matches!(plane, crate::Plane::Public) && repository == "ghcr.io/estuary/derive-python" {
            anyhow::bail!("Python derivations may only run in private data-planes");
        }
        if matches!(plane, crate::Plane::Public) && repository == "ghcr.io/estuary/capture-python" {
            anyhow::bail!("Python captures may only run in private data-planes");
        }
        if matches!(plane, crate::Plane::Public)
            && repository == "ghcr.io/estuary/materialize-python"
        {
            anyhow::bail!("Python materializations may only run in private data-planes");
        }

        Ok(Self {
            image: image.to_string(),
            repository,
            first_party,
        })
    }

    pub(crate) fn repository(&self) -> &str {
        &self.repository
    }

    pub(crate) fn usage_rate(
        &self,
        runtime_protocol: crate::RuntimeProtocol,
        declared: Option<f32>,
    ) -> anyhow::Result<UsageRate> {
        if let Some(value) = declared {
            if !self.first_party {
                anyhow::bail!(
                    "connector image '{}' declares '{}', but only first-party images under ghcr.io/estuary/ may set usage rates",
                    self.image,
                    crate::image::USAGE_RATE_LABEL,
                );
            }
            if !value.is_finite() || value < 0.0 {
                anyhow::bail!(
                    "connector image '{}' has invalid '{}' value {value:?}: usage rates must be finite and non-negative",
                    self.image,
                    crate::image::USAGE_RATE_LABEL,
                );
            }
            return Ok(UsageRate {
                value,
                source: crate::image::USAGE_RATE_LABEL,
            });
        }

        Ok(match runtime_protocol {
            crate::RuntimeProtocol::Derive => UsageRate {
                value: 0.0,
                source: "default for derive protocol",
            },
            crate::RuntimeProtocol::Capture | crate::RuntimeProtocol::Materialize => UsageRate {
                value: 1.0,
                source: "default for capture and materialize protocol",
            },
        })
    }
}

/// Attribution under which secrets are being decrypted.
pub(crate) enum SecretIdentity<'a> {
    /// Image contexts may decrypt declared secrets by image label, each at
    /// the configuration location the image declares for it.
    Image {
        image: &'a str,
        repository: &'a str,
        declared: &'a BTreeMap<String, String>,
    },
    /// Non-image context are restricted to sibling secrets.
    NoImage,
}

/// Check the `secrets` stanza of a task, as (secret name, JSON pointer) pairs,
/// against the identity of its connector.
///
/// A sibling secret may be merged wherever the task chooses. An image-owned
/// secret must be merged exactly where its image declares: the secret belongs
/// to the image's vendor and is shared by every task of that connector, so the
/// task author mustn't be able to redirect it into a location (a host, a URL,
/// a field echoed into logs) through which it would leave the connector.
pub(crate) fn check_secrets<'a>(
    task_name: &str,
    stanza: impl IntoIterator<Item = (&'a str, &'a str)>,
    identity: SecretIdentity<'_>,
) -> anyhow::Result<()> {
    let no_declarations = BTreeMap::new();
    let (image, declared) = match identity {
        SecretIdentity::Image {
            image,
            repository,
            declared,
        } => {
            if !declared.is_empty()
                && repository
                    .split('/')
                    .any(|component| component == models::IMAGE_SECRET_SEPARATOR)
            {
                anyhow::bail!(
                    "image '{image}' repository contains the reserved '{}' component and cannot declare image-owned secrets",
                    models::IMAGE_SECRET_SEPARATOR,
                );
            }
            for secret in declared.keys() {
                if !models::image_owns_secret(secret, repository) {
                    anyhow::bail!(
                        "image '{image}' declares secret '{secret}' in its '{}' label, \
                         but the image rule cannot admit it: a declared secret must be named \
                         <prefix>/connectors/{repository}/<leaf>",
                        crate::image::SECRETS_LABEL,
                    );
                }
            }
            (Some(image), declared)
        }
        SecretIdentity::NoImage => (None, &no_declarations),
    };

    let task_prefix = parent_prefix(task_name).unwrap_or("");
    for (secret, pointer) in stanza {
        if parent_prefix(secret).unwrap_or("") == task_prefix {
            continue;
        }
        let Some(image) = image else {
            anyhow::bail!(
                "task '{task_name}' uses secret '{secret}', which is not a sibling of the task \
                 (under '{task_prefix}'). A connector which runs without an image can use only \
                 sibling secrets, as it has no image identity under which to be granted others"
            );
        };
        match declared.get(secret) {
            Some(declared_pointer) if declared_pointer == pointer => {}
            Some(declared_pointer) => anyhow::bail!(
                "task '{task_name}' merges image-owned secret '{secret}' at configuration \
                 location '{pointer}', but image '{image}' declares it for location \
                 '{declared_pointer}' in its '{}' label: an image-owned secret may only be \
                 merged where its image declares",
                crate::image::SECRETS_LABEL,
            ),
            None => anyhow::bail!(
                "task '{task_name}' uses secret '{secret}', which is neither a sibling of the task \
                 (under '{task_prefix}') nor declared by image '{image}' in its '{}' \
                 label (which declares {:?})",
                crate::image::SECRETS_LABEL,
                declared.keys().collect::<Vec<_>>(),
            ),
        }
    }
    Ok(())
}

fn parent_prefix(name: &str) -> Option<&str> {
    name.rfind('/').map(|index| &name[..index + 1])
}

pub(crate) fn local_connectors_allowed(plane: crate::Plane) -> bool {
    matches!(plane, crate::Plane::Local)
}

pub(crate) fn check_remote_sqlite_vfs(
    is_wire: bool,
    sqlite_vfs_uri_is_set: bool,
) -> anyhow::Result<()> {
    if is_wire && sqlite_vfs_uri_is_set {
        anyhow::bail!(
            "Start.sqlite_vfs_uri is runtime-internal and may not be set by a remote client"
        );
    }
    Ok(())
}

/// A remote client may ask for an `ephemeral` volume, which is just an empty
/// directory. It may not ask for a `host_dir`, which is a directory on this machine.
pub(crate) fn check_remote_volume(
    is_wire: bool,
    volume: Option<&crate::proto::request::start::Volume>,
) -> anyhow::Result<()> {
    use crate::proto::request::start;

    if is_wire && matches!(volume, Some(start::Volume::HostDir(_))) {
        anyhow::bail!("Start.host_dir is runtime-internal and may not be set by a remote client");
    }
    Ok(())
}

pub(crate) fn check_connector_sqlite_vfs(
    is_sqlite: bool,
    sqlite_vfs_uri_is_set: bool,
) -> anyhow::Result<()> {
    if sqlite_vfs_uri_is_set && !is_sqlite {
        anyhow::bail!("Start.sqlite_vfs_uri may only be set for a Sqlite derivation connector");
    }
    Ok(())
}

/// Make a connector's Log safe to forward, and restrict its events to their
/// task and supported event types.
///
/// The Log is built within the connector's container, so every field of it is
/// untrusted. Loggers which receive it require that it serializes, which
/// [`make_serializable`] ensures.
pub(crate) fn sanitize_connector_log(
    quoted_task_name: &bytes::Bytes,
    mut log: ops::Log,
) -> ops::Log {
    make_serializable(&mut log, std::time::SystemTime::now);

    match log
        .fields_json_map
        .get("eventType")
        .map(|v| v == "\"connectorStatus\"" || v == "\"configUpdate\"")
    {
        Some(true) => match log
            .fields_json_map
            .get("eventTarget")
            .map(|target| target == quoted_task_name)
        {
            Some(true) => {}
            Some(false) => {
                let value = log.fields_json_map.remove("eventTarget").unwrap();
                log.fields_json_map
                    .insert("_sanitized_eventTarget".to_string(), value);
                log.fields_json_map
                    .insert("eventTarget".to_string(), quoted_task_name.clone());
            }
            None => {
                log.fields_json_map
                    .insert("eventTarget".to_string(), quoted_task_name.clone());
            }
        },
        Some(false) => {
            let value = log.fields_json_map.remove("eventType").unwrap();
            log.fields_json_map
                .insert("_sanitized_eventType".to_string(), value);
        }
        None => {}
    }
    log
}

/// Seconds of a protobuf Timestamp, which spans the years 1 through 9999.
const TIMESTAMP_SECONDS: std::ops::RangeInclusive<i64> = -62_135_596_800..=253_402_300_799;

/// Repair each part of `log` which would fail to serialize as JSON, and
/// likewise its spans. A repair keeps what the connector sent where it can:
///
/// - An unknown level becomes `warn`.
/// - A timestamp outside of protobuf's range becomes `now`.
/// - A field value which isn't JSON becomes a JSON string of its bytes.
///
/// `meta` and `shard` are cleared, as the runtime sets both when it publishes
/// the Log.
fn make_serializable(log: &mut ops::Log, now: fn() -> std::time::SystemTime) {
    log.meta = None;
    log.shard = None;

    if ops::LogLevel::try_from(log.level).is_err() {
        log.level = ops::LogLevel::Warn as i32;
    }
    if log.timestamp.as_ref().is_some_and(|ts| {
        !TIMESTAMP_SECONDS.contains(&ts.seconds) || !(0..1_000_000_000).contains(&ts.nanos)
    }) {
        log.timestamp = Some(proto_flow::as_timestamp(now()));
    }
    for value in log.fields_json_map.values_mut() {
        if serde_json::from_slice::<&serde_json::value::RawValue>(value).is_err() {
            *value = crate::json_field(&String::from_utf8_lossy(value));
        }
    }
    for span in &mut log.spans {
        make_serializable(span, now);
    }
}

/// Deadline for beginning a graceful session restart ahead of IAM token expiry.
pub(crate) fn token_restart_deadline(
    now: std::time::SystemTime,
    expires_at: std::time::SystemTime,
) -> std::time::SystemTime {
    use std::time::Duration;

    const LONG_LIFETIME: Duration = Duration::from_secs(4 * 3600);
    const LONG_MARGIN: Duration = Duration::from_secs(30 * 60);
    const SHORT_MARGIN: Duration = Duration::from_secs(5 * 60);

    let lifetime = expires_at.duration_since(now).unwrap_or_default();
    let margin = if lifetime >= LONG_LIFETIME {
        LONG_MARGIN
    } else {
        SHORT_MARGIN
    };
    expires_at - margin.min(lifetime)
}

/// Lifetime of the `TASK_UPDATE` token written into a connector mount.
pub(crate) const TASK_UPDATE_LIFETIME: tokens::TimeDelta = tokens::TimeDelta::hours(1);

#[cfg(test)]
mod test {
    use super::{
        Image, SecretIdentity, check_connector_sqlite_vfs, check_remote_sqlite_vfs,
        check_remote_volume, check_secrets, sanitize_connector_log,
    };
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn image_admission_and_usage_rate() {
        let cases = [
            (
                crate::Plane::Public,
                "ghcr.io/estuary/source-a:v1",
                crate::RuntimeProtocol::Capture,
                None,
            ),
            (
                crate::Plane::Public,
                "ghcr.io/estuary/derive-python@sha256:abc",
                crate::RuntimeProtocol::Derive,
                None,
            ),
            (
                crate::Plane::Public,
                "example.test/acme/source-a:v1",
                crate::RuntimeProtocol::Capture,
                None,
            ),
            (
                crate::Plane::Private,
                "example.test/acme/source-a:v1",
                crate::RuntimeProtocol::Capture,
                None,
            ),
            (
                crate::Plane::Private,
                "example.test/acme/source-a:v1",
                crate::RuntimeProtocol::Capture,
                Some(1.25),
            ),
            (
                crate::Plane::Private,
                "ghcr.io/estuary/source-a:v1",
                crate::RuntimeProtocol::Capture,
                Some(1.25),
            ),
            (
                crate::Plane::Private,
                "ghcr.io/estuary/derive-a:v1",
                crate::RuntimeProtocol::Derive,
                None,
            ),
            (
                crate::Plane::Private,
                "ghcr.io/estuary/derive-a:v1",
                crate::RuntimeProtocol::Derive,
                Some(1.25),
            ),
            (
                crate::Plane::Private,
                "ghcr.io/estuary/source-a:v1",
                crate::RuntimeProtocol::Capture,
                Some(-1.0),
            ),
            (
                crate::Plane::Private,
                "ghcr.io/estuary/source-a:v1",
                crate::RuntimeProtocol::Capture,
                Some(f32::INFINITY),
            ),
            (
                crate::Plane::Private,
                "ghcr.io/estuary/source-a:v1",
                crate::RuntimeProtocol::Capture,
                Some(f32::NAN),
            ),
        ];
        let outcomes: Vec<_> = cases
            .into_iter()
            .map(|(plane, image, protocol, declared)| {
                (
                    plane,
                    image,
                    Image::check(plane, image)
                        .and_then(|policy| policy.usage_rate(protocol, declared))
                        .map(|rate| (rate.value, rate.source))
                        .map_err(|err| format!("{err:#}")),
                )
            })
            .collect();

        insta::assert_debug_snapshot!(outcomes);
    }

    #[test]
    fn public_python_policy_uses_the_normalized_exact_repository() {
        for image in [
            "ghcr.io/estuary/derive-python",
            "ghcr.io/estuary/derive-python:v1",
            "ghcr.io/estuary/derive-python@sha256:abc",
            "ghcr.io/estuary/derive-python@sha512:abc",
            // A tag and a digest together must not leave the tag in the repository.
            "ghcr.io/estuary/derive-python:v1@sha256:abc",
        ] {
            assert_eq!(
                Image::check(crate::Plane::Public, image)
                    .unwrap_err()
                    .to_string(),
                "Python derivations may only run in private data-planes"
            );
            Image::check(crate::Plane::Private, image).unwrap();
        }

        Image::check(
            crate::Plane::Public,
            "ghcr.io/estuary/derive-python-tools:v1",
        )
        .unwrap();

        for image in [
            "ghcr.io/estuary/capture-python",
            "ghcr.io/estuary/capture-python:stable",
            "ghcr.io/estuary/capture-python@sha256:abc",
            "ghcr.io/estuary/capture-python:v1@sha256:abc",
        ] {
            assert_eq!(
                Image::check(crate::Plane::Public, image)
                    .unwrap_err()
                    .to_string(),
                "Python captures may only run in private data-planes"
            );
            Image::check(crate::Plane::Private, image).unwrap();
        }

        assert_eq!(
            Image::check(
                crate::Plane::Public,
                "ghcr.io/estuary/materialize-python:stable"
            )
            .unwrap_err()
            .to_string(),
            "Python materializations may only run in private data-planes"
        );
        Image::check(
            crate::Plane::Private,
            "ghcr.io/estuary/materialize-python:stable",
        )
        .unwrap();
    }

    #[test]
    fn secret_admission() {
        const REPOSITORY: &str = "ghcr.io/acmeVendor/source-widgets";
        const IMAGE: &str = "ghcr.io/acmeVendor/source-widgets:v1";
        const TASK: &str = "acmeCo/team/capture";
        const IMAGE_RULE: &str =
            "acmeVendor/oauth/connectors/ghcr.io/acmeVendor/source-widgets/oauth-client";

        const SIBLING: (&str, &str) = ("acmeCo/team/password", "/password");
        const BOUND: (&str, &str) = (IMAGE_RULE, "/credentials");

        let cases: [(&str, &[(&str, &str)], &[(&str, &str)]); 12] = [
            ("sibling only", &[SIBLING], &[]),
            // A sibling belongs to the task, which may merge it anywhere.
            ("sibling at the root", &[("acmeCo/team/password", "")], &[]),
            ("declared image-rule secret", &[BOUND], &[BOUND]),
            ("both kinds", &[SIBLING, BOUND], &[BOUND]),
            ("declared but unused", &[SIBLING], &[BOUND]),
            // The task names a declared secret, but would merge it at a
            // location of its own choosing rather than the image's.
            (
                "declared, at another location",
                &[(IMAGE_RULE, "/host")],
                &[BOUND],
            ),
            (
                "declares a non-admissible name",
                &[],
                &[("acmeCo/team/password", "/password")],
            ),
            (
                "declares another image's secret",
                &[],
                &[(
                    "acmeVendor/oauth/connectors/ghcr.io/acmeVendor/source-gadgets/oauth-client",
                    "/credentials",
                )],
            ),
            (
                "repo is a grandparent",
                &[],
                &[(
                    "acmeVendor/connectors/ghcr.io/acmeVendor/source-widgets/nested/leaf",
                    "/credentials",
                )],
            ),
            (
                "no prefix above the repo",
                &[],
                &[(
                    "connectors/ghcr.io/acmeVendor/source-widgets/oauth-client",
                    "/credentials",
                )],
            ),
            ("undeclared image-rule secret", &[BOUND], &[]),
            (
                "undeclared stranger",
                &[("acmeCo/other-team/password", "/password")],
                &[],
            ),
        ];
        let outcomes: Vec<_> = cases
            .into_iter()
            .map(|(case, stanza, declared)| {
                let declared: BTreeMap<String, String> = declared
                    .iter()
                    .map(|(name, pointer)| (name.to_string(), pointer.to_string()))
                    .collect();
                (
                    case,
                    check_secrets(
                        TASK,
                        stanza.iter().copied(),
                        SecretIdentity::Image {
                            image: IMAGE,
                            repository: REPOSITORY,
                            declared: &declared,
                        },
                    )
                    .map_err(|err| format!("{err:#}")),
                )
            })
            .collect();

        insta::assert_debug_snapshot!(outcomes);

        let declared = BTreeMap::from([(
            "vendor/connectors/registry.vendor.test/connectors/registry.attacker.test/source/client"
                .to_string(),
            "/credentials".to_string(),
        )]);
        let err = check_secrets(
            TASK,
            std::iter::empty(),
            SecretIdentity::Image {
                image: "registry.vendor.test/connectors/registry.attacker.test/source:v1",
                repository: "registry.vendor.test/connectors/registry.attacker.test/source",
                declared: &declared,
            },
        )
        .unwrap_err();
        insta::assert_snapshot!(format!("{err:#}"), @"image 'registry.vendor.test/connectors/registry.attacker.test/source:v1' repository contains the reserved 'connectors' component and cannot declare image-owned secrets");
    }

    #[test]
    fn no_image_secret_admission() {
        check_secrets(
            "acmeCo/team/capture",
            [("acmeCo/team/password", "/password")],
            SecretIdentity::NoImage,
        )
        .unwrap();
        insta::assert_snapshot!(
            format!(
                "{:#}",
                check_secrets(
                    "acmeCo/team/capture",
                    [
                        ("acmeCo/team/password", "/password"),
                        ("acmeCo/other/password", "/password"),
                    ],
                    SecretIdentity::NoImage,
                )
                .unwrap_err()
            ),
            @"task 'acmeCo/team/capture' uses secret 'acmeCo/other/password', which is not a sibling of the task (under 'acmeCo/team/'). A connector which runs without an image can use only sibling secrets, as it has no image identity under which to be granted others"
        );
    }

    #[test]
    fn connector_log_policy() {
        let target = bytes::Bytes::from("\"a/b/c\"");
        let logs = [
            json!({"message": "good", "fields": {"eventType": "connectorStatus", "eventTarget": "a/b/c"}}),
            json!({"message": "type", "fields": {"eventType": "other", "eventTarget": "a/b/c"}}),
            json!({"message": "target", "fields": {"eventType": "configUpdate", "eventTarget": "other/task"}}),
            json!({"message": "missing", "fields": {"eventType": "configUpdate"}}),
        ];
        let outcomes: Vec<_> = logs
            .into_iter()
            .map(|value| {
                let log: ops::Log = serde_json::from_value(value).unwrap();
                sanitize_connector_log(&target, log).fields_json_map
            })
            .collect();
        insta::assert_debug_snapshot!(outcomes);
    }

    /// A Log which a hostile container builds serializes once repaired. Each
    /// of its parts would otherwise fail to: the level, the timestamp, and
    /// the field values, of the Log and of its span.
    #[test]
    fn connector_log_is_made_serializable() {
        let hostile = || ops::Log {
            meta: Some(ops::Meta {
                uuid: "forged".to_string(),
            }),
            shard: Some(ops::ShardRef {
                name: "acmeCo/other/task".to_string(),
                ..Default::default()
            }),
            timestamp: Some(proto_flow::Timestamp {
                seconds: i64::MAX,
                nanos: -1,
            }),
            level: -1,
            message: "hostile".to_string(),
            fields_json_map: [
                ("json", r#"{"kept": [1, true]}"#.as_bytes()),
                ("text", b"not json".as_slice()),
                ("truncated", br#"{"a":"#.as_slice()),
                ("binary", b"\"\xff\"".as_slice()),
            ]
            .into_iter()
            .map(|(name, value)| (name.to_string(), bytes::Bytes::copy_from_slice(value)))
            .collect(),
            spans: Vec::new(),
        };
        let mut log = hostile();
        log.spans = vec![hostile()];
        log.timestamp = Some(proto_flow::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        });

        assert!(serde_json::to_string(&log).is_err());

        super::make_serializable(&mut log, || {
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_800_000_000)
        });

        insta::assert_snapshot!(serde_json::to_string_pretty(&log).unwrap(), @r#"
        {
          "ts": "2023-11-14T22:13:20+00:00",
          "level": "warn",
          "message": "hostile",
          "fields": {
            "binary": "\"�\"",
            "json": {"kept": [1, true]},
            "text": "not json",
            "truncated": "{\"a\":"
          },
          "spans": [
            {
              "ts": "2027-01-15T08:00:00+00:00",
              "level": "warn",
              "message": "hostile",
              "fields": {
                "binary": "\"�\"",
                "json": {"kept": [1, true]},
                "text": "not json",
                "truncated": "{\"a\":"
              }
            }
          ]
        }
        "#);
    }

    #[test]
    fn token_restart_deadline_margins() {
        use std::time::Duration;
        let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let expires = now + Duration::from_secs(3600);
        assert_eq!(
            super::token_restart_deadline(now, expires),
            expires - Duration::from_secs(5 * 60)
        );
        let expires = now + Duration::from_secs(12 * 3600);
        assert_eq!(
            super::token_restart_deadline(now, expires),
            expires - Duration::from_secs(30 * 60)
        );
        let expires = now + Duration::from_secs(60);
        assert_eq!(super::token_restart_deadline(now, expires), now);
        assert_eq!(super::token_restart_deadline(now, now), now);
    }

    #[test]
    fn sqlite_vfs_is_runtime_internal_and_sqlite_only() {
        let outcomes =
            [(false, false), (false, true), (true, false), (true, true)].map(|(flag, is_set)| {
                (
                    flag,
                    is_set,
                    check_remote_sqlite_vfs(flag, is_set).is_ok(),
                    check_connector_sqlite_vfs(flag, is_set).is_ok(),
                )
            });

        // Columns are `(is_wire / is_sqlite, uri is set, remote ok, connector ok)`.
        insta::assert_debug_snapshot!(outcomes, @r"
        [
            (
                false,
                false,
                true,
                true,
            ),
            (
                false,
                true,
                true,
                false,
            ),
            (
                true,
                false,
                true,
                true,
            ),
            (
                true,
                true,
                false,
                true,
            ),
        ]
        ");
    }

    #[test]
    fn only_a_host_dir_volume_is_runtime_internal() {
        use crate::proto::request::start::{Ephemeral, Volume};

        let mut outcomes = Vec::new();
        for (transport, is_wire) in [("local", false), ("wire", true)] {
            for (kind, volume) in [
                ("none", None),
                ("host_dir", Some(Volume::HostDir("/a/dir".to_string()))),
                ("ephemeral", Some(Volume::Ephemeral(Ephemeral {}))),
            ] {
                let outcome = match check_remote_volume(is_wire, volume.as_ref()) {
                    Ok(()) => "ok".to_string(),
                    Err(err) => err.to_string(),
                };
                outcomes.push(format!("{transport} {kind}: {outcome}"));
            }
        }

        insta::assert_debug_snapshot!(outcomes, @r#"
        [
            "local none: ok",
            "local host_dir: ok",
            "local ephemeral: ok",
            "wire none: ok",
            "wire host_dir: Start.host_dir is runtime-internal and may not be set by a remote client",
            "wire ephemeral: ok",
        ]
        "#);
    }
}
