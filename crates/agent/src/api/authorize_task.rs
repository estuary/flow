use super::{App, Snapshot};
use crate::api::error::ApiErrorExt;
use anyhow::Context;
use axum::http::StatusCode;
use rand::Rng;
use std::sync::Arc;

type Request = models::authorizations::TaskAuthorizationRequest;
type Response = models::authorizations::TaskAuthorization;

/// Authorizes some set of actions to be performed on a particular collection by way of a task.
/// This checks that:
///     * The request is `iss`ued by an actor in a particular data plane
///         * Validated by checking the request signature against the HMACs for the `iss`uer data-plane
///     * The request is on behalf of a `sub`ject task running in that data plane
///         * The subject task is identified by its `shard_template_id`, not just its name.
///     * The request is to perform some `cap`abilities on a particular collection
///         * The collection is identified by its `journal_template_name`, not just its name.
///         * The target collection is specified as a label selector for the label `name`
///     * The request's subject is granted those capabilities on that collection by
///       the control-plane
///     * The requested collection may be in a different data plane than the issuer.
#[axum::debug_handler]
#[tracing::instrument(skip(app), err(level = tracing::Level::WARN))]
pub async fn authorize_task(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Json(Request { token }): axum::Json<Request>,
) -> Result<axum::Json<Response>, crate::api::ApiError> {
    do_authorize_task(&app.snapshot, token).await
}

async fn do_authorize_task(
    snapshot: &std::sync::RwLock<Snapshot>,
    token: String,
) -> Result<axum::Json<Response>, crate::api::ApiError> {
    let (_header, mut claims) = super::parse_untrusted_data_plane_claims(&token)?;
    let journal_name_or_prefix = labels::expect_one(claims.sel.include(), "name")
        .map_err(|err| anyhow::anyhow!(err).with_status(StatusCode::BAD_REQUEST))?
        .to_owned();

    // Require the request was signed with the AUTHORIZE capability,
    // and then strip this capability before issuing a response token.
    if claims.cap & proto_flow::capability::AUTHORIZE == 0 {
        return Err(
            anyhow::anyhow!("missing required AUTHORIZE capability: {}", claims.cap)
                .with_status(StatusCode::FORBIDDEN),
        );
    }
    claims.cap &= !proto_flow::capability::AUTHORIZE;

    // Validate and match the requested capabilities to a corresponding role.
    // NOTE: Because we pass through the claims after validating them here,
    // we need to explicitly enumerate and exactly match every case, as just
    // checking that the requested capability contains a particular grant isn't enough.
    // For example, we wouldn't want to allow a request for `REPLICATE` just
    // because it also requests `READ`.
    let required_role = match claims.cap {
        cap if (cap == proto_gazette::capability::LIST)
            || (cap == proto_gazette::capability::READ)
            || (cap == (proto_gazette::capability::LIST | proto_gazette::capability::READ)) =>
        {
            models::Capability::Read
        }
        // We're intentionally rejecting requests for both APPLY and APPEND, as those two
        // grants authorize wildly different capabilities, and no sane logic should
        // need both at the same time. So as a sanity check/defense-in-depth measure
        // we won't grant you a token that has both, even if we technically could.
        cap if (cap == proto_gazette::capability::APPLY)
            || (cap == proto_gazette::capability::APPEND) =>
        {
            models::Capability::Write
        }
        cap => {
            return Err(
                anyhow::anyhow!("capability {cap} cannot be authorized by this service")
                    .with_status(StatusCode::FORBIDDEN),
            )
        }
    };

    let (encoding_key, data_plane_fqdn, broker_address) = match Snapshot::evaluate(
        snapshot,
        chrono::DateTime::from_timestamp(claims.iat as i64, 0).unwrap_or_default(),
        |snapshot: &Snapshot| {
            evaluate_authorization(
                snapshot,
                &claims.sub,
                &claims.iss,
                &token,
                &journal_name_or_prefix,
                required_role,
            )
        },
    ) {
        Ok((exp, ok)) => {
            claims.exp = exp.timestamp() as u64;
            ok
        }
        Err(Err(err)) if err.error.downcast_ref::<BlackHole>().is_some() => {
            let BlackHole { ok } = err.error.downcast::<BlackHole>().unwrap();

            // Return a "black hole" response by extending the request claims
            // with a label selector that never matches anything. This:
            // - Causes List RPCs to succeed, but return nothing:
            //   This avoids failing a sourcing task, on the presumption
            //   that it's just awaiting an update from the control plane
            //   and should be allowed to gracefully finish its other work.
            // - Causes Append RPCs to error with status JOURNAL_NOT_FOUND.
            //   This allows for correct handling of recovered ACK intents
            //   destined for a journal which has since been deleted.
            // - Causes an Apply RPC, used to create partitions, to fail.
            //
            // Note that we're directing the task back to it's own data-plane,
            // since we have no idea what data-plane the collection might have
            // once lived in, as we couldn't find it.
            claims.sel.include = Some(labels::add_value(
                claims.sel.include.unwrap_or_default(),
                "estuary.dev/match-nothing",
                "1",
            ));
            // The request predates our latest snapshot, so the client cannot
            // have prior knowledge of a generation ID we don't yet know about.
            // Implication: the referenced collection was reset or deleted.
            claims.exp = claims.iat + rand::thread_rng().gen_range(1800..3600);

            ok
        }
        Err(Ok(backoff)) => {
            return Ok(axum::Json(Response {
                retry_millis: backoff.as_millis() as u64,
                ..Default::default()
            }))
        }
        Err(Err(err)) => return Err(err),
    };

    claims.iss = data_plane_fqdn;

    let token = jsonwebtoken::encode(&jsonwebtoken::Header::default(), &claims, &encoding_key)
        .context("failed to encode authorized JWT")?;

    Ok(axum::Json(Response {
        broker_address,
        token,
        ..Default::default()
    }))
}

// A BlackHole error is raised when a task request is authorized,
// but its `journal_name_or_prefix` doesn't map to a collection
// (either because the collection is deleted, or exists under a
// different generation ID).
//
// We return an error (which triggers a snapshot, and is retried)
// to preserve causality: the client may have prior knowledge of a
// generation ID we don't yet and we cannot black-hole the request
// without that knowledge.
#[derive(thiserror::Error)]
#[error("black hole")]
struct BlackHole {
    ok: (jsonwebtoken::EncodingKey, String, String),
}

impl std::fmt::Debug for BlackHole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

fn evaluate_authorization(
    snapshot: &Snapshot,
    shard_id: &str,
    shard_data_plane_fqdn: &str,
    token: &str,
    journal_name_or_prefix: &str,
    required_role: models::Capability,
) -> Result<
    (
        Option<chrono::DateTime<chrono::Utc>>,
        (jsonwebtoken::EncodingKey, String, String),
    ),
    crate::api::ApiError,
> {
    // Map `claims.iss`, a data-plane FQDN, into its token-verified data-plane.
    let Some(task_data_plane) = snapshot
        .verify_data_plane_token(shard_data_plane_fqdn, token)
        .context("invalid data-plane hmac key")?
    else {
        return Err(
            anyhow::anyhow!("no data-plane keys validated against the token signature")
                .with_status(StatusCode::FORBIDDEN),
        );
    };

    // Map `claims.sub`, a Shard ID, into a task running in `task_data_plane`.
    let Some(task) = snapshot
        .task_by_shard_id(shard_id)
        .filter(|task| task.data_plane_id == task_data_plane.control_id)
    else {
        return Err(anyhow::anyhow!(
            "task shard {shard_id} within data-plane {shard_data_plane_fqdn} is not known"
        )
        .with_status(StatusCode::PRECONDITION_FAILED));
    };

    // Map a required `name` journal label selector into its collection.
    let collection = snapshot.collection_by_journal_name(journal_name_or_prefix);

    let (found, collection_data_plane, is_ops) = if let Some(collection) = collection {
        let Some(collection_data_plane) =
            snapshot.data_planes.get_by_key(&collection.data_plane_id)
        else {
            return Err(anyhow::anyhow!(
                "collection data-plane {} not found",
                collection.data_plane_id
            )
            .with_status(StatusCode::INTERNAL_SERVER_ERROR));
        };

        // As a special case outside of the RBAC system, allow a task to write
        // to its designated partition within its ops collections.
        let is_ops = required_role == models::Capability::Write
            && (collection.collection_name == task_data_plane.ops_logs_name
                || collection.collection_name == task_data_plane.ops_stats_name)
            && journal_name_or_prefix.ends_with(&super::ops_suffix(task));

        (true, collection_data_plane, is_ops)
    } else {
        // This collection doesn't exist, or exists under a different generation ID.
        (false, task_data_plane, false)
    };

    if !is_ops
        && !tables::RoleGrant::is_authorized(
            &snapshot.role_grants,
            &task.task_name,
            &journal_name_or_prefix,
            required_role,
        )
    {
        tracing::warn!(
            %task.spec_type,
            %shard_id,
            %journal_name_or_prefix,
            ?required_role,
            ops_logs=%task_data_plane.ops_logs_name,
            ops_stats=%task_data_plane.ops_stats_name,
            ops_suffix=%super::ops_suffix(task),
            "task authorization rejection context"
        );
        return Err(anyhow::anyhow!(
            "task shard {shard_id} is not authorized to {journal_name_or_prefix} for {required_role:?}"
        ).with_status(StatusCode::FORBIDDEN));
    }

    let Some(encoding_key) =
        snapshot.data_plane_first_hmac_key(&collection_data_plane.data_plane_name)
    else {
        return Err(anyhow::anyhow!(
            "collection data-plane {} has no configured HMAC keys",
            collection_data_plane.data_plane_name
        )
        .with_status(StatusCode::INTERNAL_SERVER_ERROR));
    };
    let encoding_key = jsonwebtoken::EncodingKey::from_base64_secret(&encoding_key)
        .context("invalid data-plane hmac key")?;

    let cordon_at = match (
        snapshot.cordon_at(&task.task_name, task_data_plane),
        collection.and_then(|collection| {
            snapshot.cordon_at(&collection.collection_name, collection_data_plane)
        }),
    ) {
        (Some(l), Some(r)) => Some(l.min(r)),
        (Some(i), None) | (None, Some(i)) => Some(i),
        (None, None) => None,
    };

    let ok = (
        encoding_key,
        collection_data_plane.data_plane_fqdn.clone(),
        super::maybe_rewrite_address(
            task.data_plane_id != collection_data_plane.control_id,
            &collection_data_plane.broker_address,
        ),
    );

    if found {
        Ok((cordon_at, ok))
    } else {
        Err(anyhow::anyhow!(BlackHole { ok }).with_status(StatusCode::INTERNAL_SERVER_ERROR))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_collection_success() {
        let outcome = run(proto_gazette::Claims {
            iat: 0,
            exp: 0,
            cap: proto_flow::capability::AUTHORIZE | proto_gazette::capability::APPEND,
            iss: "fqdn1".to_string(),
            sel: proto_gazette::LabelSelector {
                include: Some(labels::build_set([(
                    "name",
                    "acmeCo/pineapples/1122334455667788/pivot=00",
                )])),
                exclude: None,
            },
            sub: "capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000".to_string(),
        })
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": [
            "broker.1",
            {
              "cap": 16,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn1",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "name",
                      "value": "acmeCo/pineapples/1122334455667788/pivot=00"
                    }
                  ]
                }
              },
              "sub": "capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000"
            }
          ]
        }
        "###);
    }

    #[tokio::test]
    async fn test_ops_success() {
        let outcome = run(proto_gazette::Claims {
            iat: 0,
            exp: 0,
            cap: proto_flow::capability::AUTHORIZE | proto_gazette::capability::APPEND,
            iss: "fqdn1".to_string(),
            sel: proto_gazette::LabelSelector {
                include: Some(labels::build_set([(
                    "name",
                    "ops/tasks/public/plane-one/logs/1122334455667788/kind=capture/name=acmeCo%2Fsource-pineapple/pivot=00",
                )])),
                exclude: None,
            },
            sub: "capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000".to_string(),
        })
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": [
            "broker.1",
            {
              "cap": 16,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn1",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "name",
                      "value": "ops/tasks/public/plane-one/logs/1122334455667788/kind=capture/name=acmeCo%2Fsource-pineapple/pivot=00"
                    }
                  ]
                }
              },
              "sub": "capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000"
            }
          ]
        }
        "###);
    }

    #[tokio::test]
    async fn test_not_authorized() {
        let outcome = run(proto_gazette::Claims {
            iat: 0,
            exp: 0,
            cap: proto_flow::capability::AUTHORIZE | proto_gazette::capability::APPEND,
            iss: "fqdn1".to_string(),
            sel: proto_gazette::LabelSelector {
                include: Some(labels::build_set([(
                    "name",
                    "bobCo/bananas/1122334455667788/pivot=00",
                )])),
                exclude: None,
            },
            sub: "capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000".to_string(),
        })
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "task shard capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000 is not authorized to bobCo/bananas/1122334455667788/pivot=00 for Write"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_cordon_collection() {
        let outcome = run(proto_gazette::Claims {
            iat: 0,
            exp: 0,
            cap: proto_flow::capability::AUTHORIZE | proto_gazette::capability::APPEND,
            iss: "fqdn1".to_string(),
            sel: proto_gazette::LabelSelector {
                include: Some(labels::build_set([(
                    "name",
                    "acmeCo/bananas/1122334455667788/pivot=00",
                )])),
                exclude: None,
            },
            sub: "capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000".to_string(),
        })
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 500,
            "error": "retry"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_cordon_task() {
        let outcome = run(proto_gazette::Claims {
            iat: 0,
            exp: 0,
            cap: proto_flow::capability::AUTHORIZE | proto_gazette::capability::APPEND,
            iss: "fqdn1".to_string(),
            sel: proto_gazette::LabelSelector {
                include: Some(labels::build_set([(
                    "name",
                    "acmeCo/pineapples/1122334455667788/pivot=00",
                )])),
                exclude: None,
            },
            sub: "capture/acmeCo/source-banana/0011223344556677/00000000-00000000".to_string(),
        })
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 500,
            "error": "retry"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_black_hole() {
        let mut claims = proto_gazette::Claims {
            iat: 1, // After current snapshot taken.
            exp: 0,
            cap: proto_flow::capability::AUTHORIZE | proto_gazette::capability::APPEND,
            iss: "fqdn1".to_string(),
            sel: proto_gazette::LabelSelector {
                include: Some(labels::build_set([(
                    "name",
                    "acmeCo/pineapples/88667755330099/pivot=00", // Not found.
                )])),
                exclude: None,
            },
            sub: "capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000".to_string(),
        };

        insta::assert_json_snapshot!(run(claims.clone()).await, @r###"
        {
          "Err": {
            "status": 500,
            "error": "retry"
          }
        }
        "###);

        claims.iat = 0; // Now it's taken prior to current snapshot.
        insta::assert_json_snapshot!(run(claims).await, @r###"
        {
          "Ok": [
            "broker.1",
            {
              "cap": 16,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn1",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "estuary.dev/match-nothing",
                      "value": "1"
                    },
                    {
                      "name": "name",
                      "value": "acmeCo/pineapples/88667755330099/pivot=00"
                    }
                  ]
                }
              },
              "sub": "capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000"
            }
          ]
        }
        "###);
    }

    async fn run(
        mut claims: proto_gazette::Claims,
    ) -> Result<(String, proto_gazette::Claims), crate::api::ApiError> {
        let taken = chrono::Utc::now();
        let snapshot = Snapshot::build_fixture(Some(taken));
        let snapshot = std::sync::RwLock::new(snapshot);

        claims.iat = claims.iat + taken.timestamp() as u64;
        claims.exp = taken.timestamp() as u64 + 100;

        let request_token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret("key1".as_bytes()),
        )
        .unwrap();

        let Response {
            token: response_token,
            broker_address,
            retry_millis,
        } = do_authorize_task(&snapshot, request_token).await?.0;

        if retry_millis != 0 {
            return Err(anyhow::anyhow!("retry").into());
        }

        // Decode and verify the response token.
        let mut decoded = jsonwebtoken::decode::<proto_gazette::Claims>(
            &response_token,
            &jsonwebtoken::DecodingKey::from_secret("key1".as_bytes()),
            &jsonwebtoken::Validation::default(),
        )
        .expect("failed to decode response token")
        .claims;
        (decoded.iat, decoded.exp) = (0, 0);

        Ok((broker_address, decoded))
    }
}
