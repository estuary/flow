type Request = models::authorizations::UserCollectionAuthorizationRequest;
type Response = models::authorizations::UserCollectionAuthorization;

#[axum::debug_handler(state=std::sync::Arc<crate::App>)]
#[tracing::instrument(skip(env), err(Debug, level = tracing::Level::WARN))]
pub async fn authorize_user_collection(
    mut env: crate::Envelope,
    super::Request(Request {
        collection,
        capability_bundle,
        started_unix,
    }): super::Request<Request>,
) -> Result<axum::Json<Response>, crate::ApiError> {
    // Legacy: if `started_unix` is set, then use as the logical request start
    // rounded up to the next second (as it was round down when encoded).
    if started_unix != 0 {
        env.started =
            tokens::DateTime::from_timestamp_secs(1 + started_unix as i64).unwrap_or_default();
    }

    let policy_result = evaluate_authorization(
        env.snapshot(),
        env.subject()?,
        env.claims()?.email.as_deref(),
        &collection,
        capability_bundle,
    );

    // Legacy: if `started_unix` was set then use a custom 200 response for client-side retries.
    let (expiry, (data_plane, mut claims, journal_name_prefix)) =
        match env.authorization_outcome(policy_result).await {
            Ok(ok) => ok,
            Err(crate::ApiError::AuthZRetry(retry)) if started_unix != 0 => {
                return Ok(axum::Json(Response {
                    retry_millis: (retry.retry_after - retry.failed).num_milliseconds() as u64,
                    ..Default::default()
                }));
            }
            Err(err) => return Err(err),
        };

    claims.iat = env.started.timestamp() as u64;
    claims.exp = expiry.timestamp() as u64;

    let broker_token = data_plane.sign_claims(claims)?;

    Ok(axum::Json(Response {
        broker_address: data_plane.broker_address.clone(),
        broker_token,
        journal_name_prefix,
        retry_millis: 0,
    }))
}

fn evaluate_authorization<'s>(
    snapshot: &'s crate::Snapshot,
    subject: &models::authz::Subject,
    user_email: Option<&str>,
    collection_name: &models::Collection,
    capability_bundle: models::authz::CapabilityBundle,
) -> crate::AuthZResult<(
    &'s crate::snapshot::DataPlane,
    proto_gazette::Claims,
    String,
)> {
    let user_id = subject.user_id;

    let (claim_bits, cap) = super::evaluate_user_claims(
        snapshot,
        subject,
        user_email,
        collection_name,
        capability_bundle,
    )?;
    () = super::evaluate_support_apply(
        snapshot,
        subject,
        user_email,
        collection_name,
        capability_bundle,
        claim_bits,
    )?;

    let Some(collection) = snapshot.collection_by_catalog_name(collection_name) else {
        return Err(tonic::Status::not_found(format!(
            "collection {collection_name} is not known"
        )));
    };
    let Some(data_plane) = snapshot.data_plane_by_id(collection.data_plane_id) else {
        return Err(tonic::Status::internal(format!(
            "collection data-plane {} not found",
            collection.data_plane_id
        )));
    };
    let claims = proto_gazette::Claims {
        cap,
        exp: 0, // Filled later.
        iat: 0, // Filled later.
        iss: data_plane.data_plane_fqdn.clone(),
        sub: user_id.to_string(),
        sel: proto_gazette::broker::LabelSelector {
            include: Some(labels::build_set([
                ("name:prefix", collection.journal_template_name.as_str()),
                (labels::COLLECTION, collection_name.as_str()),
            ])),
            exclude: None,
        },
    };

    Ok((
        snapshot.cordon_at(&collection.collection_name, data_plane),
        (data_plane, claims, collection.journal_template_name.clone()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_success() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/anvils/peaches"),
            models::authz::CapabilityBundle::Writer,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": [
            "broker.2",
            "bobCo/anvils/peaches/1122334455667788/",
            {
              "cap": 26,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "estuary.dev/collection",
                      "value": "bobCo/anvils/peaches"
                    },
                    {
                      "name": "name",
                      "value": "bobCo/anvils/peaches/1122334455667788/",
                      "prefix": true
                    }
                  ]
                }
              },
              "sub": "20202020-2020-2020-2020-202020202020"
            }
          ]
        }
        "###);
    }

    #[test]
    fn test_not_authorized() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("acmeCo/other/thing"),
            models::authz::CapabilityBundle::Viewer,
        );

        insta::assert_json_snapshot!(outcome, @r#"
        {
          "Err": {
            "status": 403,
            "error": "the presented credential (user: bob@bob) lacks the journal and shard capability bits of the viewer capability bundle against 'acmeCo/other/thing'"
          }
        }
        "#);
    }

    #[test]
    fn test_capability_bundle_too_high() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/anvils/peaches"),
            models::authz::CapabilityBundle::Admin,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "the presented credential (user: bob@bob) lacks the journal and shard capability bits of the admin capability bundle against 'bobCo/anvils/peaches'"
          }
        }
        "###);
    }

    #[test]
    fn test_not_found() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/widgets/not/found"),
            models::authz::CapabilityBundle::Viewer,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 404,
            "error": "collection bobCo/widgets/not/found is not known"
          }
        }
        "###);
    }

    #[test]
    fn test_cordon() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/widgets/squashes"),
            models::authz::CapabilityBundle::Viewer,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok_Cordoned": [
            "broker.2",
            "bobCo/widgets/squashes/1122334455667788/",
            {
              "cap": 10,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "estuary.dev/collection",
                      "value": "bobCo/widgets/squashes"
                    },
                    {
                      "name": "name",
                      "value": "bobCo/widgets/squashes/1122334455667788/",
                      "prefix": true
                    }
                  ]
                }
              },
              "sub": "20202020-2020-2020-2020-202020202020"
            }
          ]
        }
        "###);
    }

    #[test]
    fn test_bob_cannot_get_admin_even_with_admin_grant() {
        // bob@bob has admin capability on bobCo/tires/ but lacks estuary_support/
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/tires/collection"),
            models::authz::CapabilityBundle::Admin,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "the presented credential (user: bob@bob) requests APPLY to 'bobCo/tires/collection' through the admin capability bundle, and APPLY requires the admin capability bundle on 'estuary_support/'"
          }
        }
        "###);
    }

    #[test]
    fn test_admin_with_estuary_support_grant() {
        // alice@alice has estuary_support/ grant in the fixture, so admin should succeed
        let outcome = run(
            uuid::Uuid::from_bytes([64; 16]),
            Some("alice@alice".to_string()),
            models::Collection::new("aliceCo/wonderland/data"),
            models::authz::CapabilityBundle::Admin,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": [
            "broker.2",
            "aliceCo/wonderland/data/1122334455667788/",
            {
              "cap": 30,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "estuary.dev/collection",
                      "value": "aliceCo/wonderland/data"
                    },
                    {
                      "name": "name",
                      "value": "aliceCo/wonderland/data/1122334455667788/",
                      "prefix": true
                    }
                  ]
                }
              },
              "sub": "40404040-4040-4040-4040-404040404040"
            }
          ]
        }
        "###);
    }

    /// The Gazette capabilities a request for each bundle conveys, where the
    /// caller holds every bit. Bundles without claim bits are rejected.
    #[test]
    fn test_claims_of_each_bundle() {
        let outcomes = <models::authz::CapabilityBundle as strum::VariantArray>::VARIANTS
            .iter()
            .map(|bundle| {
                let outcome = match run(
                    uuid::Uuid::from_bytes([64; 16]),
                    Some("alice@alice".to_string()),
                    models::Collection::new("aliceCo/wonderland/data"),
                    *bundle,
                ) {
                    Outcome::Ok((_, _, claims)) => format!("cap {}", claims.cap),
                    Outcome::OkCordoned(_) => unreachable!("not cordoned"),
                    Outcome::Err { status, error } => format!("{status}: {error}"),
                };
                (*bundle, outcome)
            })
            .collect::<Vec<_>>();

        insta::assert_json_snapshot!(outcomes, @r#"
        [
          [
            "viewer",
            "cap 10"
          ],
          [
            "writer",
            "cap 26"
          ],
          [
            "editor",
            "cap 14"
          ],
          [
            "admin",
            "cap 30"
          ],
          [
            "billing",
            "400: the billing capability bundle conveys no journal or shard capability bits"
          ],
          [
            "team_admin",
            "400: the team_admin capability bundle conveys no journal or shard capability bits"
          ],
          [
            "manage_service_accounts",
            "400: the manage_service_accounts capability bundle conveys no journal or shard capability bits"
          ],
          [
            "manage_data_plane",
            "400: the manage_data_plane capability bundle conveys no journal or shard capability bits"
          ],
          [
            "create_sandbox",
            "400: the create_sandbox capability bundle conveys no journal or shard capability bits"
          ],
          [
            "delegate",
            "400: the delegate capability bundle conveys no journal or shard capability bits"
          ],
          [
            "assume",
            "400: the assume capability bundle conveys no journal or shard capability bits"
          ],
          [
            "view_billing",
            "400: the view_billing capability bundle conveys no journal or shard capability bits"
          ],
          [
            "edit_billing",
            "400: the edit_billing capability bundle conveys no journal or shard capability bits"
          ],
          [
            "edit_secret",
            "400: the edit_secret capability bundle conveys no journal or shard capability bits"
          ],
          [
            "view_data_plane_private_networking",
            "400: the view_data_plane_private_networking capability bundle conveys no journal or shard capability bits"
          ],
          [
            "modify_data_plane_private_networking",
            "400: the modify_data_plane_private_networking capability bundle conveys no journal or shard capability bits"
          ],
          [
            "catalog_read",
            "400: the catalog_read capability bundle conveys no journal or shard capability bits"
          ]
        ]
        "#);
    }

    /// A caller holding only the `Editor` bundle holds `JournalRead`, which is
    /// all a `Viewer` request requires. Its `SpecEdit` does not obtain APPLY
    /// without the `estuary_support/` grant.
    #[test]
    fn test_editor_only_caller() {
        let user_id = uuid::Uuid::from_bytes([0x33; 16]);
        let mut snapshot = crate::Snapshot::build_fixture(None);
        snapshot.user_grants.insert(tables::UserGrant {
            user_id,
            object_role: models::Prefix::new("bobCo/anvils/"),
            capability_bits: models::authz::CapabilityBundle::Editor.bits(),
        });
        let subject = models::authz::Subject::unrestricted(user_id);

        let outcomes = [
            models::authz::CapabilityBundle::Viewer,
            models::authz::CapabilityBundle::Writer,
            models::authz::CapabilityBundle::Editor,
        ]
        .map(|bundle| {
            run_with(
                &snapshot,
                &subject,
                Some("editor@acme".to_string()),
                models::Collection::new("bobCo/anvils/peaches"),
                bundle,
            )
        });

        insta::assert_json_snapshot!(outcomes, @r#"
        [
          {
            "Ok": [
              "broker.2",
              "bobCo/anvils/peaches/1122334455667788/",
              {
                "cap": 10,
                "exp": 0,
                "iat": 0,
                "iss": "fqdn2",
                "sel": {
                  "include": {
                    "labels": [
                      {
                        "name": "estuary.dev/collection",
                        "value": "bobCo/anvils/peaches"
                      },
                      {
                        "name": "name",
                        "value": "bobCo/anvils/peaches/1122334455667788/",
                        "prefix": true
                      }
                    ]
                  }
                },
                "sub": "33333333-3333-3333-3333-333333333333"
              }
            ]
          },
          {
            "Err": {
              "status": 403,
              "error": "the presented credential (user: editor@acme) lacks the journal and shard capability bits of the writer capability bundle against 'bobCo/anvils/peaches'"
            }
          },
          {
            "Err": {
              "status": 403,
              "error": "the presented credential (user: editor@acme) requests APPLY to 'bobCo/anvils/peaches' through the editor capability bundle, and APPLY requires the admin capability bundle on 'estuary_support/'"
            }
          }
        ]
        "#);
    }

    /// The `estuary_support/` check requires the full `Admin` bundle, so a
    /// token masked to less than `admin` cannot obtain APPLY. Its mask does
    /// cover the claim bits of an `Editor` request.
    #[test]
    fn test_masked_support_caller() {
        let snapshot = crate::Snapshot::build_fixture(None);

        let outcomes = [vec!["editor"], vec!["editor", "team_admin"], vec!["admin"]].map(|mask| {
            let mask: Vec<String> = mask.into_iter().map(String::from).collect();
            let subject = models::authz::Subject {
                user_id: uuid::Uuid::from_bytes([64; 16]),
                capability_mask: Some(models::authz::parse_capability_mask(&mask).unwrap()),
                prefix_mask: None,
            };
            let outcome = run_with(
                &snapshot,
                &subject,
                Some("alice@alice".to_string()),
                models::Collection::new("aliceCo/wonderland/data"),
                models::authz::CapabilityBundle::Editor,
            );
            (mask, outcome)
        });

        insta::assert_json_snapshot!(outcomes, @r#"
        [
          [
            [
              "editor"
            ],
            {
              "Err": {
                "status": 403,
                "error": "the presented credential (user: alice@alice) requests APPLY to 'aliceCo/wonderland/data' through the editor capability bundle, and APPLY requires the admin capability bundle on 'estuary_support/'"
              }
            }
          ],
          [
            [
              "editor",
              "team_admin"
            ],
            {
              "Err": {
                "status": 403,
                "error": "the presented credential (user: alice@alice) requests APPLY to 'aliceCo/wonderland/data' through the editor capability bundle, and APPLY requires the admin capability bundle on 'estuary_support/'"
              }
            }
          ],
          [
            [
              "admin"
            ],
            {
              "Ok": [
                "broker.2",
                "aliceCo/wonderland/data/1122334455667788/",
                {
                  "cap": 14,
                  "exp": 0,
                  "iat": 0,
                  "iss": "fqdn2",
                  "sel": {
                    "include": {
                      "labels": [
                        {
                          "name": "estuary.dev/collection",
                          "value": "aliceCo/wonderland/data"
                        },
                        {
                          "name": "name",
                          "value": "aliceCo/wonderland/data/1122334455667788/",
                          "prefix": true
                        }
                      ]
                    }
                  },
                  "sub": "40404040-4040-4040-4040-404040404040"
                }
              ]
            }
          ]
        ]
        "#);
    }

    // Serialization wrapper that distinguishes cordoned vs non-cordoned success.
    #[derive(serde::Serialize)]
    enum Outcome {
        Ok((String, String, proto_gazette::Claims)),
        #[serde(rename = "Ok_Cordoned")]
        OkCordoned((String, String, proto_gazette::Claims)),
        Err {
            status: u16,
            error: String,
        },
    }

    fn run(
        user_id: uuid::Uuid,
        email: Option<String>,
        collection: models::Collection,
        capability_bundle: models::authz::CapabilityBundle,
    ) -> Outcome {
        let snapshot = crate::Snapshot::build_fixture(None);
        let subject = models::authz::Subject::unrestricted(user_id);
        run_with(&snapshot, &subject, email, collection, capability_bundle)
    }

    fn run_with(
        snapshot: &crate::Snapshot,
        subject: &models::authz::Subject,
        email: Option<String>,
        collection: models::Collection,
        capability_bundle: models::authz::CapabilityBundle,
    ) -> Outcome {
        match evaluate_authorization(
            snapshot,
            subject,
            email.as_deref(),
            &collection,
            capability_bundle,
        ) {
            Ok((cordon_at, (data_plane, mut data_claims, journal_name_prefix))) => {
                // Zero out timestamps for stable snapshots.
                data_claims.iat = 0;
                data_claims.exp = 0;
                let broker_address = data_plane.broker_address.clone();

                if cordon_at.is_some() {
                    Outcome::OkCordoned((broker_address, journal_name_prefix, data_claims))
                } else {
                    Outcome::Ok((broker_address, journal_name_prefix, data_claims))
                }
            }
            Err(status) => Outcome::Err {
                status: tokens::rest::grpc_status_code_to_http(status.code()),
                error: status.message().to_string(),
            },
        }
    }

    // Integration tests below use sqlx::test with actual database
    use crate::test_server;
    use flow_client_next as flow_client;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_user_collection_auth_success(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let user_tokens = server.make_fixed_user_tokens(uuid::Uuid::from_bytes([0x11; 16]), None);
        tokio::time::pause();

        let source = flow_client::workflows::UserCollectionAuth {
            client: server.rest_client(),
            user_tokens: user_tokens.clone(),
            collection: models::Collection::new("aliceCo/data/foo"),
            capability_bundle: models::authz::CapabilityBundle::Writer,
        };
        let refresh = tokens::watch(source).ready_owned().await;

        insta::assert_json_snapshot!(
            refresh.token().result().unwrap(),
            {".brokerToken" => "<redacted>"},
            @r###"
            {
              "brokerAddress": "broker.dp.one",
              "brokerToken": "<redacted>",
              "journalNamePrefix": "aliceCo/data/foo/gen1234/",
              "retryMillis": 0
            }
            "###,
        );
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_user_collection_auth_failure(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let user_tokens = server.make_fixed_user_tokens(
            uuid::Uuid::from_bytes([0x11; 16]),
            Some("alice@example.com"),
        );
        tokio::time::pause();

        let source = flow_client::workflows::UserCollectionAuth {
            client: server.rest_client(),
            user_tokens: user_tokens.clone(),
            collection: models::Collection::new("Some/Other/Collection"),
            capability_bundle: models::authz::CapabilityBundle::Writer,
        };
        let refresh = tokens::watch(source).ready_owned().await;

        insta::assert_debug_snapshot!(
            refresh.token().result().unwrap_err(),
            @r#"
        Status {
            code: PermissionDenied,
            message: "the presented credential (user: alice@example.com) lacks the journal and shard capability bits of the writer capability bundle against 'Some/Other/Collection'",
            source: None,
        }
        "#,
        );
    }
}
