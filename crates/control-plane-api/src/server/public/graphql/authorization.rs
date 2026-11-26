/// A terminal authorization denial. Resolvers with custom authorization checks
/// can return this directly after completing snapshot refresh and retry handling.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(super) struct AuthorizationDenied {
    pub capability_bits: models::authz::CapabilityBits,
    pub message: String,
}

pub(super) struct AuthorizationErrors;

impl async_graphql::extensions::ExtensionFactory for AuthorizationErrors {
    fn create(&self) -> std::sync::Arc<dyn async_graphql::extensions::Extension> {
        std::sync::Arc::new(Self)
    }
}

#[async_trait::async_trait]
impl async_graphql::extensions::Extension for AuthorizationErrors {
    async fn execute(
        &self,
        ctx: &async_graphql::extensions::ExtensionContext<'_>,
        operation_name: Option<&str>,
        next: async_graphql::extensions::NextExecute<'_>,
    ) -> async_graphql::Response {
        let mut response = next.run(ctx, operation_name).await;
        for error in &mut response.errors {
            let Some(denial) = error.source::<AuthorizationDenied>() else {
                continue;
            };
            // Each option is a list of bundles which together hold the
            // denied bits. Today every option is a single bundle.
            let options: Vec<Vec<String>> =
                models::authz::bundles_containing(denial.capability_bits)
                    .map(|bundle| vec![bundle_name(bundle)])
                    .collect();
            let descriptions: Vec<_> = options.iter().map(|option| option.join(" + ")).collect();
            error.message.push_str(&format!(
                ". Requires capabilities: {}",
                descriptions.join(" or ")
            ));
            let extensions = error.extensions.get_or_insert_with(Default::default);
            extensions.set("code", "FORBIDDEN");
            extensions.set("requiredBundleOptions", options);
        }
        response
    }
}

/// Verifies that the caller holds the `required` capability bits to every one
/// of `prefixes_or_names`.
pub(super) async fn verify_authorization<S>(
    env: &crate::Envelope,
    prefixes_or_names: impl IntoIterator<Item = S>,
    required: impl Into<models::authz::CapabilityBits>,
) -> async_graphql::Result<()>
where
    S: AsRef<str> + std::fmt::Display,
{
    let required = required.into();
    let policy_result = crate::server::evaluate_names_authorization(
        env.snapshot(),
        env.subject()?,
        env.claims()?.email.as_deref(),
        required,
        prefixes_or_names,
    );
    authorization_outcome(env, policy_result, required).await
}

/// `Envelope::authorization_outcome` of a check that the caller holds the
/// `required` capability bits. It refreshes stale permissions before reporting
/// a terminal denial, which becomes an `AuthorizationDenied`. Retry errors
/// retain their ApiError source for the HTTP handler's redirect handling.
pub(super) async fn authorization_outcome<T>(
    env: &crate::Envelope,
    policy_result: crate::AuthZResult<T>,
    required: models::authz::CapabilityBits,
) -> async_graphql::Result<T> {
    match env.authorization_outcome(policy_result).await {
        Ok((_expiry, ok)) => Ok(ok),
        Err(crate::ApiError::Status(status)) if status.code() == tonic::Code::PermissionDenied => {
            Err(async_graphql::Error::new_with_source(AuthorizationDenied {
                capability_bits: required,
                message: async_graphql::Error::from(crate::ApiError::Status(status)).message,
            }))
        }
        Err(error) => Err(error.into()),
    }
}

fn bundle_name(bundle: models::authz::CapabilityBundle) -> String {
    serde_json::to_value(bundle)
        .expect("bundle serializes")
        .as_str()
        .expect("bundle is a string")
        .to_owned()
}

#[cfg(test)]
mod tests {
    struct Query;

    #[async_graphql::Object]
    impl Query {
        async fn denied(&self) -> async_graphql::Result<Option<bool>> {
            Err(async_graphql::Error::new_with_source(
                super::AuthorizationDenied {
                    capability_bits: models::authz::CapabilityBit::EditBilling.into(),
                    message: "billing access denied".to_owned(),
                },
            ))
        }

        async fn hidden(&self) -> async_graphql::Result<Option<bool>> {
            Err(super::super::service_accounts::hide_denial_as_not_found(
                async_graphql::Error::new_with_source(super::AuthorizationDenied {
                    capability_bits: models::authz::CapabilityBit::CreateApiKey.into(),
                    message: "key access denied".to_owned(),
                }),
            ))
        }

        async fn retry(&self) -> async_graphql::Result<Option<bool>> {
            let error = crate::ApiError::AuthZRetry(crate::AuthZRetry {
                original_uri: "/graphql".parse().unwrap(),
                started: tokens::DateTime::UNIX_EPOCH,
                failed: tokens::DateTime::UNIX_EPOCH,
                retry_after: tokens::DateTime::UNIX_EPOCH,
                status: tonic::Status::permission_denied("refresh pending"),
            });
            Err(super::super::service_accounts::hide_denial_as_not_found(
                error.into(),
            ))
        }

        async fn unavailable(&self) -> async_graphql::Result<Option<bool>> {
            Err(
                crate::ApiError::Status(tonic::Status::unavailable("temporarily unavailable"))
                    .into(),
            )
        }
    }

    #[tokio::test]
    async fn formats_custom_resolver_denials() {
        let schema = async_graphql::Schema::build(
            Query,
            async_graphql::EmptyMutation,
            async_graphql::EmptySubscription,
        )
        .extension(super::AuthorizationErrors)
        .finish();
        let retry = schema.execute("{ retry }").await;
        assert!(matches!(
            retry.errors[0].source::<crate::ApiError>(),
            Some(crate::ApiError::AuthZRetry(_))
        ));
        assert!(retry.errors[0].extensions.is_none());

        // Resolver errors abort query execution, so exercise each error independently.
        let denied = schema.execute("{ denied }").await;
        let hidden = schema.execute("{ hidden }").await;
        let unavailable = schema.execute("{ unavailable }").await;
        assert!(
            denied.errors[0]
                .source::<super::AuthorizationDenied>()
                .is_some()
        );
        assert!(matches!(
            unavailable.errors[0].source::<crate::ApiError>(),
            Some(crate::ApiError::Status(status)) if status.code() == tonic::Code::Unavailable
        ));
        let responses = [denied, hidden, unavailable];
        insta::assert_json_snapshot!(responses, @r#"
        [
          {
            "data": null,
            "errors": [
              {
                "message": "billing access denied. Requires capabilities: edit_billing or billing or admin",
                "locations": [
                  {
                    "line": 1,
                    "column": 3
                  }
                ],
                "path": [
                  "denied"
                ],
                "extensions": {
                  "code": "FORBIDDEN",
                  "requiredBundleOptions": [
                    [
                      "edit_billing"
                    ],
                    [
                      "billing"
                    ],
                    [
                      "admin"
                    ]
                  ]
                }
              }
            ]
          },
          {
            "data": null,
            "errors": [
              {
                "message": "service account API key not found",
                "locations": [
                  {
                    "line": 1,
                    "column": 3
                  }
                ],
                "path": [
                  "hidden"
                ]
              }
            ]
          },
          {
            "data": null,
            "errors": [
              {
                "message": "Unavailable: temporarily unavailable",
                "locations": [
                  {
                    "line": 1,
                    "column": 3
                  }
                ],
                "path": [
                  "unavailable"
                ]
              }
            ]
          }
        ]
        "#);
    }
}
