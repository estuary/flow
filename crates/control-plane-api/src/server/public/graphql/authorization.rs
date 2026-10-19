/// A terminal authorization denial. Resolvers with custom authorization checks
/// can return this directly after completing snapshot refresh and retry handling.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(super) struct AuthorizationDenied {
    pub capability: models::authz::Capability,
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
            let options = bundle_options(denial.capability);
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

// Refresh stale permissions before reporting a terminal denial. Retry errors
// retain their ApiError source for the HTTP handler's redirect handling.
pub(super) async fn verify_authorization(
    env: &crate::Envelope,
    prefix: &str,
    capability: models::authz::Capability,
) -> async_graphql::Result<()> {
    let policy_result = crate::server::evaluate_names_authorization(
        env.snapshot(),
        env.claims()?,
        capability,
        [prefix],
    );
    match env.authorization_outcome(policy_result).await {
        Ok(_) => Ok(()),
        Err(error) => {
            if matches!(&error, crate::ApiError::Status(status) if status.code() == tonic::Code::PermissionDenied)
            {
                return Err(async_graphql::Error::new_with_source(AuthorizationDenied {
                    capability,
                    message: async_graphql::Error::from(error).message,
                }));
            }
            Err(error.into())
        }
    }
}

fn bundle_options(required: models::authz::Capability) -> Vec<Vec<String>> {
    use models::authz::CapabilityBundle;
    let mut options: Vec<_> = <CapabilityBundle as strum::VariantArray>::VARIANTS
        .iter()
        .filter_map(|bundle| {
            let capabilities = bundle.capabilities();
            if !capabilities.contains(required) {
                return None;
            }
            let name = serde_json::to_value(bundle)
                .expect("bundle serializes")
                .as_str()
                .expect("bundle is a string")
                .to_owned();
            Some((capabilities.len(), vec![name]))
        })
        .collect();
    // Prefer the bundles that grant the fewest capabilities.
    options.sort();
    options.into_iter().map(|(_, names)| names).collect()
}

#[cfg(test)]
mod tests {
    struct Query;

    #[async_graphql::Object]
    impl Query {
        async fn denied(&self) -> async_graphql::Result<Option<bool>> {
            Err(async_graphql::Error::new_with_source(
                super::AuthorizationDenied {
                    capability: models::authz::Capability::EditBilling,
                    message: "billing access denied".to_owned(),
                },
            ))
        }

        async fn hidden(&self) -> async_graphql::Result<Option<bool>> {
            Err(super::super::service_accounts::hide_denial_as_not_found(
                async_graphql::Error::new_with_source(super::AuthorizationDenied {
                    capability: models::authz::Capability::CreateApiKey,
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
                "message": "billing access denied. Requires capabilities: billing or admin",
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
