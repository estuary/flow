use async_graphql::ErrorExtensions;
use models::authz::{Capability, CapabilitySet};
use std::{fmt, sync::Arc};

/// Capabilities that must all be held to satisfy an authorization requirement.
#[derive(Clone, Copy, Debug)]
pub(super) struct RequiredCapabilities(CapabilitySet);

impl RequiredCapabilities {
    pub(super) fn new(capabilities: impl Into<CapabilitySet>) -> Self {
        Self(capabilities.into())
    }

    pub(super) fn capabilities(self) -> CapabilitySet {
        self.0
    }

    fn names(self) -> Vec<String> {
        self.0
            .iter()
            .map(|capability| capability.to_string())
            .collect()
    }
}

impl fmt::Display for RequiredCapabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, capability) in self.0.iter().enumerate() {
            if index != 0 {
                f.write_str(" & ")?;
            }
            capability.fmt(f)?;
        }
        Ok(())
    }
}

impl From<RequiredCapabilities> for CapabilitySet {
    fn from(required: RequiredCapabilities) -> Self {
        required.capabilities()
    }
}

/// The effective authorization scope applied to a scoped GraphQL list.
///
/// The listed prefixes are the same values used to filter the backing query,
/// allowing clients to distinguish "authorized, but empty" from "no scope".
#[derive(Clone, Debug, async_graphql::SimpleObject)]
pub(super) struct AuthorizationScope {
    all_of: Vec<Capability>,
    effective_catalog_prefixes: Vec<models::Prefix>,
}

impl AuthorizationScope {
    pub(super) fn new(
        required: RequiredCapabilities,
        effective_catalog_prefixes: &[String],
    ) -> Self {
        Self {
            all_of: required.capabilities().iter().collect(),
            effective_catalog_prefixes: effective_catalog_prefixes
                .iter()
                .map(models::Prefix::new)
                .collect(),
        }
    }
}

/// A runtime authorization requirement used both to evaluate access and to
/// describe a terminal denial to GraphQL clients.
pub(super) struct AuthorizationRequirement<'a> {
    all_of: RequiredCapabilities,
    catalog_prefixes: Vec<&'a str>,
}

impl<'a> AuthorizationRequirement<'a> {
    pub(super) fn for_catalog_prefix(
        all_of: impl Into<CapabilitySet>,
        catalog_prefix: &'a str,
    ) -> Self {
        Self {
            all_of: RequiredCapabilities::new(all_of),
            catalog_prefixes: vec![catalog_prefix],
        }
    }

    pub(super) async fn verify(self, env: &crate::Envelope) -> async_graphql::Result<()> {
        let policy_result = crate::server::evaluate_names_authorization(
            env.snapshot(),
            env.claims()?,
            self.all_of,
            self.catalog_prefixes.iter().copied(),
        );

        match env.authorization_outcome(policy_result).await {
            Ok((_expiry, ())) => Ok(()),
            Err(api_error) if is_terminal_permission_denial(&api_error) => {
                Err(self.permission_denied_error(api_error))
            }
            Err(api_error) => Err(api_error.into()),
        }
    }

    fn permission_denied_error(self, api_error: crate::ApiError) -> async_graphql::Error {
        let message = match &api_error {
            crate::ApiError::Status(status) => {
                format!("{:?}: {}", status.code(), status.message())
            }
            crate::ApiError::AuthZRetry(_) => unreachable!("a retry is not a terminal denial"),
        };
        let authorization = async_graphql::Value::from_json(serde_json::json!({
            "requirements": [{
                "allOf": self.all_of.names(),
                "catalogPrefixes": self.catalog_prefixes,
            }]
        }))
        .expect("authorization requirements are valid GraphQL values");

        let mut error = async_graphql::Error::new(message).extend_with(|_, extensions| {
            extensions.set("code", "PERMISSION_DENIED");
            extensions.set("authorization", authorization);
        });
        // The handler identifies authorization retries through this source, and
        // other resolvers downcast ApiError to selectively hide denials.
        error.source = Some(Arc::new(api_error));
        error
    }
}

/// Authorization for a resource selected by an opaque identifier.
///
/// The resource's catalog prefix is used for enforcement but is never included
/// in a client-facing denial. Missing and terminally unauthorized resources
/// instead share the same not-found response and disclose only the capability
/// required by the operation.
#[derive(Clone, Copy)]
pub(super) struct OpaqueResourceAuthorization {
    all_of: RequiredCapabilities,
    not_found_message: &'static str,
}

impl OpaqueResourceAuthorization {
    pub(super) fn new(all_of: impl Into<CapabilitySet>, not_found_message: &'static str) -> Self {
        Self {
            all_of: RequiredCapabilities::new(all_of),
            not_found_message,
        }
    }

    pub(super) async fn verify(
        self,
        env: &crate::Envelope,
        catalog_prefix: &str,
    ) -> async_graphql::Result<()> {
        let policy_result = crate::server::evaluate_names_authorization(
            env.snapshot(),
            env.claims()?,
            self.all_of,
            [catalog_prefix],
        );

        match env.authorization_outcome(policy_result).await {
            Ok((_expiry, ())) => Ok(()),
            Err(api_error) if is_terminal_permission_denial(&api_error) => {
                Err(self.not_found_error_with_source(api_error))
            }
            Err(api_error) => Err(api_error.into()),
        }
    }

    pub(super) fn not_found_error(self) -> async_graphql::Error {
        let authorization = async_graphql::Value::from_json(serde_json::json!({
            "requirements": [{
                "allOf": self.all_of.names(),
            }]
        }))
        .expect("authorization requirements are valid GraphQL values");

        async_graphql::Error::new(self.not_found_message).extend_with(|_, extensions| {
            extensions.set("code", "NOT_FOUND");
            extensions.set("authorization", authorization);
        })
    }

    fn not_found_error_with_source(self, api_error: crate::ApiError) -> async_graphql::Error {
        let mut error = self.not_found_error();
        // Retain the terminal ApiError for existing downcast-based handling.
        // GraphQL doesn't serialize the source, so the client-visible error is
        // still identical to the missing-resource case.
        error.source = Some(Arc::new(api_error));
        error
    }
}

fn is_terminal_permission_denial(error: &crate::ApiError) -> bool {
    matches!(
        error,
        crate::ApiError::Status(status) if status.code() == tonic::Code::PermissionDenied
    )
}

/// Intersects already-authorized prefixes with a prefix filter so one exact
/// vector can both filter SQL and describe the effective client scope.
pub(super) fn effective_catalog_prefixes(
    authorized_prefixes: Vec<String>,
    starts_with: Option<&str>,
    exact: Option<&[String]>,
) -> Vec<String> {
    if let Some(exact) = exact {
        let mut effective: Vec<_> = exact
            .iter()
            .filter(|requested| {
                authorized_prefixes
                    .iter()
                    .any(|authorized| requested.starts_with(authorized))
            })
            .cloned()
            .collect();
        effective.sort();
        effective.dedup();
        return effective;
    }

    let Some(starts_with) = starts_with else {
        return authorized_prefixes;
    };

    authorized_prefixes
        .into_iter()
        .map(|prefix| {
            if starts_with.starts_with(&prefix) {
                starts_with.to_string()
            } else {
                prefix
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        AuthorizationRequirement, OpaqueResourceAuthorization, effective_catalog_prefixes,
    };

    #[test]
    fn effective_prefixes_intersect_authorization_with_filter() {
        assert_eq!(
            effective_catalog_prefixes(vec!["acmeCo/".to_string()], Some("acmeCo/widgets/"), None,),
            vec!["acmeCo/widgets/"]
        );
        assert_eq!(
            effective_catalog_prefixes(vec!["acmeCo/widgets/".to_string()], Some("acmeCo/"), None,),
            vec!["acmeCo/widgets/"]
        );
        assert_eq!(
            effective_catalog_prefixes(vec!["acmeCo/".to_string()], None, None),
            vec!["acmeCo/"]
        );
        assert_eq!(
            effective_catalog_prefixes(
                vec!["acmeCo/".to_string()],
                None,
                Some(&["acmeCo/one".to_string(), "other/two".to_string()]),
            ),
            vec!["acmeCo/one"]
        );
        assert_eq!(
            effective_catalog_prefixes(
                vec!["acmeCo/team/".to_string()],
                None,
                Some(&["acmeCo/".to_string()]),
            ),
            Vec::<String>::new()
        );
    }

    #[test]
    fn terminal_denial_has_a_machine_readable_requirement() {
        let error = AuthorizationRequirement::for_catalog_prefix(
            models::authz::Capability::CreateInviteLink,
            "acmeCo/",
        )
        .permission_denied_error(crate::ApiError::Status(
            tonic::Status::permission_denied("not authorized"),
        ));
        assert!(error.source.is_some());
        let extensions = error.extensions.expect("extensions");

        assert_eq!(
            extensions.get("code"),
            Some(&async_graphql::Value::from("PERMISSION_DENIED"))
        );
        assert_eq!(
            extensions.get("authorization"),
            Some(
                &async_graphql::Value::from_json(serde_json::json!({
                    "requirements": [{
                        "allOf": ["CreateInviteLink"],
                        "catalogPrefixes": ["acmeCo/"],
                    }]
                }))
                .unwrap()
            )
        );
    }

    #[test]
    fn opaque_resource_missing_and_denied_errors_are_indistinguishable() {
        let requirement = OpaqueResourceAuthorization::new(
            models::authz::Capability::DeleteInviteLink,
            "invite link not found",
        );
        let missing = requirement.not_found_error();
        let denied = requirement.not_found_error_with_source(crate::ApiError::Status(
            tonic::Status::permission_denied("not authorized for sensitiveCo/secret-prefix/"),
        ));

        // Error equality compares the client-visible message and extensions,
        // while deliberately ignoring the internal source.
        assert_eq!(missing, denied);
        assert!(denied.source.is_some());
        let serialized = serde_json::to_value(denied).unwrap();
        assert_eq!(
            serialized,
            serde_json::json!({
                "message": "invite link not found",
                "extensions": {
                    "code": "NOT_FOUND",
                    "authorization": {
                        "requirements": [{
                            "allOf": ["DeleteInviteLink"],
                        }]
                    }
                }
            })
        );
        assert!(!serialized.to_string().contains("sensitiveCo"));
    }
}
