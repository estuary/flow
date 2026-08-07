use models::authz::{Capability, CapabilitySet};

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
    use super::effective_catalog_prefixes;

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
}
