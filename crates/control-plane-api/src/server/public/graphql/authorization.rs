use async_graphql::ErrorExtensions;

// Preserve the snapshot refresh path and ApiError source: callers use the
// source to conceal selected terminal denials as not-found responses.
pub(super) async fn verify_authorization(
    env: &crate::Envelope,
    prefix: &str,
    capability: impl Into<models::authz::CapabilitySet> + std::fmt::Display + Copy,
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
            let terminal_denial = matches!(
                &error,
                crate::ApiError::Status(status) if status.code() == tonic::Code::PermissionDenied
            );
            let mut error = async_graphql::Error::from(error);
            if terminal_denial {
                let options = bundle_options(capability.into());
                let descriptions: Vec<_> =
                    options.iter().map(|option| option.join(" + ")).collect();
                error.message.push_str(&format!(
                    ". Requires bundles: {}",
                    descriptions.join(" or ")
                ));
                error = error.extend_with(|_, extensions| {
                    extensions.set("code", "FORBIDDEN");
                    extensions.set("requiredBundleOptions", options);
                });
            }
            Err(error)
        }
    }
}

fn bundle_options(required: models::authz::CapabilitySet) -> Vec<Vec<String>> {
    use models::authz::CapabilityBundle;
    let bundles = <CapabilityBundle as strum::VariantArray>::VARIANTS;
    let capabilities: Vec<_> = bundles.iter().map(CapabilityBundle::capabilities).collect();
    let coverage = |selection: usize| {
        capabilities.iter().enumerate().fold(
            models::authz::CapabilitySet::empty(),
            |all, (index, bits)| {
                if selection & (1 << index) != 0 {
                    all | *bits
                } else {
                    all
                }
            },
        )
    };
    let mut options = Vec::new();
    // Keep alternative bundles, but never ask for an extra bundle whose removal
    // still satisfies every required capability.
    for selection in 1usize..(1 << bundles.len()) {
        let covered = coverage(selection);
        if !required.is_subset(covered)
            || (0..bundles.len()).any(|index| {
                selection & (1 << index) != 0
                    && required.is_subset(coverage(selection & !(1 << index)))
            })
        {
            continue;
        }
        let names: Vec<String> = bundles
            .iter()
            .enumerate()
            .filter(|(index, _)| selection & (1 << index) != 0)
            .map(|(_, bundle)| {
                serde_json::to_value(bundle)
                    .expect("bundle serializes")
                    .as_str()
                    .expect("bundle is a string")
                    .to_owned()
            })
            .collect();
        options.push((covered.len(), names));
    }
    options.sort();
    options.into_iter().map(|(_, names)| names).collect()
}
