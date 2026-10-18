use async_graphql::ErrorExtensions;

// If cached permissions are stale, request a refresh and retry before denying
// access. Keep the original ApiError attached so callers can report a final
// permission denial as "not found" without revealing that the resource exists.
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
            let terminal_denial = matches!(
                &error,
                crate::ApiError::Status(status) if status.code() == tonic::Code::PermissionDenied
            );
            let mut error = async_graphql::Error::from(error);
            if terminal_denial {
                let options = bundle_options(capability);
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
