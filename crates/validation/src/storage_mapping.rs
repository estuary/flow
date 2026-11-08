use super::{Error, Scope, indexed};
use models::{
    AZURE_CONTAINER_RE, AZURE_STORAGE_ACCOUNT_RE, CATALOG_PREFIX_RE, GCS_BUCKET_RE, S3_BUCKET_RE,
    Store, TOKEN_RE,
};

pub fn walk_all_storage_mappings(
    storage_mappings: &tables::StorageMappings,
    errors: &mut tables::Errors,
) {
    for m in storage_mappings {
        let scope = m.scope();
        let scope = Scope::new(&scope);

        for (index, store) in m.stores.iter().enumerate() {
            walk_store(scope.push_item(index), &m.catalog_prefix, store, errors);
        }
        for (index, store) in m.recovery_stores.iter().enumerate() {
            walk_store(
                scope.push_prop("recoveryStores").push_item(index),
                &m.catalog_prefix,
                store,
                errors,
            );
        }

        if m.catalog_prefix.is_empty() {
            // Prefix is allowed to be empty. Continue because
            // walk_name will otherwise produce an error.
            continue;
        }
        indexed::walk_name(
            scope,
            "storageMapping",
            m.catalog_prefix.as_ref(),
            models::Prefix::regex(),
            errors,
        );
    }

    let scope = url::Url::parse("flow://storageMappings/").unwrap();
    let scope = Scope::new(&scope);

    indexed::walk_duplicates(
        storage_mappings.iter().map(|m| {
            (
                "storageMapping",
                // Prefixes explicitly end in a '/'. Strip it for the sake of
                // walking duplicates, which (currently) expects non-prefix names.
                m.catalog_prefix
                    .as_str()
                    .strip_suffix("/")
                    .unwrap_or(m.catalog_prefix.as_str()),
                scope.push_prop(&m.catalog_prefix),
            )
        }),
        errors,
    );
}

fn walk_store(
    scope: Scope<'_>,
    catalog_prefix: &models::Prefix,
    store: &Store,
    errors: &mut tables::Errors,
) {
    // Disallow specifying custom storage endpoints for the 'default/' prefix and empty prefix.
    // See: https://github.com/estuary/flow/issues/892#issuecomment-1403873100
    if let Store::Custom(cfg) = store {
        let scope = scope.push_prop("custom");

        indexed::walk_name(
            scope.push_prop("endpoint"),
            "custom storage endpoint",
            &cfg.endpoint,
            models::StorageEndpoint::regex(),
            errors,
        );

        let scope = scope.push_prop("prefix");
        if catalog_prefix.is_empty() {
            Error::InvalidCustomStoragePrefix {
                prefix: catalog_prefix.to_string(),
                disallowed: "empty",
            }
            .push(scope, errors);
        } else if catalog_prefix.starts_with("default/") {
            Error::InvalidCustomStoragePrefix {
                prefix: catalog_prefix.to_string(),
                disallowed: "'default/'",
            }
            .push(scope, errors);
        }
    }

    match store {
        Store::S3(cfg) => {
            indexed::walk_name(
                scope.push_prop("bucket"),
                "storage mapping bucket",
                &cfg.bucket,
                &S3_BUCKET_RE,
                errors,
            );
            if let Some(prefix) = &cfg.prefix {
                indexed::walk_name(
                    scope.push_prop("prefix"),
                    "storage mapping prefix",
                    prefix,
                    &CATALOG_PREFIX_RE,
                    errors,
                );
            }
        }
        Store::Gcs(cfg) => {
            indexed::walk_name(
                scope.push_prop("bucket"),
                "storage mapping bucket",
                &cfg.bucket,
                &GCS_BUCKET_RE,
                errors,
            );
            if let Some(prefix) = &cfg.prefix {
                indexed::walk_name(
                    scope.push_prop("prefix"),
                    "storage mapping prefix",
                    prefix,
                    &CATALOG_PREFIX_RE,
                    errors,
                );
            }
        }
        Store::Custom(cfg) => {
            // The GCS bucket naming rules are the most permissive, so we use those for any custom storage providers
            indexed::walk_name(
                scope.push_prop("bucket"),
                "custom storage mapping bucket",
                &cfg.bucket,
                &GCS_BUCKET_RE,
                errors,
            );
            if let Some(prefix) = &cfg.prefix {
                indexed::walk_name(
                    scope.push_prop("prefix"),
                    "custom storage mapping prefix",
                    prefix,
                    &CATALOG_PREFIX_RE,
                    errors,
                )
            }
        }
        Store::Azure(cfg) => {
            indexed::walk_name(
                scope.push_prop("storage_account_name"),
                "azure storage account name",
                &cfg.storage_account_name,
                &AZURE_STORAGE_ACCOUNT_RE,
                errors,
            );
            indexed::walk_name(
                scope.push_prop("account_tenant_id"),
                "azure storage account tenant",
                &cfg.account_tenant_id,
                &TOKEN_RE,
                errors,
            );
            indexed::walk_name(
                scope.push_prop("container_name"),
                "azure storage container name",
                &cfg.container_name,
                &AZURE_CONTAINER_RE,
                errors,
            );

            if let Some(prefix) = &cfg.prefix {
                indexed::walk_name(
                    scope.push_prop("prefix"),
                    "azure storage path prefix",
                    prefix,
                    &CATALOG_PREFIX_RE,
                    errors,
                )
            }
        }
    }
}

/// Returns the StorageMapping covering `name`, or an Error which suggests a
/// near-miss prefix that the user may have intended.
pub fn lookup_mapping<'a>(
    entity: &'static str,
    name: &str,
    storage_mappings: &'a tables::StorageMappings,
) -> Result<&'a tables::StorageMapping, Error> {
    if let Some(mapping) = storage_mappings.lookup(name) {
        return Ok(mapping);
    }

    if let Some((_, suggest_prefix)) = storage_mappings
        .iter()
        .map(|m| {
            (
                strsim::osa_distance(&name, &m.catalog_prefix),
                &m.catalog_prefix,
            )
        })
        .min()
    {
        Err(Error::NoStorageMappingSuggest {
            this_entity: entity,
            this_name: name.to_string(),
            suggest_prefix: suggest_prefix.clone(),
        })
    } else {
        Err(Error::NoStorageMapping {
            this_entity: entity,
            this_name: name.to_string(),
        })
    }
}
