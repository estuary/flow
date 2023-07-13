use super::{indexed, Error, Scope};
use models::{Store, BUCKET_RE, CATALOG_PREFIX_RE, TOKEN_RE};
use superslice::Ext;

pub fn walk_all_storage_mappings(
    storage_mappings: &[tables::StorageMapping],
    errors: &mut tables::Errors,
) {
    for m in storage_mappings {
        let scope = Scope::new(&m.scope);

        for (index, store) in m.stores.iter().enumerate() {
            let scope = scope.push_item(index);

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
                if m.prefix.is_empty() {
                    Error::InvalidCustomStoragePrefix {
                        prefix: m.prefix.to_string(),
                        disallowed: "empty",
                    }
                    .push(scope, errors);
                } else if m.prefix.starts_with("default/") {
                    Error::InvalidCustomStoragePrefix {
                        prefix: m.prefix.to_string(),
                        disallowed: "'default/'",
                    }
                    .push(scope, errors);
                } else if m.prefix.starts_with("recovery/default/") {
                    Error::InvalidCustomStoragePrefix {
                        prefix: m.prefix.to_string(),
                        disallowed: "'recovery/default/'",
                    }
                    .push(scope, errors);
                }
            }

            match store {
                Store::S3(cfg) | Store::Gcs(cfg) => {
                    indexed::walk_name(
                        scope.push_prop("bucket"),
                        "storage mapping bucket",
                        &cfg.bucket,
                        &BUCKET_RE,
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
                    indexed::walk_name(
                        scope.push_prop("bucket"),
                        "custom storage mapping bucket",
                        &cfg.bucket,
                        &BUCKET_RE,
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
                        &BUCKET_RE,
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
                        &TOKEN_RE,
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
            }
        }

        if m.prefix.is_empty() {
            // Prefix is allowed to be empty. Continue because
            // walk_name will otherwise produce an error.
            continue;
        }
        indexed::walk_name(
            scope,
            "storageMappings",
            m.prefix.as_ref(),
            models::Prefix::regex(),
            errors,
        );
    }

    indexed::walk_duplicates(
        storage_mappings.iter().map(|m| {
            (
                "storageMapping",
                // Prefixes explicitly end in a '/'. Strip it for the sake of
                // walking duplicates, which (currently) expects non-prefix names.
                m.prefix
                    .as_str()
                    .strip_suffix("/")
                    .unwrap_or(m.prefix.as_str()),
                Scope::new(&m.scope),
            )
        }),
        errors,
    );
}

// mapped_stores maps the |entity| identified by |name| to its corresponding
// StorageMapping stores. Or, if no StorageMapping is matched, it returns an
// empty slice and records an error.
pub fn mapped_stores<'a>(
    scope: Scope<'a>,
    entity: &'static str,
    name: &str,
    storage_mappings: &'a [tables::StorageMapping],
    errors: &mut tables::Errors,
) -> &'a [models::Store] {
    match lookup_mapping(storage_mappings, name) {
        Some(m) => &m.stores,
        None if storage_mappings.is_empty() => {
            // We produce a single, top-level error if no mappings are defined.
            &[]
        }
        None => {
            let (_, suggest_name, suggest_scope) = storage_mappings
                .iter()
                .map(|m| (strsim::osa_distance(&name, &m.prefix), &m.prefix, &m.scope))
                .min()
                .unwrap();

            Error::NoStorageMappingSuggest {
                this_thing: name.to_string(),
                this_entity: entity,
                suggest_name: suggest_name.to_string(),
                suggest_scope: suggest_scope.clone(),
            }
            .push(scope, errors);

            &[]
        }
    }
}

// lookup_mapping returns a StorageMapping which has a prefix of |name|,
// or None if no such StorageMapping exists.
fn lookup_mapping<'a>(
    storage_mappings: &'a [tables::StorageMapping],
    name: &str,
) -> Option<&'a tables::StorageMapping> {
    let index = storage_mappings.upper_bound_by_key(&name, |m| &m.prefix);

    index
        // We've located the first entry *greater* than name.
        // Step backwards one to the last entry less-then or equal to name.
        .checked_sub(1)
        .and_then(|i| storage_mappings.get(i))
        // Then test if it's indeed a prefix of |name|. It may not be.
        .filter(|m| name.starts_with(m.prefix.as_str()))
}

#[cfg(test)]
mod test {
    use super::lookup_mapping;
    use models::Prefix;

    #[test]
    fn test_matched_mapping() {
        let mut mappings = tables::StorageMappings::new();
        let scope = url::Url::parse("http://scope").unwrap();

        mappings.insert_row(&scope, Prefix::new("foo/"), Vec::new());
        mappings.insert_row(&scope, Prefix::new("bar/one/"), Vec::new());
        mappings.insert_row(&scope, Prefix::new("bar/two/"), Vec::new());

        assert!(lookup_mapping(&mappings, "foo/foo").is_some());
        assert!(lookup_mapping(&mappings, "fooo/foo").is_none());
        assert!(lookup_mapping(&mappings, "bar/one").is_none());
        assert!(lookup_mapping(&mappings, "bar/one/1").is_some());
        assert!(lookup_mapping(&mappings, "bar/pne/2").is_none());
        assert!(lookup_mapping(&mappings, "bar/two/3").is_some());
        assert!(lookup_mapping(&mappings, "bar/uwo/4").is_none());
    }
}
