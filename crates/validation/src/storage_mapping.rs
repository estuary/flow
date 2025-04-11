use super::{indexed, Error, Scope};
use models::{
    Store, AZURE_CONTAINER_RE, AZURE_STORAGE_ACCOUNT_RE, CATALOG_PREFIX_RE, GCS_BUCKET_RE,
    S3_BUCKET_RE, TOKEN_RE,
};
use superslice::Ext;

pub fn walk_all_storage_mappings(
    storage_mappings: &tables::StorageMappings,
    errors: &mut tables::Errors,
) {
    for m in storage_mappings {
        let scope = m.scope();
        let scope = Scope::new(&scope);

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
                if m.catalog_prefix.is_empty() {
                    Error::InvalidCustomStoragePrefix {
                        prefix: m.catalog_prefix.to_string(),
                        disallowed: "empty",
                    }
                    .push(scope, errors);
                } else if m.catalog_prefix.starts_with("default/") {
                    Error::InvalidCustomStoragePrefix {
                        prefix: m.catalog_prefix.to_string(),
                        disallowed: "'default/'",
                    }
                    .push(scope, errors);
                } else if m.catalog_prefix.starts_with("recovery/default/") {
                    Error::InvalidCustomStoragePrefix {
                        prefix: m.catalog_prefix.to_string(),
                        disallowed: "'recovery/default/'",
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

// mapped_stores maps the `entity` identified by `name` to its corresponding
// StorageMapping, which is the StorageMapping having the longest `catalog_prefix`
// which is also a prefix of `name`. If no such StorageMapping exists,
// it returns an empty slice and records an error.
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
            Error::NoStorageMapping {
                this_thing: name.to_string(),
                this_entity: entity,
            }
            .push(scope, errors);

            &[]
        }
        None => {
            let (_, suggest_name, suggest_scope) = storage_mappings
                .iter()
                .map(|m| {
                    (
                        strsim::osa_distance(&name, &m.catalog_prefix),
                        &m.catalog_prefix,
                        m.scope(),
                    )
                })
                .min()
                .unwrap();

            Error::NoStorageMappingSuggest {
                this_thing: name.to_string(),
                this_entity: entity,
                suggest_name: suggest_name.to_string(),
                suggest_scope: suggest_scope,
            }
            .push(scope, errors);

            &[]
        }
    }
}

// lookup_mapping returns the StorageMapping having the longest `catalog_prefix`
// which is also a prefix of `name`, or None if no such StorageMapping exists.
fn lookup_mapping<'a>(
    storage_mappings: &'a [tables::StorageMapping],
    name: &str,
) -> Option<&'a tables::StorageMapping> {
    let index = storage_mappings.upper_bound_by_key(&name, |m| &m.catalog_prefix);

    for mapping in storage_mappings[0..index].iter().rev() {
        if name.starts_with(mapping.catalog_prefix.as_str()) {
            return Some(mapping);
        }
    }
    None
}

#[cfg(test)]
mod test {
    use super::lookup_mapping;
    use models::Prefix;

    #[test]
    fn test_matched_mapping() {
        let mut mappings = tables::StorageMappings::new();

        mappings.insert_row(Prefix::new("foo/"), models::Id::new([1; 8]), Vec::new());
        mappings.insert_row(Prefix::new("bar/"), models::Id::new([2; 8]), Vec::new());
        mappings.insert_row(Prefix::new("bar/one/"), models::Id::new([3; 8]), Vec::new());
        mappings.insert_row(Prefix::new("bar/two/"), models::Id::new([4; 8]), Vec::new());
        mappings.insert_row(Prefix::new("baz/a/"), models::Id::new([5; 8]), Vec::new());
        mappings.insert_row(Prefix::new("baz/b/"), models::Id::new([6; 8]), Vec::new());

        let expect = |name, id: Option<[u8; 8]>| {
            assert_eq!(
                lookup_mapping(&mappings, name).map(|mapping| mapping.control_id.as_array()),
                id,
                "expected {name} to match {id:?}"
            )
        };

        expect("foo/foo", Some([1; 8]));
        expect("fooo/foo", None);
        expect("bar/other", Some([2; 8]));
        expect("bar/one", Some([2; 8]));
        expect("barr/one", None);
        expect("bar/one/1", Some([3; 8]));
        expect("bar/pne/2", Some([2; 8]));
        expect("bar/two/3", Some([4; 8]));
        expect("bar/uwo/3", Some([2; 8]));
        expect("baz/a/3", Some([5; 8]));
        expect("baz/b/4", Some([6; 8]));
        expect("baz/c/5", None);
        expect("baz/a", None);
        expect("baz/other", None);
    }
}
