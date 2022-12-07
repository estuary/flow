use crate::errors::Error;

use super::{indexed, reference};
use superslice::Ext;

pub fn walk_all_storage_mappings(
    storage_mappings: &[tables::StorageMapping],
    errors: &mut tables::Errors,
) {
    for m in storage_mappings {
        if m.prefix.is_empty() {
            // Prefix is allowed to be empty. Continue because
            // walk_name will otherwise produce an error.
            continue;
        }
        indexed::walk_name(
            &m.scope,
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
                &m.scope,
            )
        }),
        errors,
    );
}

// mapped_stores maps the |entity| identified by |name| to its corresponding
// StorageMapping stores. Or, if no StorageMapping is matched, it returns an
// empty slice and records an error.
pub fn mapped_stores<'a>(
    scope: &url::Url,
    entity: &'static str,
    name: &str,
    storage_mappings: &'a [tables::StorageMapping],
    errors: &mut tables::Errors,
) -> &'a [models::Store] {
    match lookup_mapping(storage_mappings, name) {
        Some(m) => {
            // Ensure that there is an import path.
            reference::walk_reference(
                scope,
                entity,
                "storageMapping",
                &m.prefix,
                storage_mappings,
                |m| (&m.prefix, &m.scope),
                errors,
            );

            &m.stores
        }
        None if storage_mappings.is_empty() => {
            // We produce a single, top-level error if no mappings are defined.
            &EMPTY_STORES
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

            &EMPTY_STORES
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

static EMPTY_STORES: Vec<models::Store> = Vec::new();

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
