use super::Format;
use crate::Scope;
use std::collections::BTreeMap;
use tables::EitherOrBoth;

/// Policy is a layout policy which maps a catalog name, source root,
/// and possible current resource into a resource where the specification should live,
/// as well as an optional leading import chain.
///
/// For example, given name "acmeCo/anvils/products" and root file:///path/flow.yaml,
/// a returned result might be:
///  1) file:///path/flow.yaml
///  2) file:///path/acmeCo/flow.yaml
///  3) file:///path/acmeCo/anvils/flow.yaml
///
/// In this case the catalog spec 1) would be updated to import 2),
/// 2) would import 3), and 3) would contain the specification.
///
/// Or, if the existing resource were file:///path/other/flow.json, then the policy
/// could simply return [ file:///path/other/flow.json ] to update that specification
/// file in place without adding any new imports.
///
/// Or, if the specification should not be updated at all, then the policy
/// should return an empty array.
///
/// Fn(&str, &url::Url, Option<&url::Url>) -> Vec<url::Url>,

// Canonical layout policy for Flow specifications which prefers local
// versions over remote ones.
pub fn canonical_layout_keep(
    name: &str,
    root: &url::Url,
    exists: Option<&url::Url>,
) -> Vec<url::Url> {
    if exists.is_some() {
        return vec![];
    }
    canonical_layout_replace(name, root, exists)
}

// Canonical layout policy for Flow specifications which prefers remote
// versions over local ones.
pub fn canonical_layout_replace(
    name: &str,
    root: &url::Url,
    exists: Option<&url::Url>,
) -> Vec<url::Url> {
    // If we're replacing an existing spec then don't move it from it's current resource.
    if let Some(exists) = exists {
        return vec![exists.clone()];
    }

    let ext = Format::from_scope(root).extension();
    let mut pivot = 0;
    let mut out = vec![root.clone()];

    while let Some(next) = name[pivot..].find("/") {
        pivot += next + 1;

        out.push(root.join(&format!("{}flow.{ext}", &name[..pivot])).unwrap())
    }
    out
}

// Flat layout policy for Flow specifications which prefers local
// versions over remote ones.
pub fn flat_layout_keep(_name: &str, root: &url::Url, exists: Option<&url::Url>) -> Vec<url::Url> {
    if exists.is_some() {
        return vec![];
    } else {
        return vec![root.clone()];
    }
}

// Flat layout policy for Flow specifications which prefers remote
// versions over local ones.
pub fn flat_layout_replace(
    _name: &str,
    root: &url::Url,
    exists: Option<&url::Url>,
) -> Vec<url::Url> {
    if let Some(exists) = exists {
        vec![exists.clone()]
    } else {
        vec![root.clone()]
    }
}

// Map tables::DraftCatalog into a flattened Catalog.
// The DraftCatalog should already be inline.
pub fn into_catalog(draft: tables::DraftCatalog) -> models::Catalog {
    let tables::DraftCatalog {
        captures,
        collections,
        fetches: _,
        imports: _,
        materializations,
        resources: _,
        storage_mappings: _,
        tests,
        errors,
    } = draft;

    assert!(errors.is_empty());

    models::Catalog {
        _schema: None,
        import: Vec::new(), // Fully inline and requires no imports.
        captures: captures
            .into_iter()
            .filter_map(
                |tables::DraftCapture {
                     catalog_name, spec, ..
                 }| spec.map(|spec| (catalog_name, spec)),
            )
            .collect(),
        collections: collections
            .into_iter()
            .filter_map(
                |tables::DraftCollection {
                     catalog_name, spec, ..
                 }| spec.map(|spec| (catalog_name, spec)),
            )
            .collect(),
        materializations: materializations
            .into_iter()
            .filter_map(
                |tables::DraftMaterialization {
                     catalog_name, spec, ..
                 }| spec.map(|spec| (catalog_name, spec)),
            )
            .collect(),
        tests: tests
            .into_iter()
            .filter_map(
                |tables::DraftTest {
                     catalog_name, spec, ..
                 }| spec.map(|spec| (catalog_name, spec)),
            )
            .collect(),

        // We deliberately omit storage mappings.
        // The control plane will inject these during its builds.
        storage_mappings: BTreeMap::new(),
    }
}

// Map specifications from a Catalog into tables::DraftCatalog.
// The DraftCatalog should already be inline.
pub fn extend_from_catalog<P>(
    draft: &mut tables::DraftCatalog,
    catalog: models::Catalog,
    policy: P,
) -> usize
where
    P: Fn(&str, &url::Url, Option<&url::Url>) -> Vec<url::Url>,
{
    let models::Catalog {
        _schema: _,
        import,
        captures,
        collections,
        materializations,
        tests,
        storage_mappings,
    } = catalog;

    assert!(
        import.is_empty(),
        "catalog must be fully inline and self-contained"
    );
    assert!(
        storage_mappings.is_empty(),
        "catalog must not include storage mappings"
    );

    let mut count = 0;

    fn inner<R, P>(
        tbl: tables::Table<R>,
        it: impl Iterator<Item = (R::Key, R::Spec)>,
        policy: P,
        entity: &'static str,
        draft: &mut tables::DraftCatalog,
        count: &mut usize,
    ) -> tables::Table<R>
    where
        R: tables::DraftRow,
        R::Key: AsRef<str>,
        P: Fn(&str, &url::Url, Option<&url::Url>) -> Vec<url::Url>,
    {
        let root = draft.fetches[0].resource.clone();

        tbl.outer_join(it, |eob| match eob {
            EitherOrBoth::Left(row) => Some(row), // Do not modify.
            EitherOrBoth::Both(row, (catalog_name, spec)) => {
                let chain = eval_policy(
                    &policy,
                    entity,
                    catalog_name.as_ref(),
                    &root,
                    Some(row.scope()),
                );

                if let Some(last) = chain.last() {
                    add_imports(draft, &chain);
                    *count += 1;

                    Some(R::new(catalog_name, last.clone(), None, Some(spec)))
                } else {
                    Some(row) // Do not modify.
                }
            }
            EitherOrBoth::Right((catalog_name, spec)) => {
                let chain = eval_policy(&policy, entity, catalog_name.as_ref(), &root, None);
                if let Some(last) = chain.last() {
                    add_imports(draft, &chain);
                    *count += 1;

                    Some(R::new(catalog_name, last.clone(), None, Some(spec)))
                } else {
                    None // Do not insert.
                }
            }
        })
        .collect()
    }

    draft.captures = inner(
        std::mem::take(&mut draft.captures),
        captures.into_iter(),
        &policy,
        "captures",
        draft,
        &mut count,
    );
    draft.collections = inner(
        std::mem::take(&mut draft.collections),
        collections.into_iter(),
        &policy,
        "collections",
        draft,
        &mut count,
    );
    draft.materializations = inner(
        std::mem::take(&mut draft.materializations),
        materializations.into_iter(),
        &policy,
        "materializations",
        draft,
        &mut count,
    );
    draft.tests = inner(
        std::mem::take(&mut draft.tests),
        tests.into_iter(),
        &policy,
        "tests",
        draft,
        &mut count,
    );

    count
}

// Evaluate the policy, and then fix up the fragment-encoded JSON pointer of
// the final URL, which is used as the specification scope within the entity table.
fn eval_policy<P>(
    policy: P,
    entity: &str,
    name: &str,
    root: &url::Url,
    exists: Option<&url::Url>,
) -> Vec<url::Url>
where
    P: Fn(&str, &url::Url, Option<&url::Url>) -> Vec<url::Url>,
{
    let mut chain = policy(name, root, exists);

    for u in chain.iter_mut() {
        u.set_fragment(None);
    }

    if let Some(last) = chain.last_mut() {
        *last = Scope::new(last).push_prop(entity).push_prop(name).flatten();
    }

    chain
}

fn add_imports(draft: &mut tables::DraftCatalog, chain: &[url::Url]) {
    for (importer, imports) in chain.windows(2).map(|pair| (&pair[0], &pair[1])) {
        let mut scope = importer.clone();
        scope.set_fragment(Some("/import/-"));

        let mut imports = imports.clone();
        imports.set_fragment(None);

        // Add a new import if we haven't added one already. We'll do more de-duplication later.
        if let Err(_) = draft
            .imports
            .binary_search_by(|l| (&l.scope, &l.to_resource).cmp(&(&scope, &imports)))
        {
            draft.imports.insert_row(scope, imports);
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::rebuild_catalog_resources;
    use super::*;
    use url::Url;

    #[test]
    fn test_canonical_layout() {
        let out = canonical_layout_replace(
            "acmeCo/anvils/products",
            &Url::parse("file://root/flow.yaml").unwrap(),
            None,
        );
        assert_eq!(
            out,
            vec![
                Url::parse("file://root/flow.yaml").unwrap(),
                Url::parse("file://root/acmeCo/flow.yaml").unwrap(),
                Url::parse("file://root/acmeCo/anvils/flow.yaml").unwrap(),
            ]
        );

        // If the spec exists already, we don't move it.
        let out = canonical_layout_replace(
            "acmeCo/anvils/products",
            &Url::parse("file://root/flow.yaml").unwrap(),
            Some(&Url::parse("file://root/existing/flow.yaml").unwrap()),
        );
        assert_eq!(
            out,
            vec![Url::parse("file://root/existing/flow.yaml").unwrap(),]
        );
    }

    #[test]
    fn test_merging() {
        let target = serde_yaml::from_slice(include_bytes!("merge_test_tgt.yaml")).unwrap();
        let mut target = crate::scenarios::evaluate_fixtures(Default::default(), &target);
        assert!(target.errors.is_empty(), "{:?}", target.errors);

        let source: serde_json::Value =
            serde_yaml::from_slice(include_bytes!("merge_test_src.yaml")).unwrap();
        let source: models::Catalog = serde_json::from_value(source).unwrap();

        let count = extend_from_catalog(&mut target, source, canonical_layout_replace);

        rebuild_catalog_resources(&mut target);
        insta::assert_debug_snapshot!(target);
        assert_eq!(count, 7);
    }
}
