use super::Format;
use crate::Scope;
use std::collections::BTreeMap;

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

// TODO: should into_catalog _only_ use `drafted` specs and ignore `live_spec`?
// Map tables::Sources into a flattened Catalog.
// Sources should already be inline.
pub fn into_catalog(sources: tables::Sources) -> models::Catalog {
    let tables::Sources {
        captures,
        collections,
        fetches: _,
        imports: _,
        materializations,
        resources: _,
        storage_mappings: _,
        tests,
        errors,
    } = sources;

    assert!(errors.is_empty());

    let captures = captures
        .into_iter()
        .filter_map(
            |tables::Capture {
                 capture,
                 drafted,
                 live_spec,
                 ..
             }| drafted.or(live_spec).map(|spec| (capture, spec)),
        )
        .collect();
    let collections = collections
        .into_iter()
        .filter_map(
            |tables::Collection {
                 collection,
                 drafted,
                 live_spec,
                 ..
             }| drafted.or(live_spec).map(|spec| (collection, spec)),
        )
        .collect();
    let materializations = materializations
        .into_iter()
        .filter_map(
            |tables::Materialization {
                 materialization,
                 drafted,
                 live_spec,
                 ..
             }| drafted.or(live_spec).map(|spec| (materialization, spec)),
        )
        .collect();
    let tests = tests
        .into_iter()
        .filter_map(
            |tables::Test {
                 test,
                 drafted,
                 live_spec,
                 ..
             }| drafted.or(live_spec).map(|spec| (test, spec)),
        )
        .collect();
    models::Catalog {
        _schema: None,
        import: Vec::new(), // Fully inline and requires no imports.
        captures,
        collections,
        materializations,
        tests,
        // We deliberately omit storage mappings.
        // The control plane will inject these during its builds.
        storage_mappings: BTreeMap::new(),
    }
}

// Map specifications from a Catalog into tables::Sources.
// Sources should already be inline.
pub fn extend_from_catalog<P>(
    sources: &mut tables::Sources,
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

    const CAPTURES: &str = "captures";
    const COLLECTIONS: &str = "collections";
    const MATERIALIZATIONS: &str = "materializations";
    const TESTS: &str = "tests";

    let root = sources.fetches[0].resource.clone();
    let mut count = 0;

    for (capture, spec) in captures {
        match sources
            .captures
            .binary_search_by(|other| other.capture.cmp(&capture))
        {
            Ok(index) => {
                let chain = eval_policy(
                    &policy,
                    CAPTURES,
                    &capture,
                    &root,
                    Some(&sources.captures[index].scope),
                );

                if let Some(last) = chain.last() {
                    sources.captures[index].scope = last.clone();
                    sources.captures[index].drafted = Some(spec);
                    sources.captures[index].action = Some(tables::Action::Update);
                    add_imports(sources, &chain);
                    count += 1;
                }
            }
            Err(_) => {
                let chain = eval_policy(&policy, CAPTURES, &capture, &root, None);

                if let Some(last) = chain.last() {
                    sources.captures.insert(tables::Capture {
                        id: None,
                        capture: capture.clone(),
                        scope: last.clone(),
                        drafted: Some(spec),
                        live_spec: None,
                        action: Some(tables::Action::Update),
                        expect_pub_id: None,
                        last_pub_id: None,
                    });
                    add_imports(sources, &chain);
                    count += 1;
                }
            }
        }
    }
    for (collection, spec) in collections {
        match sources
            .collections
            .binary_search_by(|other| other.collection.cmp(&collection))
        {
            Ok(index) => {
                let chain = eval_policy(
                    &policy,
                    COLLECTIONS,
                    &collection,
                    &root,
                    Some(&sources.collections[index].scope),
                );

                if let Some(last) = chain.last() {
                    sources.collections[index].scope = last.clone();
                    sources.collections[index].drafted = Some(spec);
                    sources.collections[index].action = Some(tables::Action::Update);
                    add_imports(sources, &chain);
                    count += 1;
                }
            }
            Err(_) => {
                let chain = eval_policy(&policy, COLLECTIONS, &collection, &root, None);

                if let Some(last) = chain.last() {
                    sources.collections.insert(tables::Collection {
                        scope: last.clone(),
                        collection,
                        id: None,
                        action: Some(tables::Action::Update),
                        expect_pub_id: None,
                        drafted: Some(spec),
                        live_spec: None,
                        last_pub_id: None,
                        inferred_schema_md5: None,
                    });
                    add_imports(sources, &chain);
                    count += 1;
                }
            }
        }
    }
    for (materialization, spec) in materializations {
        match sources
            .materializations
            .binary_search_by(|other| other.materialization.cmp(&materialization))
        {
            Ok(index) => {
                let chain = eval_policy(
                    &policy,
                    MATERIALIZATIONS,
                    &materialization,
                    &root,
                    Some(&sources.materializations[index].scope),
                );

                if let Some(last) = chain.last() {
                    sources.materializations[index].scope = last.clone();
                    sources.materializations[index].drafted = Some(spec);
                    sources.materializations[index].action = Some(tables::Action::Update);
                    add_imports(sources, &chain);
                    count += 1;
                }
            }
            Err(_) => {
                let chain = eval_policy(&policy, MATERIALIZATIONS, &materialization, &root, None);

                if let Some(last) = chain.last() {
                    sources.materializations.insert(tables::Materialization {
                        scope: last.clone(),
                        materialization,
                        id: None,
                        action: Some(tables::Action::Update),
                        expect_pub_id: None,
                        drafted: Some(spec),
                        live_spec: None,
                        last_pub_id: None,
                    });
                    add_imports(sources, &chain);
                    count += 1;
                }
            }
        }
    }
    for (test, spec) in tests {
        match sources
            .tests
            .binary_search_by(|other| other.test.cmp(&test))
        {
            Ok(index) => {
                let chain = eval_policy(
                    &policy,
                    TESTS,
                    &test,
                    &root,
                    Some(&sources.tests[index].scope),
                );

                if let Some(last) = chain.last() {
                    sources.tests[index].scope = last.clone();
                    sources.tests[index].drafted = Some(spec);
                    sources.tests[index].action = Some(tables::Action::Update);
                    add_imports(sources, &chain);
                    count += 1;
                }
            }
            Err(_) => {
                let chain = eval_policy(&policy, TESTS, &test, &root, None);

                if let Some(last) = chain.last() {
                    sources.tests.insert(tables::Test {
                        scope: last.clone(),
                        test,
                        id: None,
                        action: Some(tables::Action::Update),
                        expect_pub_id: None,
                        drafted: Some(spec),
                        live_spec: None,
                        last_pub_id: None,
                    });
                    add_imports(sources, &chain);
                    count += 1;
                }
            }
        }
    }
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

fn add_imports(sources: &mut tables::Sources, chain: &[url::Url]) {
    for (importer, imports) in chain.windows(2).map(|pair| (&pair[0], &pair[1])) {
        let mut scope = importer.clone();
        scope.set_fragment(Some("/import/-"));

        let mut imports = imports.clone();
        imports.set_fragment(None);

        // Add a new import if we haven't added one already. We'll do more de-duplication later.
        if let Err(_) = sources
            .imports
            .binary_search_by(|l| (&l.scope, &l.to_resource).cmp(&(&scope, &imports)))
        {
            sources.imports.insert_row(scope, imports);
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
