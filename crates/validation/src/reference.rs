use super::Error;
use models::tables;
use superslice::Ext;
use url::Url;

pub fn walk_reference<'a, T, F, N>(
    this_scope: &Url,
    this_thing: &str,
    ref_entity: &'static str,
    ref_name: &N,
    entities: &'a [T],
    entity_fn: F,
    imports: &'a [&'a tables::Import],
    errors: &mut tables::Errors,
) -> Option<&'a T>
where
    F: Fn(&'a T) -> (&'a N, &'a Url),
    N: std::ops::Deref<Target = str> + Eq + 'static,
{
    if let Some(entity) = entities.iter().find(|t| entity_fn(t).0 == ref_name) {
        let ref_scope = entity_fn(entity).1;
        if !import_path(imports, this_scope, ref_scope) {
            Error::MissingImport {
                this_thing: this_thing.to_string(),
                ref_entity,
                ref_name: ref_name.to_string(),
                ref_scope: ref_scope.clone(),
            }
            .push(this_scope, errors);
        }
        return Some(entity);
    }

    let closest = entities
        .iter()
        .filter_map(|t| {
            let (name, scope) = entity_fn(t);
            let dist = strsim::osa_distance(&ref_name, &name);

            if dist <= 4 {
                Some((dist, name.deref(), scope))
            } else {
                None
            }
        })
        .min();

    if let Some((_, suggest_name, suggest_scope)) = closest {
        Error::NoSuchEntitySuggest {
            this_thing: this_thing.to_string(),
            ref_entity,
            ref_name: ref_name.to_string(),
            suggest_name: suggest_name.to_string(),
            suggest_scope: suggest_scope.clone(),
        }
        .push(this_scope, errors);
    } else {
        Error::NoSuchEntity {
            this_thing: this_thing.to_string(),
            ref_entity,
            ref_name: ref_name.to_string(),
        }
        .push(this_scope, errors);
    }

    None
}

pub fn import_path(imports: &[&tables::Import], src_scope: &Url, tgt_scope: &Url) -> bool {
    let edges = |from: &Url| {
        let range = imports.equal_range_by_key(&from, |import| &import.from_resource);
        imports[range].iter().map(|import| &import.to_resource)
    };

    // Trim any fragment suffix of each scope to obtain the base resource.
    let (mut src, mut tgt) = (src_scope.clone(), tgt_scope.clone());
    src.set_fragment(None);
    tgt.set_fragment(None);

    // Search forward paths.
    if let Some(_) = pathfinding::directed::bfs::bfs(&&src, |f| edges(f), |s| s == &&tgt) {
        true
    } else if let Some(_) =
        // Search backward paths.
        pathfinding::directed::bfs::bfs(&&tgt, |f| edges(f), |s| s == &&src)
    {
        true
    } else {
        false
    }
}
