use super::Error;
use models::tables;
use url::Url;

pub fn walk_reference<'a, T, F, N>(
    this_scope: &Url,
    this_thing: &str,
    ref_entity: &'static str,
    ref_name: &N,
    entities: &'a [T],
    entity_fn: F,
    imports: &'a [tables::Import],
    errors: &mut tables::Errors,
) -> Option<&'a T>
where
    F: Fn(&'a T) -> (&'a N, &'a Url),
    N: std::ops::Deref<Target = str> + Eq + 'static,
{
    if let Some(entity) = entities.iter().find(|t| entity_fn(t).0 == ref_name) {
        let ref_scope = entity_fn(entity).1;
        if !tables::Import::path_exists(imports, this_scope, ref_scope) {
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
