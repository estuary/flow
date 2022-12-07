use super::Error;
use url::Url;

pub fn walk_reference<'a, T, F>(
    this_scope: &Url,
    this_thing: &str,
    ref_entity: &'static str,
    ref_name: &str,
    entities: &'a [T],
    entity_fn: F,
    errors: &mut tables::Errors,
) -> Option<&'a T>
where
    F: Fn(&'a T) -> (&'a str, &'a Url),
{
    if let Some(entity) = entities.iter().find(|t| entity_fn(t).0 == ref_name) {
        return Some(entity);
    }

    let closest = entities
        .iter()
        .filter_map(|t| {
            let (name, scope) = entity_fn(t);
            let dist = strsim::osa_distance(&ref_name, &name);

            if dist <= 4 {
                Some((dist, name, scope))
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
