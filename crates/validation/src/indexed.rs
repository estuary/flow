use crate::{Error, Scope};
use itertools::Itertools;
use models::collate::collate;
use regex::Regex;

pub fn walk_name(
    scope: Scope,
    entity: &'static str,
    name: &str,
    re: &Regex,
    errors: &mut tables::Errors,
) {
    if name.len() == 0 {
        Error::NameEmpty { entity }.push(scope, errors);
    }

    let (start, stop) = re
        .find(name)
        .map(|m| (m.start(), m.end()))
        .unwrap_or((0, 0));
    let unmatched = [&name[..start], &name[stop..]].concat();

    if !unmatched.is_empty() {
        Error::NameRegex {
            entity,
            name: name.to_string(),
            unmatched,
        }
        .push(scope, errors);
    }
}

pub fn walk_duplicates<'a, I>(i: I, errors: &mut tables::Errors)
where
    I: Iterator<Item = (&'static str, &'a str, Scope<'a>)> + 'a,
{
    // Sort entity iterator by increasing, collated name.
    let i = i.sorted_by(|(_, lhs, _), (_, rhs, _)| collate(lhs.chars()).cmp(collate(rhs.chars())));

    // Walk ordered 2-tuples of names & their scopes, looking for exact duplicates.
    for ((lhs_entity, lhs, lhs_scope), (rhs_entity, rhs, rhs_scope)) in i.tuple_windows() {
        // Check if the collated names are exactly equal.
        if collate(lhs.chars()).eq(collate(rhs.chars())) {
            Error::NameCollision {
                error_class: "collides with",
                lhs_entity,
                lhs_name: lhs.to_string(),
                rhs_entity,
                rhs_name: rhs.to_string(),
                rhs_scope: rhs_scope.flatten(),
            }
            .push(lhs_scope, errors);
        }
    }
}
