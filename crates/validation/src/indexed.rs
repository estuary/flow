use super::{collate::collate, Error};
use itertools::{EitherOrBoth, Itertools};
use models::tables;
use regex::Regex;
use url::Url;

const TOKEN: &'static str = r"[\pL\pN\-_.]+";

lazy_static::lazy_static! {
    pub static ref COLLECTION_RE: Regex = Regex::new(&[TOKEN, "(:?/", TOKEN, ")*"].concat()).unwrap();
    pub static ref ENDPOINT_RE: Regex = Regex::new(&[TOKEN, "(:?/", TOKEN, ")*"].concat()).unwrap();
    pub static ref PARTITION_RE: Regex = Regex::new(TOKEN).unwrap();
    pub static ref TRANSFORM_RE: Regex = Regex::new(TOKEN).unwrap();
}

pub fn walk_name(
    scope: &Url,
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

pub fn walk_duplicates<'a, I, N>(entity: &'static str, i: I, errors: &mut tables::Errors)
where
    I: Iterator<Item = (&'a N, &'a Url)> + 'a,
    N: std::ops::Deref<Target = str> + Clone + 'static,
{
    // Sort entity iterator by increasing, collated name.
    let i = i.sorted_by(|(lhs, _), (rhs, _)| collate(lhs.chars()).cmp(collate(rhs.chars())));

    // Walk ordered 2-tuples of names & their scopes,
    // looking for duplicates or prefixes.
    for ((lhs, lhs_scope), (rhs, rhs_scope)) in i.tuple_windows() {
        // This loop is walking zipped characters of each name, and doing two things:
        // 1) Identifying an exact match (iterator drains with no different characters).
        // 2) Identifying hierarchical prefixes:
        //     "foo/bar" is a prefix of "foo/bar/baz"
        //     "foo/bar" is *not* a prefix of "foo/bark".
        let l = collate(lhs.chars());
        let r = collate(rhs.chars());
        let mut it = l.zip_longest(r);

        loop {
            match it.next() {
                Some(EitherOrBoth::Both(l, r)) if l == r => continue,
                Some(EitherOrBoth::Both(_, _)) => {
                    break; // Neither equal nor a prefix.
                }
                Some(EitherOrBoth::Left(_)) => unreachable!("prevented by sorting"),
                Some(EitherOrBoth::Right(r)) if r == '/' => {
                    // LHS finished *just* as we reached a '/',
                    // as in "foo/bar" vs "foo/bar/".
                    Error::Prefix {
                        entity,
                        lhs: lhs.to_string(),
                        rhs: rhs.to_string(),
                        rhs_scope: rhs_scope.clone(),
                    }
                    .push(lhs_scope, errors);
                }
                Some(EitherOrBoth::Right(_)) => {
                    // E.x. "foo/bar" vs "foo/bark". A prefix, but not a hierarchical one.
                    break;
                }
                None => {
                    // Iterator finished with no different characters.
                    Error::Duplicate {
                        entity,
                        lhs: lhs.to_string(),
                        rhs_scope: rhs_scope.clone(),
                    }
                    .push(lhs_scope, errors);
                    break;
                }
            }
        }
    }
}
