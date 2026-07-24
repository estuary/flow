use chrono::NaiveDate;

#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct BoolFilter {
    pub eq: Option<bool>,
}

#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct DateFilter {
    pub gt: Option<NaiveDate>,
    pub lt: Option<NaiveDate>,
}

#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct PrefixFilter {
    /// Match values that start with this prefix — a subtree match, e.g.
    /// `acmeCo/` matches `acmeCo/`, `acmeCo/team/`, and so on.
    pub starts_with: Option<String>,
    /// Match values exactly equal to any entry in this set. When provided the
    /// set must be non-empty; an empty `in` is rejected during input
    /// validation rather than silently matching nothing (or everything).
    /// `startsWith` and `in` are mutually exclusive: a resolver rejects a
    /// filter that sets both, so a prefix scope is always either a subtree
    /// (`startsWith`) or an exact set (`in`), never a mix.
    #[graphql(validator(min_items = 1))]
    pub r#in: Option<Vec<String>>,
}

impl PrefixFilter {
    /// Enforces that `startsWith` and `in` are not both set — they are mutually
    /// exclusive prefix-scoping modes — and returns `(startsWith, in)`. `field`
    /// names the enclosing GraphQL input field for the error message, e.g.
    /// `filter.catalogPrefix`.
    pub fn into_parts(
        self,
        field: &str,
    ) -> async_graphql::Result<(Option<String>, Option<Vec<String>>)> {
        if self.starts_with.is_some() && self.r#in.is_some() {
            return Err(async_graphql::Error::new(format!(
                "`{field}.startsWith` and `.in` are mutually exclusive; provide only one"
            )));
        }
        Ok((self.starts_with, self.r#in))
    }

    /// Narrows a caller's `authorized` prefixes to those that overlap the exact
    /// `in` set, so a `MAX_PREFIXES` guard stays meaningful for callers who can
    /// access many prefixes. The overlap is bidirectional (an `in` entry may be
    /// an ancestor or a descendant of an authorized prefix) and deliberately
    /// approximate: it can only remove entries, never add them, so it cannot
    /// widen visibility. Exact membership is still enforced by the resolver's
    /// SQL (`= ANY($in)`) against these authorized prefixes.
    pub fn narrow_to_exact_set<S: AsRef<str>>(authorized: &mut Vec<String>, exact: &[S]) {
        authorized.retain(|a| {
            exact.iter().any(|e| {
                let e = e.as_ref();
                e.starts_with(a.as_str()) || a.as_str().starts_with(e)
            })
        });
    }
}

#[cfg(test)]
mod test {
    use super::PrefixFilter;

    #[test]
    fn into_parts_passes_through_at_most_one_mode() {
        // Neither mode set.
        let (starts_with, r#in) = PrefixFilter::default()
            .into_parts("filter.catalogPrefix")
            .unwrap();
        assert_eq!(starts_with, None);
        assert_eq!(r#in, None);

        // `startsWith` alone.
        let (starts_with, r#in) = PrefixFilter {
            starts_with: Some("acmeCo/".to_string()),
            r#in: None,
        }
        .into_parts("filter.catalogPrefix")
        .unwrap();
        assert_eq!(starts_with.as_deref(), Some("acmeCo/"));
        assert_eq!(r#in, None);

        // `in` alone.
        let (starts_with, r#in) = PrefixFilter {
            starts_with: None,
            r#in: Some(vec!["acmeCo/".to_string()]),
        }
        .into_parts("filter.catalogPrefix")
        .unwrap();
        assert_eq!(starts_with, None);
        assert_eq!(r#in, Some(vec!["acmeCo/".to_string()]));
    }

    #[test]
    fn into_parts_rejects_both_modes_and_names_the_field() {
        let err = PrefixFilter {
            starts_with: Some("acmeCo/".to_string()),
            r#in: Some(vec!["acmeCo/".to_string()]),
        }
        .into_parts("filter.catalogPrefix")
        .unwrap_err();
        assert_eq!(
            err.message,
            "`filter.catalogPrefix.startsWith` and `.in` are mutually exclusive; provide only one"
        );
    }

    #[test]
    fn narrow_to_exact_set_retains_only_overlapping_prefixes() {
        // Overlap is bidirectional: an exact entry keeps an authorized prefix
        // when it equals, descends from, or is an ancestor of it. Disjoint
        // entries drop it, and an empty exact set drops everything. Retained
        // entries keep their original order.
        let cases: &[(&[&str], &[&str], &[&str])] = &[
            // Equal.
            (&["acmeCo/"], &["acmeCo/"], &["acmeCo/"]),
            // Exact entry descends from the authorized prefix.
            (&["acmeCo/"], &["acmeCo/team/"], &["acmeCo/"]),
            // Exact entry is an ancestor of the authorized prefix.
            (&["acmeCo/team/"], &["acmeCo/"], &["acmeCo/team/"]),
            // Disjoint.
            (&["acmeCo/"], &["betaCo/"], &[]),
            // Mixed: only overlapping authorized prefixes survive.
            (
                &["acmeCo/", "acmeCo/team/", "betaCo/"],
                &["acmeCo/", "ghostCo/"],
                &["acmeCo/", "acmeCo/team/"],
            ),
            // Empty exact set overlaps nothing.
            (&["acmeCo/"], &[], &[]),
        ];
        for &(authorized, exact, expected) in cases {
            let mut got: Vec<String> = authorized.iter().map(|s| s.to_string()).collect();
            PrefixFilter::narrow_to_exact_set(&mut got, exact);
            let want: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
            assert_eq!(got, want, "authorized={authorized:?} exact={exact:?}");
        }
    }
}
