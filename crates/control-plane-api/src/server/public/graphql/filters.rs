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
    /// Match values exactly equal to any entry in this set. The set must hold
    /// between 1 and 100 entries: an empty `in` is rejected during input
    /// validation rather than silently matching nothing (or everything), and
    /// the upper bound keeps this caller-controlled set from driving unbounded
    /// work — every entry is bound into a SQL `= ANY(...)` on each request.
    /// `startsWith` and `in` are mutually exclusive: a resolver rejects a
    /// filter that sets both, so a prefix scope is always either a subtree
    /// (`startsWith`) or an exact set (`in`), never a mix.
    #[graphql(validator(min_items = 1, max_items = 100))]
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
}
