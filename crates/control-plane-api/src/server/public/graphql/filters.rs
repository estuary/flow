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
    /// Match values exactly equal to any entry in this set. An empty set
    /// matches nothing. Where a resolver accepts both, `startsWith` and `in`
    /// intersect (a value must satisfy both); resolvers whose backing query
    /// treats them as alternative modes reject the combination instead — see
    /// `storageMappings`.
    pub r#in: Option<Vec<String>>,
}
