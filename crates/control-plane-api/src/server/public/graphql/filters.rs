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
    pub starts_with: Option<String>,
}

#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct IdFilter {
    /// Match any id in this set. An empty set matches nothing.
    pub r#in: Option<Vec<models::Id>>,
}
