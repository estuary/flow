use models::status::AlertType;

#[derive(Debug, Default)]
pub struct AlertTypesQuery;

/// Describes an alert type with user-facing metadata.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct AlertTypeInfo {
    /// The alert type identifier.
    alert_type: AlertType,
    /// A short, user-facing title for the alert type.
    title: String,
    /// A user-facing description of what this alert type means.
    description: String,
}

#[async_graphql::Object]
impl AlertTypesQuery {
    /// Returns all possible alert types with their user-facing metadata.
    async fn alert_types(&self) -> Vec<AlertTypeInfo> {
        AlertType::all()
            .iter()
            .map(|at| AlertTypeInfo {
                alert_type: *at,
                title: at.title().to_string(),
                description: at.description().to_string(),
            })
            .collect()
    }
}
