use models::status::AlertType;

#[derive(Debug, Default)]
pub struct AlertTypesQuery;

/// Describes an alert type with user-facing metadata.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct AlertTypeInfo {
    /// The alert type identifier.
    alert_type: AlertType,
    /// A user-facing description of what this alert type means.
    description: String,
    /// A short, user-facing alert type name.
    display_name: String,
    /// An indication of whether the alert type is subscribed to by default.
    is_default: bool,
    /// An indication of whether the alert type is considered to be a system alert.
    is_system: bool,
}

#[async_graphql::Object]
impl AlertTypesQuery {
    /// Returns all possible alert types with their user-facing metadata.
    async fn alert_types(&self) -> Vec<AlertTypeInfo> {
        AlertType::all()
            .iter()
            .map(|at| AlertTypeInfo {
                alert_type: *at,
                description: at.description().to_string(),
                display_name: at.display_name().to_string(),
                is_default: at.is_default(),
                is_system: at.is_system(),
            })
            .collect()
    }
}
