use async_graphql::{Context, types::connection};

use crate::server::{App, ControlClaims, snapshot::Snapshot};
use std::sync::Arc;

/// A prefix to which the user is authorized.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct Prefix {
    /// The prefix to which the user is authorized.
    pub prefix: String,
    /// The capability granted to the user for this prefix.
    pub capability: models::Capability,
}

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct PrefixesBy {
    /// Filter returned prefixes by user capability.
    pub min_capability: models::Capability,
}

pub type PaginatedPrefixes = connection::Connection<
    String,
    Prefix,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[derive(Debug, Default)]
pub struct PrefixesQuery;

#[async_graphql::Object]
impl PrefixesQuery {
    pub async fn prefixes(
        &self,
        ctx: &Context<'_>,
        by: PrefixesBy,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<PaginatedPrefixes> {
        let claims = ctx.data::<ControlClaims>().unwrap();
        let app = ctx.data::<Arc<App>>()?;
        connection::query(after, None, first, None, |after, _, first, _| async move {
            let (_, mut all_roles) =
                Snapshot::evaluate(app.snapshot(), chrono::Utc::now(), |snapshot: &Snapshot| {
                    let roles = tables::UserGrant::transitive_roles(
                        &snapshot.role_grants,
                        &snapshot.user_grants,
                        claims.sub,
                    )
                    .filter(|grant| grant.capability >= by.min_capability)
                    .filter(|grant| after.as_deref().is_none_or(|min| grant.object_role > min))
                    .map(|grant| Prefix {
                        prefix: grant.object_role.to_string(),
                        capability: grant.capability,
                    })
                    .collect::<Vec<_>>();
                    Ok((None, roles))
                })
                .expect("evaluation cannot fail");

            all_roles.sort_by(|l, r| {
                l.prefix
                    .cmp(&r.prefix)
                    .then(l.capability.cmp(&r.capability).reverse())
            });
            all_roles.dedup_by(|l, r| l.prefix == r.prefix);

            let take = first.unwrap_or(all_roles.len());
            let has_next = first.is_some_and(|limit| all_roles.len() > limit);

            let edges = all_roles
                .into_iter()
                .take(take)
                .map(|prefix| {
                    let cursor = prefix.prefix.clone();
                    connection::Edge::new(cursor, prefix)
                })
                .collect();

            let mut conn = connection::Connection::new(false, has_next);
            conn.edges = edges;
            async_graphql::Result::<PaginatedPrefixes>::Ok(conn)
        })
        .await
    }
}
