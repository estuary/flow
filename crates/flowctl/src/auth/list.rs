use crate::graphql::*;
use anyhow::Context;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/auth/list-authorized-prefixes.graphql",
    response_derives = "Serialize,Clone",
    extern_enums("Capability")
)]
struct ListAuthorizedPrefixes;

pub type AuthorizedPrefix = list_authorized_prefixes::SelectAuthorizedPrefix;

pub async fn list_authorized_prefixes(
    ctx: &mut crate::CliContext,
    min_capability: models::Capability,
    limit: usize,
) -> anyhow::Result<Vec<AuthorizedPrefix>> {
    let vars = list_authorized_prefixes::Variables {
        min_capability,
        after: None,
        first: limit as i64,
    };

    let resp = post_graphql::<ListAuthorizedPrefixes>(&ctx.client, vars)
        .await
        .context("fetching authorized catalog prefixes")?;

    let prefixes = resp.prefixes.edges.into_iter().map(|e| e.node).collect();
    Ok(prefixes)
}
