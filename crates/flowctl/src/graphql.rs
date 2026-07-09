//! # Graphql + flowctl
//!
//! This module contains some common types for use with graphql queries.
//!
//! We use the `graphql_client` crate for all graphql requests:
//! https://github.com/graphql-rust/graphql-client
//!
//! To use it, you'll need two things:
//! - a graphql query, as a file with a `.graphql` extension, which lives next to the rust module it's used by
//! - A rust struct with `#[derive(graphql_client::GraphqlQuery)]`, and a handful of other `graphql` attributes
//!
//! The derive macro will generate a struct for the results of your query, which helps ensure that your rust types are in sync
//! with both the query and the schema. Here's a basic example:
//!
//! ```ignore
//! use crate::graphql::*;
//!
//! #[derive(graphql_client::GraphqlQuery)]
//! #[graphql(
//!   schema_path = "../flow-client/control-plane-api.graphql",
//!   query_path = "src/path/to/your/query.graphql",
//! )]
//! pub struct MyQuery;
//! ```
//!
//! But most queries will also need a few other attributes, not all of which are well documented.
//! Other important attributes are:
//!
//! - `extern_enums("CatalogType", "AlertType", ...)`: Any enums that are returned by your query will named in
//!   this attribute in order for them to have the proper types in the query resonse.
//! - `response_derives`, `variables_derives`: A string with a comma-separated list of derives for either the response
//!   or variables struct. For example `response_derives = "Debug,Serialize"`
//!
//! The most comprehensive docs I could find are on this struct:
//! https://docs.rs/graphql_client_codegen/0.14.0/graphql_client_codegen/struct.GraphQLClientCodegenOptions.html
//!
//! ### Scalars
//!
//! Any scalar types that are returned by the query will be matched to types in whatever rust module uses the derive.
//! They are just matched by name, so if a query selects a field with a graphql scalar type called `DateTime`, then
//! the derive will expect that your module has a `DateTime` type defined. This module should define types for all
//! the scalars in our graphql schema. That way you can just `use crate::graphql::*`, and your response should have
//! all the correct scalar types defined.

#[allow(unused)]
pub use models::{
    Capability, Capture, CatalogType, Collection, Id, Materialization, Name, Prefix, Test,
};

/// The GraphQL `Capability` enum was renamed to `LegacyCapability`; both names
/// resolve to `models::Capability` for `graphql_client`'s `extern_enums`.
#[allow(unused)]
pub use models::Capability as LegacyCapability;

/// Used for all timestamps throughout the schema
pub type DateTime = chrono::DateTime<chrono::Utc>;

/// Used for types that the schema describes as opaque JSON objects
pub type JSONObject = models::RawValue;

/// Used for types that the schema describes as opaque JSON values
pub type JSON = models::RawValue;

/// Used for user ids
pub type UUID = uuid::Uuid;

pub(crate) const GRAPHQL_PATH: &str = "/api/graphql";

/// Perform a unary POST to the control-plane agent API and deserialize its JSON
/// response. This is the flow-client-next equivalent of the former
/// `flow_client::Client::agent_unary`.
pub async fn agent_unary<Request, Response>(
    rest: &flow_client_next::rest::Client,
    access_token: Option<&str>,
    path: &'static str,
    request: &Request,
) -> anyhow::Result<Response>
where
    Request: serde::Serialize,
    Response: serde::de::DeserializeOwned,
{
    let response = rest.post(path, request, access_token).send().await?;
    let status = response.status();

    if status.is_success() {
        let bytes = response.bytes().await?;
        serde_json::from_slice(&bytes).map_err(|error| {
            let body_prefix = String::from_utf8_lossy(&bytes[..(bytes.len().min(500))]);
            tracing::warn!(?error, %body_prefix, "failed to deserialize response body");
            anyhow::Error::from(error)
        })
    } else {
        let body = response.text().await?;
        anyhow::bail!("POST {path}: {status}: {body}");
    }
}

#[tracing::instrument(level = tracing::Level::DEBUG, err, skip_all)]
pub async fn post_graphql<Q: graphql_client::GraphQLQuery>(
    rest: &flow_client_next::rest::Client,
    access_token: Option<&str>,
    variables: Q::Variables,
) -> anyhow::Result<Q::ResponseData> {
    use itertools::Itertools;

    let body = Q::build_query(variables);
    let resp: graphql_client::Response<Q::ResponseData> =
        agent_unary(rest, access_token, GRAPHQL_PATH, &body).await?;

    if let Some(errors) = resp.errors.filter(|e| !e.is_empty()) {
        tracing::warn!(?errors, "graphql query response has errors");

        anyhow::bail!("graphql query errors: [{}]", errors.iter().format(", "));
    }
    resp.data
        .ok_or_else(|| anyhow::anyhow!("graphql query returned no data (also no errors)"))
}
