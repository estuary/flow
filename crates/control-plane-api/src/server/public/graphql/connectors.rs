mod protocol;
mod tags;

use crate::{
    envelope::{Envelope, Locale},
    server::public::graphql::PgDataLoader,
};
use async_graphql::{
    Context,
    connection::{self, Connection, Edge},
    dataloader::DataLoader,
};
use chrono::{DateTime, Utc};
use models::Id;

pub use self::protocol::ConnectorProto;
pub use tags::ConnectorSpec;
use tags::ConnectorSpecKey;

/// Lightweight summary of a connector tag, used internally to drive default-tag
/// resolution. Not exposed in the GraphQL schema.
#[derive(Debug, Clone, sqlx::Type)]
struct ConnectorTagRef {
    /// The primary key of the connector_tags row, as a string instead of an Id
    /// because sqlx currently lacks support for using this custom type when mapping
    /// the `array_agg(...)` column value to a `Vec<ConnectorTagRef>`. So we work
    /// around that by casting the id to text and then use `Id::from_hex` to convert.
    id: String,
    image_tag: String,
    protocol: Option<ConnectorProto>,
    /// Whether both `endpoint_spec_schema` and `resource_spec_schema` are present.
    has_schemas: bool,
}

impl ConnectorTagRef {
    fn spec_succeeded_sync(&self) -> bool {
        self.protocol.is_some() && self.has_schemas
    }
}

/// A connector from the Estuary connector catalog, identified by its OCI image
/// name (e.g. "ghcr.io/estuary/source-postgres"). Use `defaultSpec` to get the
/// configuration schemas for the blessed image tag, or `spec(imageTag)` for a
/// specific version.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct Connector {
    /// Unique id of the connector
    id: Id,
    /// Timestamp of when the connector was first created
    created_at: DateTime<Utc>,
    /// Link to an external site with more information about the endpoint
    external_url: String,
    /// Name of the connector's OCI (Docker) container image, for example "ghcr.io/estuary/source-postgres"
    image_name: String,
    /// Whether this connector should appear in a promoted position in connector listings
    recommended: bool,
    /// The title, a few words at most
    title: Option<String>,
    /// The connector's logo image, represented as a URL per locale
    logo_url: Option<String>,
    /// A string that contains a list of connector details (latency, batch, etc.)
    detail: Option<String>,
    /// Internal: all tags for this connector, used to drive default-tag resolution.
    #[graphql(skip)]
    tags: Vec<ConnectorTagRef>,
}

impl Connector {
    // The canonical tag is derived here as the lexicographically greatest
    // `image_tag` among tags with a fetched spec. Under the `:dev` / `:vN`
    // naming convention this yields `:vN` > `:dev` and higher `N` wins, which
    // holds for single-digit `N`. Known gotcha: `:v10` sorts before `:v2`, so
    // this will need revisiting if any connector reaches double-digit versions.
    // TODO: once we drop `connector_tags` and switch to dynamically fetching
    // connector specs, we'll likely want to introduce a `default_tag` column
    // to `connectors`, which will end up being what we return here.
    fn default_image_tag_ref(&self) -> Option<&ConnectorTagRef> {
        self.tags
            .iter()
            .filter(|t| t.spec_succeeded_sync())
            .max_by_key(|t| &t.image_tag)
    }

    async fn load_spec(
        &self,
        ctx: &Context<'_>,
        tag_ref: Option<&ConnectorTagRef>,
    ) -> async_graphql::Result<Option<ConnectorSpec>> {
        let Some(tag_ref) = tag_ref else {
            return Ok(None);
        };
        let loader = ctx.data::<DataLoader<PgDataLoader>>()?;
        let key = ConnectorSpecKey(
            Id::from_hex(&tag_ref.id).expect("connector_tags id must be a valid Id"),
        );
        loader.load_one(key).await
    }
}

#[async_graphql::ComplexObject]
impl Connector {
    /// The protocol of this connector (capture or materialization).
    pub async fn protocol(&self) -> Option<ConnectorProto> {
        self.default_image_tag_ref().and_then(|t| t.protocol)
    }

    /// Look up the spec for a specific image tag of this connector.
    pub async fn spec(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "the OCI Image tag, including the leading ':', e.g. ':v1'")]
        image_tag: String,
    ) -> async_graphql::Result<Option<ConnectorSpec>> {
        let extant_tag = self
            .tags
            .iter()
            .find(|t| t.image_tag == image_tag && t.spec_succeeded_sync());

        self.load_spec(ctx, extant_tag).await
    }

    /// The spec for this connector's default (blessed) image tag. This is the
    /// spec that should be used when configuring newly created tasks.
    pub async fn default_spec(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Option<ConnectorSpec>> {
        self.load_spec(ctx, self.default_image_tag_ref()).await
    }

    /// The blessed image tag for newly created tasks using this connector.
    /// Resolved as the lexicographically highest image tag among tags with
    /// a complete spec, e.g. `:v2` wins over `:v1`, `:v1` wins over `:dev`.
    pub async fn default_image_tag(&self) -> Option<String> {
        self.default_image_tag_ref().map(|t| t.image_tag.clone())
    }
}

type PaginatedConnectors = Connection<
    Id,
    Connector,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[derive(Debug, Default)]
pub struct ConnectorsQuery;

/// Filter connectors by their protocol (capture or materialization).
#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct ProtocolFilter {
    /// Match connectors that have at least one version with this protocol.
    eq: ConnectorProto,
}

/// Filters for the paginated `connectors` query.
#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct ConnectorsFilter {
    /// Filter by connector protocol. Only connectors with at least one version matching this protocol will be returned.
    protocol: Option<ProtocolFilter>,
    /// Filter by whether the connector is recommended.
    recommended: Option<bool>,
}

impl ConnectorsFilter {
    fn protocol_eq(&self) -> Option<ConnectorProto> {
        self.protocol.as_ref().map(|p| p.eq)
    }
}

const DEFAULT_PAGE_SIZE: usize = 20;

#[async_graphql::Object]
impl ConnectorsQuery {
    /// Resolve the spec for a full OCI image name (e.g.
    /// "ghcr.io/estuary/source-postgres:v1"). If the requested tag is not
    /// available, falls back to the default tag. Check the returned `imageTag`
    /// field to see which tag was actually resolved.
    pub async fn connector_spec(
        &self,
        ctx: &Context<'_>,
        #[graphql(
            desc = "the full OCI image name, including the version tag, e.g. 'ghcr.io/estuary/source-foo:v1'"
        )]
        full_image_name: String,
    ) -> async_graphql::Result<Option<ConnectorSpec>> {
        let env = ctx.data::<Envelope>()?;
        let _claims = env.claims()?;

        let (image, tag) = models::split_image_tag(&full_image_name);
        if tag.is_empty() {
            return Err(async_graphql::Error::new(
                "image name must be in the form of 'registry/name:version' or 'registry/name@sha256:hash'",
            ));
        };

        let Some(connector) = self.connector(ctx, Some(image), None).await? else {
            return Ok(None);
        };
        // Try the specific tag first, then fall back to the default.
        if let Some(spec) = connector.spec(ctx, tag).await? {
            return Ok(Some(spec));
        }
        connector.default_spec(ctx).await
    }

    /// Returns information about a single connector. At least one parameter
    /// must be provided. If both are provided, the connector must match both
    /// the image name and id in order to be returned.
    pub async fn connector(
        &self,
        ctx: &Context<'_>,
        #[graphql(
            desc = "the OCI image name, without a version tag, e.g. 'ghcr.io/estuary/source-foo'"
        )]
        image_name: Option<String>,
        #[graphql(
            desc = "the id of the connector, with or without ':' separators, e.g. '1122334455aabbcc'"
        )]
        id: Option<Id>,
    ) -> async_graphql::Result<Option<Connector>> {
        if image_name.is_none() && id.is_none() {
            return Err(async_graphql::Error::new(
                "must provide at least one of 'imageName' or 'id' parameters",
            ));
        }
        // Require an authenticated user, just to avoid getting spammed by
        // randos. There's no authorization checks to perform, though, as our
        // ACLs don't currently cover connectors.
        let env = ctx.data::<Envelope>()?;
        let _claims = env.claims()?;
        let locale: &str = env.locale.as_ref();
        sqlx::query_as!(
            Connector,
            r#"select
                c.id as "id: Id",
                c.created_at,
                c.external_url,
                c.image_name,

                c.recommended,
                c.detail,
                jsonb_extract_path_text(c.title::jsonb, $1) as "title: String",
                jsonb_extract_path_text(c.logo_url::jsonb, $1) as "logo_url: String",

                coalesce(
                  array_agg(
                    (
                      ct.id::text,
                      ct.image_tag,
                      ct.protocol,
                      (ct.endpoint_spec_schema is not null and ct.resource_spec_schema is not null)
                    )
                  ) filter (
                    where ct.image_tag is not null and ct.id is not null
                  ),
                  '{}'
                ) as "tags!: Vec<ConnectorTagRef>"
            from connectors c
            left outer join connector_tags ct on c.id = ct.connector_id
            where ($2::text is null or c.image_name = $2::text)
            and ($3::flowid is null or c.id = $3::flowid)
            group by c.id
            "#,
            locale,
            image_name,
            id as Option<Id>,
        )
        .fetch_optional(&env.pg_pool)
        .await
        .map_err(async_graphql::Error::from)
    }

    /// Returns a paginated list of connectors, optionally filtered by protocol.
    pub async fn connectors(
        &self,
        ctx: &Context<'_>,
        filter: Option<ConnectorsFilter>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> async_graphql::Result<PaginatedConnectors> {
        // Require an authenticated user, just to avoid getting spammed by
        // randos. There's no authorization checks to perform, though, as our
        // ACLs don't currently cover connectors.
        let env = ctx.data::<Envelope>()?;
        let _claims = env.claims()?;
        let locale = env.locale;

        connection::query_with::<models::Id, _, _, _, async_graphql::Error>(
            after,
            before,
            first,
            last,
            |after, before, first, last| async move {
                let limit = first.or(last).unwrap_or(DEFAULT_PAGE_SIZE);
                if limit == 0 {
                    return Ok(PaginatedConnectors::new(false, false));
                }
                let filter = filter.unwrap_or_default();

                let (page, has_next, has_prev) = if before.is_some() || last.is_some() {
                    // Reverse pagination
                    let (rows, has_more) =
                        fetch_connectors_before(locale, &filter, before, limit, &env.pg_pool)
                            .await
                            .map_err(async_graphql::Error::from)?;
                    // A next page is implied if the request had a before cursor
                    let has_next = before.is_some();
                    (rows, has_next, has_more)
                } else {
                    // Forward pagination, is the default if no pagination
                    // parameters were provided.
                    let (rows, has_more) =
                        fetch_connectors_after(locale, &filter, after, limit, &env.pg_pool)
                            .await
                            .map_err(async_graphql::Error::from)?;
                    // A previous page is implied if the request had an after cursor
                    let has_prev = after.is_some();
                    (rows, has_more, has_prev)
                };

                let mut conn = PaginatedConnectors::new(has_prev, has_next);
                conn.edges.extend(
                    page.into_iter()
                        .map(|connector| Edge::new(connector.id, connector)),
                );
                async_graphql::Result::Ok(conn)
            },
        )
        .await
    }
}

async fn fetch_connectors_after(
    locale: Locale,
    filter: &ConnectorsFilter,
    after: Option<Id>,
    limit: usize,
    db: &sqlx::PgPool,
) -> sqlx::Result<(Vec<Connector>, bool)> {
    let mut results = sqlx::query_as!(
        Connector,
        r#"select
          c.id as "id: Id",
          c.created_at,
          c.external_url,
          c.image_name,

          c.recommended,
          c.detail,
          jsonb_extract_path_text(c.title::jsonb, $4) as "title: String",
          jsonb_extract_path_text(c.logo_url::jsonb, $4) as "logo_url: String",

          coalesce(
            array_agg(
              (
                ct.id::text,
                ct.image_tag,
                ct.protocol,
                (ct.endpoint_spec_schema is not null and ct.resource_spec_schema is not null)
              )
            ) filter (
                where ct.image_tag is not null and ct.id is not null
            ),
            '{}'
          ) as "tags!: Vec<ConnectorTagRef>"
        from connectors c
        join connector_tags ct on c.id = ct.connector_id
        where ($1::text is null or exists (
          select 1 from connector_tags ct_filter
          where ct_filter.connector_id = c.id
          and ct_filter.protocol = $1::text
        ))
        and ($5::bool is null or c.recommended = $5::bool)
        and ($2::flowid is null or c.id > $2::flowid)
        group by c.id
        order by c.id asc
        limit $3 + 1
          "#,
        filter.protocol_eq() as Option<ConnectorProto>,
        after as Option<Id>,
        limit as i64,
        locale.as_ref() as &str,
        filter.recommended as Option<bool>,
    )
    .fetch_all(db)
    .await?;
    let has_more = results.len() > limit;

    // The SQL fetches `limit + 1` rows as a probe for the existence of the
    // next page; the extra row is dropped via `truncate` before returning.
    results.truncate(limit);
    Ok((results, has_more))
}

async fn fetch_connectors_before(
    locale: Locale,
    filter: &ConnectorsFilter,
    before: Option<Id>,
    limit: usize,
    db: &sqlx::PgPool,
) -> sqlx::Result<(Vec<Connector>, bool)> {
    let mut results = sqlx::query_as!(
        Connector,
        r#"select
        c.id as "id: Id",
        c.created_at,
        c.external_url,
        c.image_name,

        c.recommended,
        c.detail,
        jsonb_extract_path_text(c.title::jsonb, $4) as "title: String",
        jsonb_extract_path_text(c.logo_url::jsonb, $4) as "logo_url: String",

        coalesce(
          array_agg(
            (
              ct.id::text,
              ct.image_tag,
              ct.protocol,
              (ct.endpoint_spec_schema is not null and ct.resource_spec_schema is not null)
            )
          ) filter (
              where ct.image_tag is not null and ct.id is not null
          ),
          '{}'
        ) as "tags!: Vec<ConnectorTagRef>"
        from connectors c
        join connector_tags ct on c.id = ct.connector_id
        where ($1::text is null or exists (
          select 1 from connector_tags ct_filter
          where ct_filter.connector_id = c.id
          and ct_filter.protocol = $1::text
        ))
        and ($5::bool is null or c.recommended = $5::bool)
        and ($2::flowid is null or c.id < $2::flowid)
        group by c.id
        order by c.id desc
        limit $3 + 1
          "#,
        filter.protocol_eq() as Option<ConnectorProto>,
        before as Option<Id>,
        limit as i64,
        locale.as_ref() as &str,
        filter.recommended as Option<bool>,
    )
    .fetch_all(db)
    .await?;
    let has_more = results.len() > limit;
    results.truncate(limit);
    // Put results back into ascending order by id
    results.reverse();
    Ok((results, has_more))
}

#[cfg(test)]
mod test {

    use crate::test_server;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("connectors"))
    )]
    async fn test_connectors_query(pool: sqlx::PgPool) {
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let access_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    fragment Select on ConnectorConnection {
                      edges {
                        node {
                          id
                          imageName
                          recommended
                          title
                          protocol
                          defaultImageTag
                          devSpec: spec(imageTag: ":dev") {
                            imageTag
                            protocol
                          }
                          vTwoSpec: spec(imageTag: ":v2") {
                            imageTag
                            protocol
                          }
                          defaultSpec {
                            imageTag
                            protocol
                            endpointSpecSchema
                            resourceSpecSchema
                            disableBackfill
                            documentationUrl
                            defaultCaptureInterval
                          }
                          missingSpec: spec(imageTag: ":vMissing") {
                            imageTag
                            protocol
                          }
                        }
                      }
                    }

                    query TestConnectors {
                      captures: connectors(filter: {protocol: {eq: "capture"}}) {
                        ...Select
                      }

                      materializations: connectors(filter: {protocol: {eq: "materialization"}}) {
                        ...Select
                      }

                      all: connectors {
                        ...Select
                      }

                      allWithEmptyFilter: connectors(filter: {}) {
                        ...Select
                      }

                      recommended: connectors(filter: {recommended: true}) {
                        ...Select
                      }

                      notRecommended: connectors(filter: {recommended: false}) {
                        ...Select
                      }
                    }
            "#
                }),
                Some(&access_token),
            )
            .await;

        assert_eq!(
            response["data"]["all"], response["data"]["allWithEmptyFilter"],
            "`filter: null` and `filter: {{}}` must return identical results"
        );

        insta::assert_json_snapshot!(response);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("connectors"))
    )]
    async fn test_single_connector_queries(pool: sqlx::PgPool) {
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let access_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    fragment Select on Connector {
                        id
                        imageName
                        recommended
                        title
                        protocol
                        defaultImageTag
                        defaultSpec {
                            imageTag
                            protocol
                            endpointSpecSchema
                            resourceSpecSchema
                            disableBackfill
                        }
                    }

                    query TestConnector {
                      source: connector(id: "55:55:55:55:00:00:00:04") {
                        ...Select
                      }
                      dest: connector(id: "55:55:55:55:00:00:00:05") {
                        ...Select
                      }

                      noTagsSource: connector(imageName: "source/no-tags-test") {
                        ...Select
                      }
                      noTagsDest: connector(imageName: "materialize/no-tags-test") {
                        ...Select
                      }
                      missing: connector(imageName: "does/not/exist") {
                        ...Select
                      }

                      # Both provided but referring to different rows; must
                      # return null because the AND of filters doesn't match
                      # any single connector.
                      mismatchedPair: connector(
                        imageName: "source/multi-tag-test"
                        id: "55:55:55:55:00:00:00:05"
                      ) {
                        ...Select
                      }
                    }
            "#
                }),
                Some(&access_token),
            )
            .await;

        insta::assert_json_snapshot!(response);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("connectors"))
    )]
    async fn test_single_connector_spec(pool: sqlx::PgPool) {
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let access_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    fragment Select on ConnectorSpec {
                        imageTag
                        protocol
                        endpointSpecSchema
                        resourceSpecSchema
                    }

                    query TestConnectorSpecs {
                      byFullName: connectorSpec(fullImageName: "materialize/multi-tag-test:dev") {
                        ...Select
                      }
                      fallbackToDefault: connectorSpec(fullImageName: "materialize/multi-tag-test:v2") {
                        ...Select
                      }
                      unknownImage: connectorSpec(fullImageName: "does/not/exist:v1") {
                        ...Select
                      }
                    }
            "#
                }),
                Some(&access_token),
            )
            .await;

        insta::assert_json_snapshot!(response);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("connectors"))
    )]
    async fn test_error_cases(pool: sqlx::PgPool) {
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let access_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        // connectorSpec with no tag delimiter should return an error.
        let no_tag: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"{ connectorSpec(fullImageName: "source/test") { imageTag } }"#
                }),
                Some(&access_token),
            )
            .await;

        // connector with no parameters should return an error.
        let no_params: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"{ connector { id } }"#
                }),
                Some(&access_token),
            )
            .await;

        // Unauthenticated request should return an error.
        let unauthed: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"{ connectors { edges { node { id } } } }"#
                }),
                None,
            )
            .await;

        insta::assert_json_snapshot!(serde_json::json!({
            "noTagDelimiter": no_tag,
            "noConnectorParams": no_params,
            "unauthenticated": unauthed,
        }));
    }

    /// Extract the list of edge IDs and the `pageInfo` flags from a
    /// ConnectorConnection response, returning them in a form that's easy
    /// to assert against. Panics if the response does not have the expected
    /// shape, which indicates the test query itself is wrong.
    fn edges_and_page_info(response: &serde_json::Value, alias: &str) -> (Vec<String>, bool, bool) {
        let conn = &response["data"][alias];
        let edges = conn["edges"]
            .as_array()
            .unwrap_or_else(|| panic!("missing edges for {alias} in {response:#?}"))
            .iter()
            .map(|e| e["node"]["id"].as_str().unwrap().to_string())
            .collect();
        let has_next = conn["pageInfo"]["hasNextPage"].as_bool().unwrap();
        let has_prev = conn["pageInfo"]["hasPreviousPage"].as_bool().unwrap();
        (edges, has_next, has_prev)
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("connectors"))
    )]
    async fn test_pagination_and_combined_filter(pool: sqlx::PgPool) {
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let access_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        // Fixture connectors returned by the list query (in ascending id order,
        // those that have at least one connector_tag):
        //   55:55:55:55:00:00:00:00  source/test           (capture,       not recommended)
        //   55:55:55:55:00:00:00:01  materialize/test      (materialize,   not recommended)
        //   55:55:55:55:00:00:00:04  source/multi-tag-test (capture,       recommended)
        //   55:55:55:55:00:00:00:05  materialize/multi-tag (materialize,   not recommended)
        // GraphQL serializes Id as compact hex without colons.
        let id0 = "5555555500000000";
        let id1 = "5555555500000001";
        let id4 = "5555555500000004";
        let id5 = "5555555500000005";

        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    fragment Page on ConnectorConnection {
                      edges { node { id } }
                      pageInfo { hasNextPage hasPreviousPage }
                    }

                    query TestPagination {
                      firstTwo: connectors(first: 2) {
                        ...Page
                      }
                      firstTwoAfterId1: connectors(first: 2, after: "55:55:55:55:00:00:00:01") {
                        ...Page
                      }
                      # Exact page size boundary: requesting a page that
                      # happens to cover every remaining row must report
                      # hasNextPage=false.
                      firstFour: connectors(first: 4) {
                        ...Page
                      }
                      # Full set is 4, so first=5 returns all 4 with no next page.
                      firstFive: connectors(first: 5) {
                        ...Page
                      }
                      lastTwo: connectors(last: 2) {
                        ...Page
                      }
                      lastTwoBeforeId4: connectors(last: 2, before: "55:55:55:55:00:00:00:04") {
                        ...Page
                      }
                      # Combined filter: capture protocol AND recommended = true
                      # only matches source/multi-tag-test (id ...04).
                      captureAndRecommended: connectors(
                        filter: {protocol: {eq: "capture"}, recommended: true}
                      ) {
                        ...Page
                      }
                    }
                    "#
                }),
                Some(&access_token),
            )
            .await;

        assert_eq!(
            response["errors"],
            serde_json::Value::Null,
            "unexpected errors: {:#?}",
            response["errors"]
        );

        let (edges, has_next, has_prev) = edges_and_page_info(&response, "firstTwo");
        assert_eq!(edges, vec![id0.to_string(), id1.to_string()]);
        assert!(has_next, "more pages exist after first 2");
        assert!(!has_prev);

        let (edges, has_next, has_prev) = edges_and_page_info(&response, "firstTwoAfterId1");
        assert_eq!(edges, vec![id4.to_string(), id5.to_string()]);
        assert!(!has_next, "no further page after the last 2");
        assert!(has_prev, "after cursor implies a previous page");

        let (edges, has_next, has_prev) = edges_and_page_info(&response, "firstFour");
        assert_eq!(
            edges,
            vec![
                id0.to_string(),
                id1.to_string(),
                id4.to_string(),
                id5.to_string()
            ]
        );
        assert!(
            !has_next,
            "exact-page-size boundary: hasNextPage must be false when the page \
             happens to contain every remaining row"
        );
        assert!(!has_prev);

        let (edges, has_next, has_prev) = edges_and_page_info(&response, "firstFive");
        assert_eq!(edges.len(), 4, "requesting more than total returns all");
        assert!(!has_next);
        assert!(!has_prev);

        let (edges, has_next, has_prev) = edges_and_page_info(&response, "lastTwo");
        assert_eq!(edges, vec![id4.to_string(), id5.to_string()]);
        assert!(!has_next);
        assert!(
            has_prev,
            "more pages exist before the last 2 in ascending order"
        );

        let (edges, has_next, has_prev) = edges_and_page_info(&response, "lastTwoBeforeId4");
        assert_eq!(edges, vec![id0.to_string(), id1.to_string()]);
        assert!(has_next, "before cursor implies a next page");
        assert!(!has_prev);

        let (edges, _, _) = edges_and_page_info(&response, "captureAndRecommended");
        assert_eq!(
            edges,
            vec![id4.to_string()],
            "combined protocol+recommended filter matches only source/multi-tag-test"
        );
    }
}
