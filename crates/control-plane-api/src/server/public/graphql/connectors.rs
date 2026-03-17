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
pub use tags::{ConnectorTag, ConnectorTagId};

#[derive(Debug, Clone, sqlx::Type, async_graphql::SimpleObject)]
#[graphql(complex)]
struct ConnectorTagRef {
    /// The primary key of the connector_tags row, as a string instead of an Id.
    /// because sqlx currently lacks support for using this custom type when mapping
    /// the `array_agg(...)` column value to a `Vec<ConnectorTagRef>`. So we work
    /// around that by casting the id to text and then use the `id` resolver function
    /// to expose it as a typed `Id`.
    #[graphql(skip)]
    id: String,
    /// The OCI image tag, includeing the leading `:`, for example `:v2`
    image_tag: String,
    /// The protocol of this connector tag, if known
    protocol: Option<ConnectorProto>,
    /// Whether the `endpoint_spec_schema` and `resource_spec_schema` values are both present in the database.
    #[graphql(skip)]
    has_schemas: bool,
}

impl ConnectorTagRef {
    /// A synchronous version of `spec_successful` so we can call it in iterator
    /// filters. Needed because `async_graphql` requires that resolver functions
    /// are async.
    fn spec_succeeded_sync(&self) -> bool {
        self.protocol.is_some() && self.has_schemas
    }
}

#[async_graphql::ComplexObject]
impl ConnectorTagRef {
    /// Returns whether a connector Spec RPC has ever been successful for this tag.
    /// Concretely, this is used to determine whether the tag could be used by the
    /// UI or flowctl for publishing tasks, because the Spec RPC populates the
    /// `endpointSpecSchema`, `resourceSpecSchema`, `protocol`, etc.
    pub async fn spec_succeeded(&self) -> bool {
        self.spec_succeeded_sync()
    }

    /// The canonical id of this connector tag
    pub async fn id(&self) -> Id {
        Id::from_hex(&self.id).expect("connector_tags id must be a valid models::Id")
    }
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct Connector {
    /// Unique id of the connector
    id: Id,
    /// Timestamp of when the connector was first created
    created_at: DateTime<Utc>,
    /// Link to an external site with more information about the endpoint
    external_url: String,
    /// Name of the conector's OCI (Docker) Container image, for example "ghcr.io/estuary/source-postgres"
    image_name: String,
    /// Does Estuary's marketing team want this one to appear at the top of the results?
    recommended: bool,
    /// Brief human readable description, at most a few sentences
    short_description: Option<String>,
    /// A longform description of this connector
    long_description: Option<String>,
    /// The title, a few words at most
    title: Option<String>,
    /// The connector's logo image, represented as a URL per locale
    logo_url: Option<String>,
    /// All the tags that are available for this connector.
    tags: Vec<ConnectorTagRef>,
}

impl Connector {
    fn default_image_tag_ref(&self) -> Option<&ConnectorTagRef> {
        self.tags
            .iter()
            .filter(|t| t.spec_succeeded_sync())
            .max_by_key(|t| &t.image_tag)
    }
}

#[async_graphql::ComplexObject]
impl Connector {
    /// Returns the ConnectorTag object for the given image tag, which must begin with a `:`.
    pub async fn connector_tag(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "the OCI Image tag, including the leading ':', e.g. ':v1'")]
        image_tag: Option<String>,
        #[graphql(
            desc = "Whether to return the default connector tag instead when the requested tag is not present or has not had a successful Spec RPC"
        )]
        or_default: bool,
    ) -> async_graphql::Result<Option<ConnectorTag>> {
        let loader = ctx.data::<DataLoader<PgDataLoader>>()?;
        // Any authZ checks have been done already when resolving the Connector.
        // If you can access the connector, then you can access the tags.
        if image_tag.is_none() && !or_default {
            return Err(async_graphql::Error::new(
                "must supply at least one of 'imageTag' or 'orDefault' parameters",
            ));
        }

        let Some(extant_tag) = image_tag
            .and_then(|tag_arg| {
                self.tags
                    .iter()
                    .find(|t| t.image_tag == tag_arg && (t.spec_succeeded_sync() || !or_default))
            })
            .or_else(|| {
                if or_default {
                    self.default_image_tag_ref()
                } else {
                    None
                }
            })
        else {
            return Ok(None);
        };

        let key = ConnectorTagId(
            Id::from_hex(&extant_tag.id).expect("connector_tag id must be a valid Id"),
        );
        loader.load_one(key).await
    }

    /// Returns the default `ConnectorTag` for this connector. This is the one
    /// that should be used by default when publishing new tasks for this
    /// connector. There will only be a default image tag if at least one tag
    /// has successfully completed the connector Spec RPC.
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

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct ProtocolFilter {
    eq: ConnectorProto,
}

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct ConnectorsFilter {
    protocol: ProtocolFilter,
}

const DEFAULT_PAGE_SIZE: usize = 20;

#[async_graphql::Object]
impl ConnectorsQuery {
    /// Returns the ConnectorTag for a given full (including the version) OCI
    /// image name. The returned tag may be different from the version in the
    /// image name. This would happen if there is no connector spec for the
    /// given tag, but one exists for a different tag. The return value will be
    /// null if either the connector image is unkown, or if there has not been a
    /// successful Spec for any version of that image.
    pub async fn connector_tag(
        &self,
        ctx: &Context<'_>,
        #[graphql(
            desc = "the full OCI image name, including the version tag, e.g. 'ghcr.io/estuary/source-foo:v1'"
        )]
        full_image_name: Option<String>,
        #[graphql(
            desc = "the id of the connectorTag, with or without ':' separators, e.g. '1122334455aabbcc'"
        )]
        id: Option<Id>,
    ) -> async_graphql::Result<Option<ConnectorTag>> {
        let env = ctx.data::<Envelope>()?;
        let _claims = env.claims()?;

        if let Some(tag_id) = id {
            let loader = ctx.data::<DataLoader<PgDataLoader>>()?;
            let key = ConnectorTagId(tag_id);
            return loader.load_one(key).await;
        }

        let Some(full_image_name) = full_image_name else {
            return Err(async_graphql::Error::new(
                "must provide at least one of 'fullImageName' or 'id' parameters",
            ));
        };

        let (image, tag) = models::split_image_tag(&full_image_name);
        if tag.is_empty() {
            return Err(async_graphql::Error::new(
                "image name must be in the form of 'registry/name:version' or 'registry/name@sha256:hash'",
            ));
        };

        let Some(connector) = self.connector(ctx, Some(image), None).await? else {
            return Ok(None);
        };
        connector.connector_tag(ctx, Some(tag), true).await
    }

    /// Returns information about a single connector, which may or may not have
    /// had a successful Spec RPC, and thus may or may not be usable in the
    /// Estuary UI. At least one parameter must be provided. If multiple
    /// parameters are provided, then the connector must match _both_ the image
    /// name and id parameters in order to be returned.
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
                jsonb_extract_path_text(c.short_description::jsonb, $1) as "short_description: String",
                jsonb_extract_path_text(c.long_description::jsonb, $1) as "long_description: String",
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

    /// Returns a paginated list of connectors. This query only returns
    /// connectors that have at least one `ConnectorTag` that has had a
    /// successful Spec RPC. Connectors that have not had at least one
    /// successful Spec RPC cannot be used by the Estuary UI, and so are
    /// excluded here.
    pub async fn connectors(
        &self,
        ctx: &Context<'_>,
        filter: ConnectorsFilter,
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
                    return Ok(PaginatedConnectors::new(first.is_some(), last.is_some()));
                }

                let (page, has_next, has_prev) = if before.is_some() || last.is_some() {
                    // Reverse pagination
                    let rows =
                        fetch_connectors_before(locale, filter, before, limit as i64, &env.pg_pool)
                            .await
                            .map_err(async_graphql::Error::from)?;
                    // A next page is implied if the request had a before cursor
                    let has_next = before.is_some();
                    let has_prev = rows.len() >= limit;
                    (rows, has_next, has_prev)
                } else {
                    // Forward pagination, is the default if no pagination
                    // parameters were provided.
                    let rows =
                        fetch_connectors_after(locale, filter, after, limit as i64, &env.pg_pool)
                            .await
                            .map_err(async_graphql::Error::from)?;
                    let has_next = rows.len() >= limit;
                    // A previous page is implied if the request had an after cursor
                    let has_prev = after.is_some();
                    (rows, has_next, has_prev)
                };

                let mut conn = PaginatedConnectors::new(has_next, has_prev);
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
    filter: ConnectorsFilter,
    after: Option<Id>,
    limit: i64,
    db: &sqlx::PgPool,
) -> sqlx::Result<Vec<Connector>> {
    let results = sqlx::query_as!(
        Connector,
        r#"select
          c.id as "id: Id",
          c.created_at,
          c.external_url,
          c.image_name,

          c.recommended,
          jsonb_extract_path_text(c.short_description::jsonb, $4) as "short_description: String",
          jsonb_extract_path_text(c.long_description::jsonb, $4) as "long_description: String",
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
        where ct.protocol = $1
        and ($2::flowid is null or c.id > $2::flowid)
        group by c.id
        order by c.id asc
        limit $3
          "#,
        filter.protocol.eq as ConnectorProto,
        after as Option<Id>,
        limit,
        locale.as_ref() as &str,
    )
    .fetch_all(db)
    .await?;
    Ok(results)
}

async fn fetch_connectors_before(
    locale: Locale,
    filter: ConnectorsFilter,
    before: Option<Id>,
    limit: i64,
    db: &sqlx::PgPool,
) -> sqlx::Result<Vec<Connector>> {
    let mut results = sqlx::query_as!(
        Connector,
        r#"select
        c.id as "id: Id",
        c.created_at,
        c.external_url,
        c.image_name,

        c.recommended,
        jsonb_extract_path_text(c.short_description::jsonb, $4) as "short_description: String",
        jsonb_extract_path_text(c.long_description::jsonb, $4) as "long_description: String",
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
          ),
          '{}'
        ) as "tags!: Vec<ConnectorTagRef>"
        from connectors c
        join connector_tags ct on c.id = ct.connector_id
        where ct.protocol = $1
        and ($2::flowid is null or c.id < $2::flowid)
        group by c.id
        order by c.id desc
        limit $3
          "#,
        filter.protocol.eq as ConnectorProto,
        before as Option<Id>,
        limit,
        locale.as_ref() as &str,
    )
    .fetch_all(db)
    .await?;
    // Put results back into ascending order by id
    results.reverse();
    Ok(results)
}

#[cfg(test)]
mod test {

    use crate::test_server;
    //use flow_client_next as flow_client;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("connectors"))
    )]
    async fn test_connectors_query(pool: sqlx::PgPool) {
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::new_snapshot(pool.clone(), true).await,
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
                          tags {
                            imageTag
                            protocol
                            specSucceeded
                          }
                          vTwoTagResult: connectorTag(imageTag: ":v2", orDefault: true) {
                            id
                            imageTag
                            protocol
                            endpointSpecSchema
                            resourceSpecSchema
                            disableBackfill
                          }
                          defaultTag: connectorTag(orDefault: true) {
                            id
                            imageTag
                            protocol
                            endpointSpecSchema
                            resourceSpecSchema
                            disableBackfill
                          }
                          missingTag: connectorTag(imageTag: ":vMissing", orDefault: false) {
                            imageTag
                            protocol
                            endpointSpecSchema
                            resourceSpecSchema
                            disableBackfill
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
    async fn test_single_connector_queries(pool: sqlx::PgPool) {
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::new_snapshot(pool.clone(), true).await,
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
                        tags {
                            id
                            imageTag
                            protocol
                            specSucceeded
                        }
                        defaultTag: connectorTag(orDefault: true) {
                            id
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
    async fn test_single_connector_tag(pool: sqlx::PgPool) {
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
                    fragment Select on ConnectorTag {
                        id
                        connectorId
                        imageTag
                        protocol
                        endpointSpecSchema
                        resourceSpecSchema
                    }

                    query TestConnectorTags {
                      byId: connectorTag(id: "66:66:66:66:00:00:00:03") {
                        ...Select
                      }
                      byFullName: connectorTag(fullImageName: "materialize/multi-tag-test:dev") {
                        ...Select
                      }
                      fallbackToDefault: connectorTag(fullImageName: "materialize/multi-tag-test:v2") {
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
}
