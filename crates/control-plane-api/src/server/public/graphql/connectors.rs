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
pub use tags::{ConnectorTag, ConnectorTagKey};

#[derive(Debug, Clone, sqlx::Type, async_graphql::SimpleObject)]
#[graphql(complex)]
struct ConnectorTagRef {
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
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct Connector {
    /// The primary key of the `connectors` table
    #[graphql(skip)]
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
    pub fn default_image_tag_sync(&self) -> Option<String> {
        self.tags
            .iter()
            .filter(|t| t.spec_succeeded_sync())
            .max_by_key(|t| &t.image_tag)
            .map(|t| t.image_tag.clone())
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

        let Some(extant_tag_value) = image_tag
            .filter(|tag_arg| {
                self.tags
                    .iter()
                    .any(|t| &t.image_tag == tag_arg && (t.spec_succeeded_sync() || !or_default))
            })
            .or_else(|| {
                if or_default {
                    self.default_image_tag_sync()
                } else {
                    None
                }
            })
        else {
            return Ok(None);
        };

        let key = ConnectorTagKey {
            connector_id: self.id,
            image_tag: extant_tag_value,
        };
        loader.load_one(key).await
    }

    /// Returns the default `ConnectorTag` for this connector. This is the one
    /// that should be used by default when publishing new tasks for this
    /// connector. There will only be a default image tag if at least one tag
    /// has successfully completed the connector Spec RPC.
    pub async fn default_image_tag(&self) -> Option<String> {
        self.default_image_tag_sync()
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
pub struct ConnectorsFilter {
    protocol: ConnectorProto,
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
        full_image_name: String,
    ) -> async_graphql::Result<Option<ConnectorTag>> {
        let env = ctx.data::<Envelope>()?;
        let _claims = env.claims()?;

        let (image, tag) = models::split_image_tag(&full_image_name);
        if tag.is_empty() {
            return Err(async_graphql::Error::new(
                "image name must be in the form of 'registry/name:version' or 'registry/name@sha256:hash'",
            ));
        };

        let Some(connector) = self.connector(ctx, image).await? else {
            return Ok(None);
        };
        connector.connector_tag(ctx, Some(tag), true).await
    }

    /// Returns information about a single connector, which may or may not have
    /// had a successful Spec RPC, and thus may or may not be usable in the
    /// Estuary UI.
    pub async fn connector(
        &self,
        ctx: &Context<'_>,
        #[graphql(
            desc = "the OCI image name, without a version tag, e.g. 'ghcr.io/estuary/source-foo'"
        )]
        image_name: String,
    ) -> async_graphql::Result<Option<Connector>> {
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
                jsonb_extract_path_text(c.short_description::jsonb, $2) as "short_description: String",
                jsonb_extract_path_text(c.long_description::jsonb, $2) as "long_description: String",
                jsonb_extract_path_text(c.title::jsonb, $2) as "title: String",
                jsonb_extract_path_text(c.logo_url::jsonb, $2) as "logo_url: String",


                coalesce(
                  array_agg(
                    (
                      ct.image_tag,
                      ct.protocol,
                      (ct.endpoint_spec_schema is not null and ct.resource_spec_schema is not null)
                    )
                  ) filter (
                    where ct.image_tag is not null
                  ),
                  '{}'
                ) as "tags!: Vec<ConnectorTagRef>"
            from connectors c
            left outer join connector_tags ct on c.id = ct.connector_id
            where c.image_name = $1
            group by c.id
            "#,
            image_name,
            locale,
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
        by: ConnectorsFilter,
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
                        fetch_connectors_before(locale, by, before, limit as i64, &env.pg_pool)
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
                        fetch_connectors_after(locale, by, after, limit as i64, &env.pg_pool)
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
    by: ConnectorsFilter,
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
        and ($2::flowid is null or c.id > $2::flowid)
        group by c.id
        order by c.id asc
        limit $3
          "#,
        by.protocol as ConnectorProto,
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
    by: ConnectorsFilter,
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
        by.protocol as ConnectorProto,
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
                          imageName
                          recommended
                          title
                          tags {
                            imageTag
                            protocol
                            specSucceeded
                          }
                          vTwoTagResult: connectorTag(imageTag: ":v2", orDefault: true) {
                            imageTag
                            protocol
                            endpointSpecSchema
                            resourceSpecSchema
                            disableBackfill
                          }
                          defaultTag: connectorTag(orDefault: true) {
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
                      captures: connectors(by: {protocol: "capture"}) {
                        ...Select
                      }

                      materializations: connectors(by: {protocol: "materialization"}) {
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
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let access_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    fragment Select on Connector {
                        imageName
                        recommended
                        title
                        tags {
                            imageTag
                            protocol
                            specSucceeded
                        }
                        defaultTag: connectorTag(orDefault: true) {
                            imageTag
                            protocol
                            endpointSpecSchema
                            resourceSpecSchema
                            disableBackfill
                        }
                    }

                    query TestConnector {
                      source: connector(imageName: "source/multi-tag-test") {
                        ...Select
                      }
                      dest: connector(imageName: "materialize/multi-tag-test") {
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
}
