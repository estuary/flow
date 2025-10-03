use async_graphql::{ComplexObject, Context, SimpleObject, dataloader};
use chrono::{DateTime, Utc};
use models::{CatalogType, Id};
use std::{collections::HashMap, sync::Arc};

use crate::server::{
    App, ControlClaims,
    public::graphql::live_spec_refs::{
        LiveSpecRef, PaginatedLiveSpecsRefs, paginate_live_specs_refs,
    },
};

#[derive(Debug, Clone, SimpleObject)]
#[graphql(complex)]
pub struct LiveSpec {
    pub live_spec_id: Id,
    pub catalog_name: String,
    pub catalog_type: models::CatalogType,
    pub model: async_graphql::Json<async_graphql::Value>,
    pub last_build_id: Id,
    pub last_pub_id: Id,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub built_spec: async_graphql::Json<async_graphql::Value>,
    pub is_disabled: bool,

    // These "connection" fields are represented here as plain strings of the
    // catalog names, which the resolver functions will expose as paginated
    // lists of `LiveSpecRef`s. This gives us an opportunity to check user
    // permissions for any live specs that are connected to this one.
    #[graphql(skip)]
    reads_from: Vec<String>,
    #[graphql(skip)]
    writes_to: Vec<String>,
    #[graphql(skip)]
    source_capture: Option<String>,
    #[graphql(skip)]
    written_by: Vec<String>,
    #[graphql(skip)]
    read_by: Vec<String>,
}

#[ComplexObject]
impl LiveSpec {
    async fn reads_from(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> async_graphql::Result<Option<PaginatedLiveSpecsRefs>> {
        if self.reads_from.is_empty() {
            return Ok(None);
        }
        let conn = paginate_live_specs_refs(
            ctx,
            None,
            self.reads_from.clone(),
            after,
            before,
            first,
            last,
        )
        .await?;
        Ok(Some(conn))
    }

    async fn writes_to(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> async_graphql::Result<Option<PaginatedLiveSpecsRefs>> {
        if self.writes_to.is_empty() {
            return Ok(None);
        }
        let conn = paginate_live_specs_refs(
            ctx,
            None,
            self.writes_to.clone(),
            after,
            before,
            first,
            last,
        )
        .await?;
        Ok(Some(conn))
    }

    async fn source_capture(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Option<LiveSpecRef>> {
        let app = ctx.data::<Arc<App>>()?;
        let claims = ctx.data::<ControlClaims>()?;

        let Some(source_capture_name) = &self.source_capture else {
            return Ok(None);
        };
        let attached = app.attach_user_capabilities(
            claims,
            vec![source_capture_name.clone()],
            |name, user_capability| {
                Some(LiveSpecRef {
                    catalog_name: models::Name::new(name),
                    user_capability,
                })
            },
        );
        Ok(attached.into_iter().next())
    }

    // Note that we must filter the `writtenBy` and `readBy` names before
    // paginating because the user may not have permission to see all the things
    // writing to the spec. This is different from `readsFrom` and `writesTo`,
    // because those are represented in the model itself, so having capability
    // to read the model will implicitly include the capability to know the
    // names of all the things that model reads from or writes to. But
    // `writtenBy` and `readBy` are determined by the existence of _other_
    // specs, and we must not leak their names to users who are not authorized
    // to see them.

    /// Returns a list of live specs that write to this spec. This will always
    /// be empty if this spec is a not a collection.
    async fn written_by(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> async_graphql::Result<Option<PaginatedLiveSpecsRefs>> {
        if self.written_by.is_empty() {
            return Ok(None);
        }
        let conn = paginate_live_specs_refs(
            ctx,
            Some(models::Capability::Read),
            self.written_by.clone(),
            after,
            before,
            first,
            last,
        )
        .await?;
        Ok(Some(conn))
    }

    /// Returns a list of live specs that read from this spec. This will always
    /// be empty if this spec is a not a collection.
    async fn read_by(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> async_graphql::Result<Option<PaginatedLiveSpecsRefs>> {
        if self.read_by.is_empty() {
            return Ok(None);
        }
        let conn = paginate_live_specs_refs(
            ctx,
            Some(models::Capability::Read),
            self.read_by.clone(),
            after,
            before,
            first,
            last,
        )
        .await?;
        Ok(Some(conn))
    }
}

/// Typed key for loading live specs by catalog_name
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LiveSpecKey {
    pub catalog_name: String,
    /// Whether to include the model in the LiveSpec
    pub with_model: bool,
    /// Whether to include the built_spec in the LiveSpec
    pub with_built: bool,
}

impl dataloader::Loader<LiveSpecKey> for super::PgDataLoader {
    type Value = LiveSpec;
    type Error = async_graphql::Error;

    async fn load(
        &self,
        keys: &[LiveSpecKey],
    ) -> Result<HashMap<LiveSpecKey, Self::Value>, Self::Error> {
        let names = keys
            .iter()
            .map(|k| k.catalog_name.as_str())
            .collect::<Vec<_>>();
        let spec_selected = keys.iter().map(|k| k.with_model).collect::<Vec<_>>();
        let built_selected = keys.iter().map(|k| k.with_built).collect::<Vec<_>>();

        tracing::debug!(count = names.len(), "loading live_specs");
        sqlx::query!(
            r#"select
                ls.catalog_name,
                inputs.with_model as "with_model!: bool",
                inputs.with_built as "with_built!: bool",
                ls.id as "live_spec_id: models::Id",
                ls.spec_type as "spec_type!: models::CatalogType",
                case when inputs.with_model then ls.spec::text else null end as "model: crate::TextJson<async_graphql::Value>",
                ls.last_build_id as "last_build_id: models::Id",
                ls.last_pub_id as "last_pub_id: models::Id",
                ls.created_at,
                ls.updated_at,
                case when inputs.with_built then ls.built_spec::text else null end as "built_spec: crate::TextJson<async_graphql::Value>",
                coalesce(ls.spec->'shards'->>'disable', ls.spec->'derive'->'shards'->>'disable', 'false')::boolean as "is_disabled!: bool",
                ls.reads_from as "reads_from?: Vec<String>",
                ls.writes_to as "writes_to?: Vec<String>",
                case json_typeof(ls.spec->'source')
                when 'object' then ls.spec->'source'->>'capture'
                when 'string' then ls.spec->>'source'
                else null
                end as "source_capture?: String",
                array_agg(distinct in_flows_specs.catalog_name) filter (where ls.spec_type = 'collection' and in_flows.flow_type = 'capture') as "written_by?: Vec<String>",
                array_agg(distinct out_flows_specs.catalog_name) filter (where ls.spec_type = 'collection' and out_flows.flow_type is not null) as "read_by?: Vec<String>"
            from unnest($1::catalog_name[], $2::boolean[], $3::boolean[]) inputs(name, with_model, with_built)
            join live_specs ls on inputs.name = ls.catalog_name
            left outer join live_spec_flows in_flows on in_flows.target_id = ls.id
            left outer join live_spec_flows out_flows on out_flows.source_id = ls.id
            left outer join live_specs in_flows_specs on in_flows_specs.id = in_flows.source_id
            left outer join live_specs out_flows_specs on out_flows_specs.id = out_flows.target_id
            group by ls.id, inputs.with_model, inputs.with_built
            "#,
            &names as &[&str],
            spec_selected.as_slice(),
            built_selected.as_slice(),
        )
        .fetch_all(&self.0)
        .await
        .map_err(|e| async_graphql::Error::from(e))
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    let key = LiveSpecKey {
                        catalog_name: row.catalog_name.clone(),
                        with_model: row.with_model,
                        with_built: row.with_built,
                    };
                    let mut live = LiveSpec {
                        catalog_name: row.catalog_name,
                        live_spec_id: row.live_spec_id,
                        catalog_type: row.spec_type,
                        model: async_graphql::Json(row.model.map(|j| j.0).unwrap_or_default()),
                        last_build_id: row.last_build_id,
                        last_pub_id: row.last_pub_id,
                        created_at: row.created_at,
                        updated_at: row.updated_at,
                        built_spec: async_graphql::Json(row.built_spec.map(|j| j.0).unwrap_or_default()),
                        is_disabled: row.is_disabled,
                        reads_from: row.reads_from.unwrap_or_default(),
                        writes_to: row.writes_to.unwrap_or_default(),
                        source_capture: row.source_capture,
                        written_by: row.written_by.unwrap_or_default(),
                        read_by: row.read_by.unwrap_or_default(),
                    };
                    // These must be in sorted order for pagination to work, because we
                    // use the catalog name as the pagination cursor.
                    live.reads_from.sort();
                    live.writes_to.sort();
                    live.read_by.sort();
                    live.written_by.sort();
                    (key, live)
                })
                .collect()
        })
    }
}
