use async_graphql::{types::Json, ComplexObject, Context, Object, Schema, SimpleObject};
use chrono::{DateTime, Utc};
use models::Id;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, sync::Arc};

use crate::api::{public::graphql::alerts, App, ControlClaims};

/*
#[graphql(concrete(
     name = "LiveCollection",
     params(models::CollectionDef, proto_flow::flow::CollectionSpec)
 ))]
 #[graphql(concrete(
     name = "LiveMaterialization",
     params(models::MaterializationDef, proto_flow::flow::MaterializationSpec)
 ))]
 #[graphql(concrete(name = "LiveTest", params(models::TestDef, proto_flow::flow::TestSpec)))]

 */
// <ControllerStatus: Send + Sync + async_graphql::OutputType>
//#[graphql(concrete(name = "LiveCapture", params(models::status::capture::CaptureStatus)))]

#[derive(Debug, Clone, SimpleObject)]
#[graphql(complex)]
pub struct LiveSpec {
    pub id: Id,
    pub catalog_name: String,
    pub spec_type: models::CatalogType,
    pub spec: async_graphql::Json<async_graphql::Value>,
    pub last_build_id: Id,
    pub last_pub_id: Id,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub built_spec: async_graphql::Json<async_graphql::Value>,
    //pub controller_status: Option<ControllerStatus>,
}

#[ComplexObject]
impl LiveSpec {
    async fn firing_alerts(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<alerts::Alert>> {
        let loader = ctx.data::<async_graphql::dataloader::DataLoader<alerts::AlertLoader>>()?;
        let alerts = loader.load_one(self.catalog_name.clone()).await?;
        Ok(alerts.unwrap_or_default())
    }
    // async fn controller_status(
    //     &self,
    //     ctx: &Context<'_>,
    // ) -> async_graphql::Result<Option<models::status::capture::CaptureStatus>> {
    //     todo!()
    // }
}

pub async fn fetch_live_specs(
    ctx: &Context<'_>,
    spec_type: models::CatalogType,
    prefixes: Vec<String>,
) -> async_graphql::Result<Vec<LiveSpec>> {
    let app = ctx.data::<Arc<App>>()?;
    let claims = ctx.data::<ControlClaims>()?;

    // Verify user authorization for the catalog names
    let authorized_prefixes = app
        .verify_user_authorization(claims, prefixes, models::Capability::Read)
        .await
        .map_err(|e| async_graphql::Error::new(format!("Authorization failed: {}", e)))?;

    if authorized_prefixes.is_empty() {
        return Ok(vec![]);
    }

    let spec_selected = ctx.look_ahead().field("spec").exists();
    let built_selected = ctx.look_ahead().field("built_spec").exists();

    let rows = sqlx::query!(
        r#"select
                ls.catalog_name,
                ls.id as "id: models::Id",
                ls.spec_type as "spec_type!: models::CatalogType",
                case when $3 then ls.spec::text else null end as "spec: sqlx::types::Json<async_graphql::Value>",
                ls.last_build_id as "last_build_id: models::Id",
                ls.last_pub_id as "last_pub_id: models::Id",
                ls.created_at,
                ls.updated_at,
                case when $4 then ls.built_spec::text else null end as "built_spec: sqlx::types::Json<async_graphql::Value>"
            from unnest($1::text[]) p(prefix)
            join live_specs ls on starts_with(ls.catalog_name::text, p.prefix)
            where ls.spec_type = $2::catalog_spec_type
        "#,
        &authorized_prefixes,
        spec_type as models::CatalogType,
        spec_selected,
        built_selected,
    )
    .fetch_all(&app.pg_pool)
    .await
    .map_err(|e| async_graphql::Error::new(format!("Failed to fetch live specs: {}", e)))?;

    let out = rows
        .into_iter()
        .map(|row| LiveSpec {
            catalog_name: row.catalog_name,
            id: row.id,
            spec_type: row.spec_type,
            spec: async_graphql::Json(row.spec.map(|j| j.0).unwrap_or_default()),
            last_build_id: row.last_build_id,
            last_pub_id: row.last_pub_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
            built_spec: async_graphql::Json(row.built_spec.map(|j| j.0).unwrap_or_default()),
        })
        .collect();

    Ok(out)
}
