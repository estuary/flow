use async_graphql::{Context, Object, Schema, SimpleObject};

use crate::api::{App, ControlClaims};

#[derive(Debug, Clone, PartialEq, SimpleObject)]
#[graphql(concrete(
    name = "LiveCapture",
    params(models::CaptureDef, proto_flow::flow::CaptureSpec)
))]
#[graphql(concrete(
    name = "LiveCollection",
    params(models::CollectionDef, proto_flow::flow::CollectionSpec)
))]
#[graphql(concrete(
    name = "LiveMaterialization",
    params(models::MaterializationDef, proto_flow::flow::MaterializationSpec)
))]
#[graphql(concrete(name = "LiveTest", params(models::TestDef, proto_flow::flow::TestSpec)))]
#[graphql(complex)]
pub struct LiveSpec<T: models::ModelDef, B> {
    pub id: Id,
    pub spec_type: models::CatalogType,
    pub model: Option<T>,
    pub last_build_id: Id,
    pub last_pub_id: Id,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub built_spec: Option<B>,
}

impl LiveSpec<models::CaptureDef, proto_flow::flow::CaptureSpec> {
    async fn controller_status(
        &self,
        ctx: &Context<'_>,
    ) -> Result<models::status::ControllerStatus> {
        todo!()
    }
}

pub async fn fetch_live_specs<M, B>(
    app: Arc<App>,
    verified_claims: &ControlClaims,
    names: &[String],
) -> async_graphql::Result<Vec<LiveSpec<M, B>>> {
    todo!()
}
