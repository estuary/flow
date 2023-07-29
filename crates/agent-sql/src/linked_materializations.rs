use crate::{CatalogType, Id, TextJson};
use proto_flow::flow::CaptureSpec;
use serde_json::value::RawValue;

pub struct Row {
    pub materialization_name: String,
    pub materialization_spec: TextJson<Box<RawValue>>,
    pub last_pub_id: Id,
}

/// Returns all the materialization specs that have a `sourceCapture` matching one of the provided `capture_names`.
/// Note that this function may return materializations that are no longer authorized to read from the `sourceCapture`,
/// in the case where the grant was subsequently revoked. If that happens, the intended failure mode is for the
/// publication of the materialization to fail.
pub async fn get_linked_materializations(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    capture_names: Vec<String>,
) -> sqlx::Result<Vec<Row>> {
    sqlx::query_as!(
        Row,
        r#"
        select
          catalog_name as materialization_name,
          spec as "materialization_spec!: TextJson<Box<RawValue>>",
          last_pub_id as "last_pub_id!: Id"
        from live_specs
        where spec_type = 'materialization' and spec->>'sourceCapture' = ANY ($1::catalog_name[])
        "#,
        capture_names as Vec<String>
    )
    .fetch_all(txn)
    .await
}

pub struct CaptureSpecRow {
    pub spec: Option<TextJson<CaptureSpec>>,
}

pub async fn get_source_capture_specs(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source_captures: &[&str],
) -> sqlx::Result<Vec<CaptureSpecRow>> {
    sqlx::query_as!(
        CaptureSpecRow,
        r#"select built_spec as "spec: TextJson<CaptureSpec>" from live_specs where catalog_name = any($1::text[])"#,
        source_captures as &[&str]
    )
    .fetch_all(txn)
    .await
}

pub struct InvalidSourceCapture {
    pub materialization_name: String,
    pub source_capture_name: String,
    pub live_type: Option<CatalogType>,
}

#[derive(Debug)]
pub struct ValidateSourceCaptureInput {
    pub materialization_name: String,
    pub source_capture_name: String,
}

pub async fn find_invalid_captures(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    input: Vec<ValidateSourceCaptureInput>,
) -> Result<Vec<InvalidSourceCapture>, sqlx::Error> {
    let (source_captures, materializations): (Vec<String>, Vec<String>) = input
        .into_iter()
        .map(
            |ValidateSourceCaptureInput {
                 materialization_name,
                 source_capture_name,
             }| (source_capture_name, materialization_name),
        )
        .unzip();

    sqlx::query_as!(
        InvalidSourceCapture,
        r#"
        select
            e.materialization_name as "materialization_name!: String",
            e.source_capture_name as "source_capture_name!: String",
            ls.spec_type as "live_type: CatalogType"
        from unnest($1::text[], $2::text[]) AS e(source_capture_name, materialization_name)
        left join live_specs ls on e.source_capture_name = ls.catalog_name
        where ls.spec_type is null or ls.spec_type != 'capture'
        "#,
        source_captures as Vec<String>,
        materializations as Vec<String>,
    )
    .fetch_all(txn)
    .await
}
