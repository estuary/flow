mod db;

use crate::{CatalogType, Id};
use anyhow::Context;
use models::draft_error::Error;

pub use db::{add_built_spec, delete_errors, delete_specs, touch, upsert_spec};

pub async fn load_draft(
    draft_id: Id,
    db: impl sqlx::PgExecutor<'static>,
) -> anyhow::Result<tables::DraftCatalog> {
    let rows = db::fetch_draft_specs(draft_id.into(), db).await?;
    let mut draft = tables::DraftCatalog::default();

    for row in rows {
        let Some(spec_type) = row.spec_type.map(Into::into) else {
            let scope = tables::synthetic_scope("deletion", &row.catalog_name);
            draft.errors.push(tables::Error {
                scope,
                error: anyhow::anyhow!(
                    "draft contains a deletion of {:?}, but no such live spec exists",
                    row.catalog_name
                ),
            });
            continue;
        };
        let scope = tables::synthetic_scope(spec_type, &row.catalog_name);

        if let Err(err) = draft.add_spec(
            spec_type,
            &row.catalog_name,
            scope,
            row.expect_pub_id.map(Into::into),
            row.spec.as_deref().map(|j| &**j),
            false, // !is_touch
        ) {
            draft.errors.push(err);
        }
    }
    Ok(draft)
}

pub async fn upsert_draft_catalog(
    draft_id: Id,
    catalog: &tables::DraftCatalog,
    txn: &mut sqlx::PgConnection,
) -> anyhow::Result<()> {
    let tables::DraftCatalog {
        captures,
        collections,
        materializations,
        tests,
        ..
    } = catalog;
    for row in collections {
        db::upsert_spec(
            draft_id,
            row.collection.as_str(),
            row.model.as_ref(),
            CatalogType::Collection,
            row.expect_pub_id,
            txn,
        )
        .await?;
    }
    for row in captures {
        db::upsert_spec(
            draft_id,
            row.capture.as_str(),
            row.model.as_ref(),
            CatalogType::Capture,
            row.expect_pub_id,
            txn,
        )
        .await?;
    }
    for row in materializations {
        db::upsert_spec(
            draft_id,
            row.materialization.as_str(),
            row.model.as_ref(),
            CatalogType::Materialization,
            row.expect_pub_id,
            txn,
        )
        .await?;
    }
    for row in tests {
        db::upsert_spec(
            draft_id,
            row.test.as_str(),
            row.model.as_ref(),
            CatalogType::Test,
            row.expect_pub_id,
            txn,
        )
        .await?;
    }

    db::touch(draft_id, txn).await?;
    Ok(())
}

pub async fn insert_errors(
    draft_id: Id,
    errors: Vec<Error>,
    txn: &mut sqlx::PgConnection,
) -> anyhow::Result<()> {
    for err in errors {
        let mut detail = err.detail;
        // Replace null bytes with the Unicode replacement character because
        // postgres chokes on them.
        if detail.contains('\0') {
            detail = detail.replace('\0', "\u{FFFD}")
        }
        db::insert_error(draft_id, err.scope.unwrap_or(err.catalog_name), detail, txn)
            .await
            .context("inserting error")?;
    }
    Ok(())
}
