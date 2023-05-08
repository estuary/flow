use crate::FIXED_DATABASE_URL;
use agent_sql::{evolutions::Row, CatalogType, Id};
use chrono::Utc;
use sqlx::Connection;

#[tokio::test]
#[serial_test::parallel]
async fn test_collection_evolution() {
    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .unwrap();
    let mut txn = conn.begin().await.unwrap();

    sqlx::query(include_str!("test_setup.sql"))
        .execute(&mut txn)
        .await
        .unwrap();

    let input = serde_json::value::to_raw_value(&serde_json::json!([
        {"old_name": "evolution/CollectionA"},
        {"old_name": "evolution/CollectionB", "new_name": "evolution/NewCollectionB"}
    ]))
    .unwrap();
    let evolution_row = Row {
        id: Id::from_hex("f100000000000000").unwrap(),
        created_at: Utc::now(),
        detail: None,
        draft_id: Id::from_hex("2230000000000000").unwrap(),
        logs_token: uuid::Uuid::new_v4(),
        updated_at: Utc::now(),
        user_id: uuid::Uuid::parse_str("43a18a3e-5a59-11ed-9b6a-0242ac188888").unwrap(),
        collections: agent_sql::TextJson(input),
    };

    let result = super::process_row(evolution_row, &mut txn)
        .await
        .expect("process row should succeed");

    insta::assert_yaml_snapshot!(result);

    let new_draft = sqlx::query!(
        r#"
        select catalog_name, spec_type as "spec_type: CatalogType", spec 
        from draft_specs
        where draft_id = '2230000000000000'
        order by catalog_name asc
    "#
    )
    .fetch_all(&mut txn)
    .await
    .expect("querying draft_specs");
    insta::assert_debug_snapshot!(new_draft);
}
