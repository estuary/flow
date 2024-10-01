use agent_sql::Id;
use serde_json::value::RawValue;
use sqlx::Connection;

const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

#[tokio::test]
async fn resource_path_pointers_cannot_be_changed() {
    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .expect("connect");

    let mut txn = conn.begin().await.unwrap();

    let row = sqlx::query!(
      r#"
      with setup_connectors as (
        insert into connectors (image_name, external_url, title, short_description, logo_url, recommended)
          values ('foo/image', 'http://test.test', '{"en-US": "foo"}', '{"en-US": "foo"}', '{"en-US": "foo"}', false)
          returning id
      )
      insert into connector_tags (connector_id, image_tag) select id, ':test' as image_tag from setup_connectors
      returning id as "id: Id"
      "#
	).fetch_one(&mut txn).await.unwrap();

    let id = row.id;

    let doc_url = "http://test-docs.test".to_string();
    let endpoint_schema = RawValue::from_string(r#"{"x-test":"endpoint"}"#.to_string()).unwrap();
    let protocol = "capture".to_string();
    let resource_schema = RawValue::from_string(r#"{"x-test":"resource"}"#.to_string()).unwrap();
    let resource_path_pointers = vec!["/ptr_one".to_string(), "/ptr_two".to_string()];

    let result_one = agent_sql::connector_tags::update_tag_fields(
        id,
        doc_url.clone(),
        endpoint_schema.clone(),
        protocol.clone(),
        resource_schema.clone(),
        resource_path_pointers.clone(),
        &mut txn,
    )
    .await
    .unwrap();

    assert!(
        result_one,
        "update_tag_fields should return true when existing resource_path_pointers is null"
    );

    let new_row = sqlx::query!(
        r#"select resource_path_pointers as "resource_path_pointers!: Vec<String>" from connector_tags where id = $1"#,
        id as Id,
    ).fetch_one(&mut txn).await.unwrap();
    assert_eq!(resource_path_pointers, new_row.resource_path_pointers);

    let result_two = agent_sql::connector_tags::update_tag_fields(
        id,
        doc_url,
        endpoint_schema,
        protocol,
        resource_schema,
        vec!["/new_pointer".to_string()],
        &mut txn,
    )
    .await
    .expect("update_tag_fields succeeds");
    assert!(!result_two, "update_tag_fields should return false when existing resource_path_pointers would be changed");

    // assert that the pointers were not changed
    let new_row = sqlx::query!(
        r#"select resource_path_pointers as "resource_path_pointers!: Vec<String>" from connector_tags where id = $1"#,
        id as Id,
    ).fetch_one(&mut txn).await.unwrap();
    assert_eq!(resource_path_pointers, new_row.resource_path_pointers);
}
