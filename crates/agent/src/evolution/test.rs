use crate::{evolution::JobStatus, FIXED_DATABASE_URL};
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

    let draft_id = Id::from_hex("2230000000000000").unwrap();
    let user_id = uuid::Uuid::parse_str("43a18a3e-5a59-11ed-9b6a-0242ac188888").unwrap();
    sqlx::query(include_str!("test_setup.sql"))
        .execute(&mut txn)
        .await
        .unwrap();

    let input = serde_json::value::to_raw_value(&serde_json::json!([
        {"old_name": "evolution/CollectionA"},
        {"old_name": "evolution/CollectionB", "new_name": "evolution/NewCollectionB"},
        {"old_name": "evolution/CollectionC"},
        {"old_name": "evolution/CollectionD", "new_name": "evolution/NewCollectionD"}
    ]))
    .unwrap();
    let evolution_row = Row {
        id: Id::from_hex("f100000000000000").unwrap(),
        created_at: Utc::now(),
        detail: None,
        draft_id,
        logs_token: uuid::Uuid::new_v4(),
        updated_at: Utc::now(),
        user_id,
        collections: agent_sql::TextJson(input),
        auto_publish: true,
    };

    let result = super::process_row(evolution_row, &mut txn)
        .await
        .expect("process row should succeed");

    let JobStatus::Success {
        evolved_collections,
        publication_id,
    } = result
    else {
        panic!("unexpected job status: {result:?}, expected success");
    };
    let publication_id = publication_id.expect("publication id should be set in status");

    insta::assert_yaml_snapshot!(evolved_collections);

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

    let publication = sqlx::query!(
        r#"select
            draft_id as "draft_id: Id",
            dry_run,
            user_id,
            auto_evolve
        from publications where id = $1;"#,
        publication_id as Id
    )
    .fetch_one(&mut txn)
    .await
    .expect("quering publications");

    assert_eq!(draft_id, publication.draft_id);
    assert_eq!(user_id, publication.user_id);
    assert!(!publication.dry_run);
    assert!(!publication.auto_evolve);
}

#[tokio::test]
#[serial_test::parallel]
async fn evolution_fails_when_collection_is_deleted() {
    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .unwrap();
    let mut txn = conn.begin().await.unwrap();

    let draft_id = Id::from_hex("2230000000000000").unwrap();
    let user_id = uuid::Uuid::parse_str("43a18a3e-5a59-11ed-9b6a-0242ac188888").unwrap();
    sqlx::query(include_str!("test_setup.sql"))
        .execute(&mut txn)
        .await
        .unwrap();

    // Simulate CollectionA being deleted in the draft
    sqlx::query(
        "update draft_specs set spec = null, spec_type = null where id = '1111000000000000'",
    )
    .execute(&mut txn)
    .await
    .unwrap();

    let input = serde_json::value::to_raw_value(&serde_json::json!([
        {"old_name": "evolution/CollectionA"},
    ]))
    .unwrap();
    let evolution_row = Row {
        id: Id::from_hex("f100000000000000").unwrap(),
        created_at: Utc::now(),
        detail: None,
        draft_id,
        logs_token: uuid::Uuid::new_v4(),
        updated_at: Utc::now(),
        user_id,
        collections: agent_sql::TextJson(input),
        auto_publish: false,
    };

    let result = super::process_row(evolution_row, &mut txn)
        .await
        .expect("process row should succeed");

    let JobStatus::EvolutionFailed { error } = result else {
        panic!("unexpected job status: {result:?}, expected failure");
    };

    assert_eq!(
        "cannot evolve collection 'evolution/CollectionA' which was already deleted in the draft",
        error
    );
}

#[tokio::test]
#[serial_test::parallel]
async fn evolution_adds_collections_to_the_draft_if_necessary() {
    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .unwrap();
    let mut txn = conn.begin().await.unwrap();

    let draft_id = Id::from_hex("2230000000000000").unwrap();
    let user_id = uuid::Uuid::parse_str("43a18a3e-5a59-11ed-9b6a-0242ac188888").unwrap();
    sqlx::query(include_str!("test_setup.sql"))
        .execute(&mut txn)
        .await
        .unwrap();

    // Clear the draft of any specs, so we can assert that they get added as necessary
    sqlx::query("delete from draft_specs where draft_id = '2230000000000000'")
        .execute(&mut txn)
        .await
        .unwrap();

    let input = serde_json::value::to_raw_value(&serde_json::json!([
        {"old_name": "evolution/CollectionA"},
        {"old_name": "evolution/CollectionC", "new_name": "evolution/CollectionC_v2"},
    ]))
    .unwrap();
    let evolution_row = Row {
        id: Id::from_hex("f100000000000000").unwrap(),
        created_at: Utc::now(),
        detail: None,
        draft_id,
        logs_token: uuid::Uuid::new_v4(),
        updated_at: Utc::now(),
        user_id,
        collections: agent_sql::TextJson(input),
        auto_publish: false,
    };

    let result = super::process_row(evolution_row, &mut txn)
        .await
        .expect("process row should succeed");

    let JobStatus::Success {
        evolved_collections,
        publication_id,
    } = result
    else {
        panic!("unexpected job status: {result:?}, expected success");
    };
    assert!(publication_id.is_none());

    insta::assert_yaml_snapshot!(evolved_collections);

    let draft_specs = sqlx::query!(
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
    insta::assert_debug_snapshot!(draft_specs);
}

#[tokio::test]
#[serial_test::parallel]
async fn evolution_preserves_changes_already_in_the_draft() {
    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .unwrap();
    let mut txn = conn.begin().await.unwrap();

    let draft_id = Id::from_hex("2230000000000000").unwrap();
    let user_id = uuid::Uuid::parse_str("43a18a3e-5a59-11ed-9b6a-0242ac188888").unwrap();
    sqlx::query(include_str!("test_setup.sql"))
        .execute(&mut txn)
        .await
        .unwrap();

    // Clear the draft, so it only contains the materialization
    sqlx::query("delete from draft_specs where draft_id = '2230000000000000'")
        .execute(&mut txn)
        .await
        .unwrap();
    sqlx::query(r##"insert into draft_specs (draft_id, catalog_name, spec_type, spec) values (
            '2230000000000000',
            'evolution/MaterializationA',
            'materialization',
            '{
                "bindings": [
                    {"source": "evolution/CollectionA", "backfill": 11, "resource": {"targetThingy": "newThing", "new": "stuff"}},
                    {"source": "evolution/CollectionB", "resource": {"targetThingy": "bThing"}}
                ],
                "endpoint": {"connector": {"image": "matImage:v1", "config": {"new": {"stuff": "here"}}}}
            }'::json
        )"##)
        .execute(&mut txn)
        .await
        .unwrap();

    let input = serde_json::value::to_raw_value(&serde_json::json!([
        {"old_name": "evolution/CollectionA"}
    ]))
    .unwrap();
    let evolution_row = Row {
        id: Id::from_hex("f100000000000000").unwrap(),
        created_at: Utc::now(),
        detail: None,
        draft_id,
        logs_token: uuid::Uuid::new_v4(),
        updated_at: Utc::now(),
        user_id,
        collections: agent_sql::TextJson(input),
        auto_publish: false,
    };

    let result = super::process_row(evolution_row, &mut txn)
        .await
        .expect("process row should succeed");

    let JobStatus::Success {
        evolved_collections: _,
        publication_id,
    } = result
    else {
        panic!("unexpected job status: {result:?}, expected success");
    };
    assert!(publication_id.is_none());

    let draft_specs = sqlx::query!(
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

    // We're looking for the new endpoint and resource configs, which ought to
    // be preserved, and for the backfill counter to have been incremented still
    // (even though it's already larger than the value in live_specs).
    insta::assert_debug_snapshot!(draft_specs);
}
