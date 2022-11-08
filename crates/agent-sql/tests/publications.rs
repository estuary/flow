use agent_sql::{CatalogType, Id};
use sqlx::{types::Uuid, Connection, Row};

const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

#[tokio::test]
async fn test_finding_forbidden_connectors() {
    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .expect("connect");

    let mut txn = conn.begin().await.unwrap();

    sqlx::query(
        r#"
        with p1 as (
          insert into live_specs (id, catalog_name, spec, spec_type, connector_image_name, last_build_id, last_pub_id) values
          ('aa00000000000000', 'testConnectors/Forbidden', '{}'::json, 'capture', 'forbidden_image', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
          ('bb00000000000000', 'testConnectors/Allowed', '{}'::json, 'capture', 'allowed_image', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb')
        ),
        p2 as (
            insert into connectors (external_url, image_name, title, short_description, logo_url) values
                ('http://example.com', 'allowed_image', '{"en-US": "foo"}'::json, '{"en-US": "foo"}'::json, '{"en-US": "foo"}'::json)
        )
        select 1;
        "#,
    )
    .execute(&mut txn)
    .await
    .unwrap();

    let res = agent_sql::connector_tags::resolve_unknown_connectors(
        vec![
            Id::from_hex("aa00000000000000").unwrap(),
            Id::from_hex("bb00000000000000").unwrap(),
        ],
        &mut txn,
    )
    .await
    .unwrap();

    insta::assert_json_snapshot!(res, @r#"
    [
      {
        "catalog_name": "testConnectors/Forbidden",
        "image_name": "forbidden_image"
      }
    ]
    "#);
}

#[tokio::test]
async fn test_publication_data_operations() {
    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .expect("connect");

    let mut txn = conn.begin().await.unwrap();

    // Fixture: insert live_specs, grants, drafts, and draft_specs fixtures.
    sqlx::query(
        r#"
        with p1 as (
            insert into live_specs (id, catalog_name, spec, spec_type, last_build_id, last_pub_id) values
            ('aa00000000000000', 'aliceCo/First/Thing', '1', 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('bb00000000000000', 'aliceCo/Second/Thing', '1', 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('cc00000000000000', 'aliceCo/Test/Fixture', '1', 'test', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('ff00000000000000', 'aliceCo/Unrelated/Thing', '1', 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb')
          ),
          p2 as (
              insert into user_grants(user_id, object_role, capability) values
                  ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin')
          ),
          p3 as (
              insert into role_grants(subject_role, object_role, capability) values
                  ('aliceCo/', 'aliceCo/', 'write'),
                  ('aliceCo/', 'examples/', 'admin'),
                  ('aliceCo/', 'ops/aliceCo/', 'read')
          ),
          p4 as (
            -- A "stale" flow of Second/Thing reading First/Thing, which we'll remove later.
            insert into live_spec_flows (source_id, target_id, flow_type) values
            ('aa00000000000000', 'bb00000000000000', 'collection')
          ),
          p5 as (
            insert into drafts (id, user_id) values
            ('dddddddddddddddd', '11111111-1111-1111-1111-111111111111')
          ),
          p6 as (
            insert into draft_specs (id, draft_id, catalog_name, spec, spec_type) values
            ('1100000000000000', 'dddddddddddddddd', 'aliceCo/First/Thing', '2', 'collection'),
            ('2200000000000000', 'dddddddddddddddd', 'aliceCo/Second/Thing', null, null),
            ('3300000000000000', 'dddddddddddddddd', 'aliceCo/New/Thing', '2', 'collection'),
            ('4400000000000000', 'dddddddddddddddd', 'otherCo/Not/AliceCo', '2', 'collection'),
            ('5500000000000000', 'dddddddddddddddd', 'aliceCo/Test/Fixture', '2', 'test')
          ),
          p7 as (
            insert into publications (id, user_id, draft_id) values
            ('eeeeeeeeeeeeeeee', '11111111-1111-1111-1111-111111111111','dddddddddddddddd')
          )
          select 1;
        "#,
    )
    .execute(&mut txn)
    .await
    .unwrap();

    let draft_id = Id::from_hex("dddddddddddddddd").unwrap();
    let pub_id = Id::from_hex("eeeeeeeeeeeeeeee").unwrap();
    let alice = Uuid::from_bytes([
        0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
        0x11,
    ]);

    // Create new `live_specs` where they don't already exist, with a NULL `spec_type`.
    agent_sql::publications::insert_new_live_specs(draft_id, pub_id, &mut txn)
        .await
        .unwrap();

    // Expect live_specs are created for new specs.
    let flows = sqlx::query(
        "select format('%s: %L', catalog_name, spec_type) from live_specs order by catalog_name",
    )
    .fetch_all(&mut txn)
    .await
    .unwrap();
    insta::assert_json_snapshot!(flows
        .iter()
        .map(|r| -> String { r.get(0) })
        .collect::<Vec<_>>(), @r###"
    [
      "aliceCo/First/Thing: 'collection'",
      "aliceCo/New/Thing: NULL",
      "aliceCo/Second/Thing: 'collection'",
      "aliceCo/Test/Fixture: 'test'",
      "aliceCo/Unrelated/Thing: 'collection'",
      "otherCo/Not/AliceCo: NULL"
    ]
    "###);

    // Expect we resolve the correct specifications, with corresponding grants.
    let resolved = agent_sql::publications::resolve_spec_rows(draft_id, alice, &mut txn)
        .await
        .unwrap();

    insta::assert_json_snapshot!(resolved, {
      "[].live_spec_id" => "<redacted>",
    });

    // Remove a stale flow of a now-deleted spec.
    agent_sql::publications::delete_stale_flow(
        Id::from_hex("bb00000000000000").unwrap(),
        CatalogType::Collection,
        &mut txn,
    )
    .await
    .unwrap();

    // Insert a number of flows between `aliceCo/Test/Fixture` and other specs.
    // Expect all flows that can be resolved, are resolved. Others are ignored.
    agent_sql::publications::insert_live_spec_flows(
        Id::from_hex("cc00000000000000").unwrap(),
        &Some(agent_sql::CatalogType::Test),
        Some(vec!["aliceCo/First/Thing", "does/not/exist"]),
        Some(vec![
            "aliceCo/First/Thing",
            "aliceCo/Second/Thing",
            "also/does/not/exist",
        ]),
        &mut txn,
    )
    .await
    .unwrap();

    let flows = sqlx::query(
        "select format('%s => %s (%s)', source_id, target_id, flow_type) from live_spec_flows order by source_id, target_id;",
    )
    .fetch_all(&mut txn)
    .await
    .unwrap();

    insta::assert_debug_snapshot!(flows
        .iter()
        .map(|r| -> String { r.get(0) })
        .collect::<Vec<_>>(), @r###"
    [
        "aa:00:00:00:00:00:00:00 => cc:00:00:00:00:00:00:00 (test)",
        "cc:00:00:00:00:00:00:00 => aa:00:00:00:00:00:00:00 (test)",
        "cc:00:00:00:00:00:00:00 => bb:00:00:00:00:00:00:00 (test)",
    ]
    "###);

    // Apply all updates to `live_specs` and delete from `draft_specs`.
    // Also insert into `publication_specs` history table.
    for row in resolved {
        agent_sql::publications::update_published_live_spec(
            &row.catalog_name,
            Some(&"an/image".to_string()),
            Some(&"a-tag".to_string()),
            &row.draft_spec,
            &row.draft_type,
            row.live_spec_id,
            pub_id,
            &Some(vec!["reads/from"]),
            &Some(vec!["writes/to"]),
            &mut txn,
        )
        .await
        .unwrap();

        agent_sql::publications::insert_publication_spec(
            row.live_spec_id,
            pub_id,
            Some(&"the details".to_string()),
            &row.draft_spec,
            &row.draft_type,
            alice,
            &mut txn,
        )
        .await
        .unwrap();

        agent_sql::publications::delete_draft_spec(row.draft_spec_id, &mut txn)
            .await
            .unwrap();
    }

    // Expect `draft_specs` is now empty.
    assert!(sqlx::query("select id from draft_specs")
        .fetch_optional(&mut txn)
        .await
        .unwrap()
        .is_none());

    // Expect `live_specs` reflects our updates.
    let flows = sqlx::query("select to_json(l) from live_specs l order by catalog_name")
        .fetch_all(&mut txn)
        .await
        .unwrap();

    insta::assert_json_snapshot!(flows
        .iter()
        .map(|r| -> serde_json::Value { r.get(0) })
        .collect::<Vec<_>>(),
    {
      "[].id" => "<redacted-id>",
      "[].created_at" => "<redacted-timestamp>",
      "[].updated_at" => "<redacted-timestamp>",
    });
}

#[tokio::test]
async fn test_tenant_usage_quotas() {
    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .expect("connect");

    let mut txn = conn.begin().await.unwrap();

    // Fixture: insert live_specs, grants, drafts, and draft_specs fixtures.
    sqlx::query(
        r#"
        with p1 as (
            insert into live_specs (id, catalog_name, spec, spec_type, last_build_id, last_pub_id) values
            ('1000000000000000', 'usageA/CollectionA', '1', 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('2000000000000000', 'usageA/CaptureA', '1', 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('3000000000000000', 'usageA/MaterializationA', '1', 'materialization', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('5000000000000000', 'usageA/DerivationA', '{"derivation": {}}'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('6000000000000000', 'usageB/CaptureA', '1', 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('7000000000000000', 'usageB/CaptureB', '1', 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('8000000000000000', 'usageB/CaptureC', '1', 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('9000000000000000', 'usageB/CaptureD', '1', 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('1100000000000000', 'usageB/CaptureDisabled', '{"shards": {"disable": true}}'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb')
          ),
          p2 as (
              insert into tenants (tenant, tasks_quota, collections_quota) values
              ('usageA/', 6, 3),
              ('usageB/', 1, 5)
          )
          select 1;
        "#,
    )
    .execute(&mut txn)
    .await
    .unwrap();

    let res = agent_sql::publications::find_tenant_quotas(
        vec![
            Id::from_hex("1000000000000000").unwrap(), // usageA/
            Id::from_hex("6000000000000000").unwrap(), // usageB/
        ],
        &mut txn,
    )
    .await
    .unwrap();

    insta::assert_debug_snapshot!(res, @r#"
        [
            Tenant {
                name: "usageA/",
                tasks_quota: 6,
                collections_quota: 3,
                tasks_used: 3,
                collections_used: 2,
            },
            Tenant {
                name: "usageB/",
                tasks_quota: 1,
                collections_quota: 5,
                tasks_used: 4,
                collections_used: 0,
            },
        ]
    "#);
}
