use agent_sql::TextJson;
use serde_json::value::RawValue;
use sqlx::Connection;

const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

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
            ('5000000000000000', 'usageA/DerivationA', '{"derive": {}}'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('6000000000000000', 'usageB/CaptureA', '1', 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('7000000000000000', 'usageB/CaptureB', '1', 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('8000000000000000', 'usageB/CaptureC', '1', 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('9000000000000000', 'usageB/CaptureD', '1', 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
            ('1100000000000000', 'usageB/CaptureDisabled', '{"shards": {"disable": true}}'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb')
            returning controller_task_id
          ),
          p2 as (
              insert into tenants (tenant, tasks_quota, collections_quota) values
              ('usageA/', 6, 3),
              ('usageB/', 1, 5)
          ),
          p3 as (
              insert into internal.tasks (task_id, task_type)
              select controller_task_id, 2 from p1
          )
          select 1;
        "#,
    )
    .execute(&mut txn)
    .await
    .unwrap();

    let res = agent_sql::publications::find_tenant_quotas(&["usageA/", "usageB/"], &mut *txn)
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

#[tokio::test]
async fn test_text_json_round_trip() {
    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .expect("connect");

    let mut txn = conn.begin().await.unwrap();

    let json_string = r#"{"zzz":   "first",    "aaa":"second" }"#.to_string();
    let raw = serde_json::value::RawValue::from_string(json_string.clone()).unwrap();

    struct Res {
        spec: Option<TextJson<Box<RawValue>>>,
    }

    let got: Res = sqlx::query_as!(
        Res,
        r#"
        with t as (
            insert into internal.tasks (task_id, task_type) values ('aaaaaaaaaaaaaaaa', 2)
            returning task_id
        )
        insert into live_specs(id, controller_task_id, catalog_name, last_build_id, last_pub_id, spec_type, spec)
        select 'aa00000000000000', task_id, 'acmeCo/testing', 'bb00000000000000', 'cc00000000000000', 'capture', $1
        from t
        returning spec as "spec: TextJson<Box<RawValue>>"
        "#,
        &Some(TextJson(raw.clone())) as &Option<TextJson<Box<RawValue>>>,
    )
    .fetch_one(&mut txn)
    .await
    .unwrap();

    assert_eq!(json_string, got.spec.unwrap().get().to_string());
}
