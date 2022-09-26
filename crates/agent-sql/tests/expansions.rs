use agent_sql::Id;
use sqlx::Connection;

const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

#[tokio::test]
async fn test_capture_expansions() {
    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .expect("connect");

    let mut txn = conn.begin().await.unwrap();

    // Fixture: a capture into two LHS and RHS collections which each have a derivation.
    sqlx::query(
        r#"
    with specs as (
        insert into live_specs (id, catalog_name, spec, spec_type) values
        ('aa00000000000000', 'g/capture', '1', 'capture'),
        ('bb00000000000000', 'g/lhs/one', '1', 'collection'),
        ('cc00000000000000', 'g/lhs/two', '1', 'collection'),
        ('dd00000000000000', 'g/rhs/one', '1', 'collection'),
        ('ee00000000000000', 'g/rhs/two', '1', 'collection')
    ),
    flows as (
        insert into live_spec_flows(source_id, target_id, flow_type) values
        ('aa00000000000000', 'bb00000000000000', 'capture'),
        ('aa00000000000000', 'dd00000000000000', 'capture'),
        ('bb00000000000000', 'cc00000000000000', 'collection'),
        ('dd00000000000000', 'ee00000000000000', 'collection')
    )
    select 1;
    "#,
    )
    .execute(&mut txn)
    .await
    .unwrap();

    assert_set(
        vec![0xaa],
        vec![0xbb, 0xdd],
        &mut txn,
        "capture expands to its direct collections",
    )
    .await;

    assert_set(
        vec![0xbb],
        vec![0xaa, 0xcc, 0xdd],
        &mut txn,
        "collection expands to bound capture, other bound collection, and its direct derivation",
    )
    .await;

    assert_set(
        vec![0xee],
        vec![0xdd],
        &mut txn,
        "derived collection expands to its source but not the capture",
    )
    .await;
}

#[tokio::test]
async fn test_materialization_expansions() {
    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .expect("connect");

    let mut txn = conn.begin().await.unwrap();

    // Fixture: a materialization of two LHS and RHS derivations which each have a source collection.
    sqlx::query(
        r#"
    with specs as (
        insert into live_specs (id, catalog_name, spec, spec_type) values
        ('aa00000000000000', 'g/materialization', '1', 'materialization'),
        ('bb00000000000000', 'g/lhs/one', '1', 'collection'),
        ('cc00000000000000', 'g/lhs/two', '1', 'collection'),
        ('dd00000000000000', 'g/rhs/one', '1', 'collection'),
        ('ee00000000000000', 'g/rhs/two', '1', 'collection')
    ),
    flows as (
        insert into live_spec_flows(source_id, target_id, flow_type) values
        ('cc00000000000000', 'aa00000000000000', 'materialization'),
        ('ee00000000000000', 'aa00000000000000', 'materialization'),
        ('bb00000000000000', 'cc00000000000000', 'collection'),
        ('dd00000000000000', 'ee00000000000000', 'collection')
    )
    select 1;
    "#,
    )
    .execute(&mut txn)
    .await
    .unwrap();

    assert_set(
        vec![0xaa],
        vec![0xbb, 0xcc, 0xdd, 0xee],
        &mut txn,
        "materialization expands to its derivations and their sources",
    )
    .await;

    assert_set(
        vec![0xcc],
        vec![0xaa, 0xbb, 0xdd, 0xee],
        &mut txn,
        "bound derivation expands to materialization and recursive sources",
    )
    .await;

    assert_set(
        vec![0xbb],
        vec![0xcc],
        &mut txn,
        "source collection expands to its derivation but not its materialization",
    )
    .await;
}

#[tokio::test]
async fn test_shared_collection_expansions() {
    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .expect("connect");

    let mut txn = conn.begin().await.unwrap();

    sqlx::query(
        r#"
    with specs as (
        insert into live_specs (id, catalog_name, spec, spec_type) values
        ('aa00000000000000', 'g/capture1', '1', 'capture'),
        ('bb00000000000000', 'g/capture2', '1', 'capture'),
        ('cc00000000000000', 'g/collection', '1', 'collection'),
        ('dd00000000000000', 'g/materialization1', '1', 'materialization'),
        ('ee00000000000000', 'g/materialization2', '1', 'materialization')
    ),
    flows as (
        insert into live_spec_flows(source_id, target_id, flow_type) values
        ('aa00000000000000', 'cc00000000000000', 'capture'),
        ('bb00000000000000', 'cc00000000000000', 'capture'),
        ('cc00000000000000', 'dd00000000000000', 'materialization'),
        ('cc00000000000000', 'ee00000000000000', 'materialization')
    )
    select 1;
    "#,
    )
    .execute(&mut txn)
    .await
    .unwrap();

    assert_set(
        vec![0xaa],
        vec![0xcc],
        &mut txn,
        "capture expands to its destination, but not other captures to that destination",
    )
    .await;

    assert_set(
        vec![0xdd],
        vec![0xcc],
        &mut txn,
        "materialization expands to its source, but not materializations from that source",
    )
    .await;

    assert_set(
        vec![0xcc],
        vec![0xaa, 0xbb, 0xdd, 0xee],
        &mut txn,
        "collection expands to all bound captures & materializations",
    )
    .await;
}

#[tokio::test]
async fn test_test_expansions() {
    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .expect("connect");

    let mut txn = conn.begin().await.unwrap();

    // Fixture: a materialization of two LHS and RHS derivations which each have a source collection.
    sqlx::query(
        r#"
    with specs as (
        insert into live_specs (id, catalog_name, spec, spec_type) values
        ('1100000000000000', 'g1/one', '1', 'collection'),
        ('2200000000000000', 'g1/two', '1', 'collection'),
        ('3300000000000000', 'g1/three', '1', 'collection'),
        ('4400000000000000', 'g1/four', '1', 'collection'),
        ('5500000000000000', 'g1/five', '1', 'collection'),
        ('6600000000000000', 'g1/six', '1', 'collection'),
        ('7700000000000000', 'g1/seven', '1', 'collection'),
        ('aa00000000000000', 'g1/test/a', '1', 'test'),
        ('bb00000000000000', 'g1/test/b', '1', 'test')
    ),
    flows as (
        insert into live_spec_flows(source_id, target_id, flow_type) values
        -- Straight shot from 1 -> 2 -> ... -> 6 -> 7
        ('1100000000000000', '2200000000000000', 'collection'),
        ('2200000000000000', '3300000000000000', 'collection'),
        ('3300000000000000', '4400000000000000', 'collection'),
        ('4400000000000000', '5500000000000000', 'collection'),
        ('5500000000000000', '6600000000000000', 'collection'),
        ('6600000000000000', '7700000000000000', 'collection'),
        -- 5 reads from itself for funsies.
        ('5500000000000000', '5500000000000000', 'collection'),
        -- Test A ingests 2 and verifies 4.
        ('aa00000000000000', '2200000000000000', 'test'),
        ('4400000000000000', 'aa00000000000000', 'test'),
        -- Test B ingests 3 and verifies 6.
        ('bb00000000000000', '3300000000000000', 'test'),
        ('6600000000000000', 'bb00000000000000', 'test')
    )
    select 1;
    "#,
    )
    .execute(&mut txn)
    .await
    .unwrap();

    // For ID's *other than* 6 or 7, we expect this component.
    // Note that it's missing 7.
    let expect_ids = vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0xaa, 0xbb];

    for id in vec![0x11, 0x22, 0x33, 0x44, 0x55, 0xaa, 0xbb].iter() {
        assert_set(
            vec![*id],
            expect_ids.iter().cloned().filter(|c| c != id).collect(),
            &mut txn,
            "connected ids are retrieved",
        )
        .await;
    }

    assert_set(
        vec![0x66],
        vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x77, 0xaa, 0xbb],
        &mut txn,
        "id 6 also pulls in its direct connection to 7",
    )
    .await;

    assert_set(
        vec![0x77],
        vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0xaa, 0xbb],
        &mut txn,
        "id 7 pulls in the entire graph",
    )
    .await;

    assert_set(
        vec![0x11, 0x22, 0x44, 0x55, 0x66, 0x77, 0xaa, 0xbb],
        vec![0x33],
        &mut txn,
        "multiple already-connected seeds are filtered",
    )
    .await;
}

async fn assert_set(
    seed_ids: Vec<u8>,
    expect_ids: Vec<u8>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    msg: &str,
) {
    let seed_ids: Vec<Id> = seed_ids.into_iter().map(|id| as_id(id)).collect();
    let expect_ids: Vec<Id> = expect_ids.into_iter().map(|id| as_id(id)).collect();

    let rows = agent_sql::publications::resolve_expanded_rows(seed_ids.clone(), txn)
        .await
        .unwrap();

    let mut actual_ids: Vec<_> = rows.into_iter().map(|r| r.live_spec_id).collect();
    actual_ids.sort();

    assert_eq!(expect_ids, actual_ids, "{msg} (seed: {seed_ids:?})");
}

fn as_id(v: u8) -> Id {
    Id::new([v, 0, 0, 0, 0, 0, 0, 0])
}
