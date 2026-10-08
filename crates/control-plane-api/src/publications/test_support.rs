//! Fixtures shared by the publication authorization tests.

use models::authz::CapabilityBundle;

/// The user of the `alice` SQL fixture.
pub(super) fn alice() -> uuid::Uuid {
    "11111111-1111-1111-1111-111111111111".parse().unwrap()
}

pub(super) fn user_grant(
    user_id: uuid::Uuid,
    object_role: &str,
    capability: models::Capability,
    bundles: &[CapabilityBundle],
) -> tables::UserGrant {
    tables::UserGrant {
        user_id,
        object_role: models::Prefix::new(object_role),
        capability,
        bundles: bundles.to_vec(),
    }
}

pub(super) fn role_grant(
    subject_role: &str,
    object_role: &str,
    capability: models::Capability,
) -> tables::RoleGrant {
    tables::RoleGrant {
        subject_role: models::Prefix::new(subject_role),
        object_role: models::Prefix::new(object_role),
        capability,
        bundles: Vec::new(),
    }
}

/// A Snapshot carrying only these grants, so a test states a subject's
/// authority exactly rather than inheriting whatever the SQL fixtures grant.
pub(super) fn snapshot_with_grants(
    user_grants: Vec<tables::UserGrant>,
    role_grants: Vec<tables::RoleGrant>,
) -> crate::Snapshot {
    crate::Snapshot::new(
        tokens::now(),
        crate::snapshot::SnapshotData {
            collections: Vec::new(),
            data_planes: Vec::new(),
            migrations: Vec::new(),
            role_grants,
            user_grants,
            storage_mapping_data_planes: Default::default(),
            tasks: Vec::new(),
        },
    )
}

/// Gives the `alice` fixture's live specs models which parse, and connects
/// them: `aliceCo/in/capture-foo` writes `aliceCo/data/foo`, which
/// `aliceCo/out/materialize-bar` reads. The fixture's `{}` specs suffice for
/// authorization lookups but not for assembling a `LiveCatalog`.
pub(super) async fn seed_alice_catalog(pool: &sqlx::PgPool) {
    let foo = models::Collection::new("aliceCo/data/foo");

    let mut capture = models::CaptureDef::example();
    capture.bindings[0].target = foo.clone();
    let mut materialization = models::MaterializationDef::example();
    materialization.bindings[0].source = models::Source::Collection(foo);

    let specs = [
        (
            "aliceCo/data/foo",
            serde_json::to_value(models::CollectionDef::example()).unwrap(),
            serde_json::to_value(proto_flow::flow::CollectionSpec::default()).unwrap(),
        ),
        (
            "aliceCo/in/capture-foo",
            serde_json::to_value(capture).unwrap(),
            serde_json::to_value(proto_flow::flow::CaptureSpec::default()).unwrap(),
        ),
        (
            "aliceCo/out/materialize-bar",
            serde_json::to_value(materialization).unwrap(),
            serde_json::to_value(proto_flow::flow::MaterializationSpec::default()).unwrap(),
        ),
    ];
    for (name, spec, built_spec) in specs {
        sqlx::query("update live_specs set spec = $1, built_spec = $2 where catalog_name = $3")
            .bind(spec)
            .bind(built_spec)
            .bind(name)
            .execute(pool)
            .await
            .unwrap();
    }

    // Ids are those declared by `fixtures/alice.sql`.
    sqlx::query(
        r#"
        insert into live_spec_flows (source_id, target_id, flow_type) values
          ('000000000005', '000000000001', 'capture'),
          ('000000000001', '000000000006', 'materialization')
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
}
