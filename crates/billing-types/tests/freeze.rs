#[sqlx::test(migrations = "../../supabase/migrations")]
async fn freezes_once_and_retains_invoices_and_stripe_ids(pool: sqlx::PgPool) {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let client = catalog_stats::test_util::connect_emulator("estuary-local").await;
    catalog_stats::test_util::delete_rows_with_prefix(&client, "freezeRetain")
        .await
        .unwrap();
    sqlx::raw_sql(
        "INSERT INTO tenants (tenant, recurring_usd_cents) VALUES
         ('freezeRetainExisting/', 100), ('freezeRetainNew/', 500);
         INSERT INTO internal.billing_adjustments (tenant, billed_month, usd_cents, authorizer)
         VALUES ('freezeRetainNew/', '2026-08-01T00:00:00Z', 125, 'test');
         INSERT INTO internal.billing_historicals (tenant, billed_month, report, stripe_invoice_id)
         VALUES ('freezeRetainExisting/', '2026-08-01T00:00:00Z', '{\"subtotal\":73}', 'in_preserved'),
                ('freezeRetainNew/', '2026-07-01T00:00:00Z', '{\"subtotal\":91}', 'in_prior_month');
         INSERT INTO tenants (tenant)
         SELECT ('freezeRetainMiddle' || n || '/')::public.catalog_tenant
         FROM generate_series(1, 499) AS n;",
    ).execute(&pool).await.unwrap();

    assert_eq!(
        billing_types::invoices::freeze(&pool, &client, 2026, chrono::Month::August)
            .await
            .unwrap(),
        500
    );
    let invoices: Vec<(String, chrono::DateTime<chrono::Utc>, serde_json::Value, Option<String>)> =
        sqlx::query_as("SELECT tenant::text, billed_month, report, stripe_invoice_id FROM internal.billing_historicals ORDER BY tenant, billed_month")
            .fetch_all(&pool).await.unwrap();
    assert_eq!(invoices.len(), 502);
    assert_eq!(invoices[0].2, serde_json::json!({"subtotal":73}));
    assert_eq!(invoices[0].3.as_deref(), Some("in_preserved"));
    let prior = &invoices[invoices.len() - 2];
    let new = invoices.last().unwrap();
    assert_eq!(prior.2, serde_json::json!({"subtotal":91}));
    assert_eq!(prior.3.as_deref(), Some("in_prior_month"));
    assert_eq!(new.2["subtotal"], 625);
    assert_eq!(new.2["recurring_fee"], 500);
    assert_eq!(new.2["billed_month"], "2026-08-01T00:00:00+00:00");
    assert!(new.3.is_none());

    sqlx::raw_sql(
        "UPDATE tenants SET recurring_usd_cents = 900;
         UPDATE internal.billing_adjustments SET usd_cents = 999;",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        billing_types::invoices::freeze(&pool, &client, 2026, chrono::Month::August)
            .await
            .unwrap(),
        0
    );
    let after = sqlx::query_as("SELECT tenant::text, billed_month, report, stripe_invoice_id FROM internal.billing_historicals ORDER BY tenant, billed_month")
        .fetch_all(&pool).await.unwrap();
    assert_eq!(invoices, after);
}

#[sqlx::test(migrations = "../../supabase/migrations")]
async fn insertion_failure_rolls_back_the_whole_month(pool: sqlx::PgPool) {
    let client = catalog_stats::test_util::connect_emulator("estuary-local").await;
    catalog_stats::test_util::delete_rows_with_prefix(&client, "freezeRollback")
        .await
        .unwrap();
    sqlx::raw_sql(
        "INSERT INTO tenants (tenant) VALUES ('freezeRollbackA/'), ('freezeRollbackZ/');
         INSERT INTO tenants (tenant)
         SELECT ('freezeRollbackMiddle' || n || '/')::public.catalog_tenant
         FROM generate_series(1, 499) AS n;
         INSERT INTO internal.billing_historicals (tenant, billed_month, report)
         VALUES ('freezeRollbackA/', '2026-07-01T00:00:00Z', '{\"subtotal\":11}');
         CREATE FUNCTION internal.reject_test_invoice() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF NEW.tenant = 'freezeRollbackZ/' THEN RAISE EXCEPTION 'forced invoice insertion failure'; END IF;
           RETURN NEW;
         END $$;
         CREATE TRIGGER reject_test_invoice BEFORE INSERT ON internal.billing_historicals
         FOR EACH ROW EXECUTE FUNCTION internal.reject_test_invoice();",
    ).execute(&pool).await.unwrap();
    track_completed_batches(&pool).await;
    let error = billing_types::invoices::freeze(&pool, &client, 2026, chrono::Month::August)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("forced invoice insertion failure"));
    assert_eq!(completed_batches(&pool).await, 1);
    let invoices: Vec<(String, serde_json::Value)> =
        sqlx::query_as("SELECT tenant::text, report FROM internal.billing_historicals")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        invoices,
        vec![(
            "freezeRollbackA/".to_string(),
            serde_json::json!({"subtotal":11})
        )]
    );
}

#[sqlx::test(migrations = "../../supabase/migrations")]
async fn stats_failure_does_not_freeze_any_invoices(pool: sqlx::PgPool) {
    sqlx::query("INSERT INTO tenants (tenant) VALUES ('freezeReadFailure/')")
        .execute(&pool)
        .await
        .unwrap();
    let client = catalog_stats::test_util::connect_emulator("nonexistent-instance").await;
    assert!(
        billing_types::invoices::freeze(&pool, &client, 2026, chrono::Month::August)
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM internal.billing_historicals")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[sqlx::test(migrations = "../../supabase/migrations")]
async fn late_calculation_failure_rolls_back_completed_batch(pool: sqlx::PgPool) {
    let client = catalog_stats::test_util::connect_emulator("estuary-local").await;
    catalog_stats::test_util::delete_rows_with_prefix(&client, "freezeCalculation")
        .await
        .unwrap();
    sqlx::raw_sql(
        "INSERT INTO tenants (tenant, recurring_usd_cents)
         VALUES ('freezeCalculationA/', 100), ('freezeCalculationZ/', 2147483647);
         INSERT INTO tenants (tenant)
         SELECT ('freezeCalculationMiddle' || n || '/')::public.catalog_tenant
         FROM generate_series(1, 499) AS n;
         INSERT INTO internal.billing_adjustments (tenant, billed_month, usd_cents, authorizer)
         VALUES ('freezeCalculationZ/', '2026-08-01', 1, 'test');
         INSERT INTO internal.billing_historicals (tenant, billed_month, report, stripe_invoice_id)
         VALUES ('freezeCalculationA/', '2026-07-01', '{\"subtotal\":91}', 'in_prior_month');",
    )
    .execute(&pool)
    .await
    .unwrap();
    track_completed_batches(&pool).await;

    let error = billing_types::invoices::freeze(&pool, &client, 2026, chrono::Month::August)
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("calculating invoice for freezeCalculationZ/"),
        "{error:#}"
    );
    assert_eq!(completed_batches(&pool).await, 1);
    let invoices: Vec<(serde_json::Value, Option<String>)> =
        sqlx::query_as("SELECT report, stripe_invoice_id FROM internal.billing_historicals")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        invoices,
        vec![(
            serde_json::json!({"subtotal":91}),
            Some("in_prior_month".to_string())
        )]
    );
}

async fn track_completed_batches(pool: &sqlx::PgPool) {
    // Sequence changes survive rollback, proving an earlier INSERT completed.
    sqlx::raw_sql(
        "CREATE SEQUENCE internal.completed_freeze_batches;
         CREATE FUNCTION internal.count_freeze_batch() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           PERFORM nextval('internal.completed_freeze_batches');
           RETURN NULL;
         END $$;
         CREATE TRIGGER count_freeze_batch AFTER INSERT ON internal.billing_historicals
         FOR EACH STATEMENT EXECUTE FUNCTION internal.count_freeze_batch();",
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn completed_batches(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM internal.completed_freeze_batches",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}
