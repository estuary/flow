#[sqlx::test(migrations = "../../supabase/migrations")]
async fn freezes_once_and_retains_invoices_stripe_ids_and_manual_bills(pool: sqlx::PgPool) {
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
         INSERT INTO internal.manual_bills (tenant, usd_cents, description, date_start, date_end)
         VALUES ('freezeRetainNew/', 2000, 'Manual contract', '2026-08-01', '2026-09-01');",
    ).execute(&pool).await.unwrap();

    assert_eq!(
        billing::invoices::freeze(&pool, &client, 2026, chrono::Month::August)
            .await
            .unwrap(),
        1
    );
    let invoices: Vec<(String, chrono::DateTime<chrono::Utc>, serde_json::Value, Option<String>)> =
        sqlx::query_as("SELECT tenant::text, billed_month, report, stripe_invoice_id FROM internal.billing_historicals ORDER BY tenant, billed_month")
            .fetch_all(&pool).await.unwrap();
    assert_eq!(invoices.len(), 3);
    assert_eq!(invoices[0].2, serde_json::json!({"subtotal":73}));
    assert_eq!(invoices[0].3.as_deref(), Some("in_preserved"));
    assert_eq!(invoices[1].2, serde_json::json!({"subtotal":91}));
    assert_eq!(invoices[1].3.as_deref(), Some("in_prior_month"));
    assert_eq!(invoices[2].2["subtotal"], 625);
    assert_eq!(invoices[2].2["recurring_fee"], 500);
    assert_eq!(invoices[2].2["billed_month"], "2026-08-01T00:00:00+00:00");
    assert!(invoices[2].3.is_none());

    sqlx::raw_sql(
        "UPDATE tenants SET recurring_usd_cents = 900;
         UPDATE internal.billing_adjustments SET usd_cents = 999;",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        billing::invoices::freeze(&pool, &client, 2026, chrono::Month::August)
            .await
            .unwrap(),
        0
    );
    let after = sqlx::query_as("SELECT tenant::text, billed_month, report, stripe_invoice_id FROM internal.billing_historicals ORDER BY tenant, billed_month")
        .fetch_all(&pool).await.unwrap();
    assert_eq!(invoices, after);
    let manual: (i32, String, chrono::NaiveDate, chrono::NaiveDate) = sqlx::query_as(
        "SELECT usd_cents, description, date_start, date_end FROM internal.manual_bills WHERE tenant = 'freezeRetainNew/'",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(
        manual,
        (
            2000,
            "Manual contract".to_string(),
            chrono::NaiveDate::from_ymd_opt(2026, 8, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2026, 9, 1).unwrap()
        )
    );
}

#[sqlx::test(migrations = "../../supabase/migrations")]
async fn insertion_failure_rolls_back_the_whole_month(pool: sqlx::PgPool) {
    let client = catalog_stats::test_util::connect_emulator("estuary-local").await;
    catalog_stats::test_util::delete_rows_with_prefix(&client, "freezeRollback")
        .await
        .unwrap();
    sqlx::raw_sql(
        "INSERT INTO tenants (tenant) VALUES ('freezeRollbackA/'), ('freezeRollbackZ/');
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
    let error = billing::invoices::freeze(&pool, &client, 2026, chrono::Month::August)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("forced invoice insertion failure"));
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
        billing::invoices::freeze(&pool, &client, 2026, chrono::Month::August)
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM internal.billing_historicals")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}
