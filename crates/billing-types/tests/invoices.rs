fn stats(
    name: &str,
    ts: &str,
    read: u64,
    written: u64,
    seconds: u64,
) -> catalog_stats::CatalogStats {
    serde_json::from_value(serde_json::json!({
        "_meta": {"uuid": "00000000-0000-0000-0000-000000000000"},
        "catalogName": name,
        "ts": ts,
        "statsSummary": {
            "readByMe": {"bytesTotal": read, "docsTotal": 0},
            "writtenByMe": {"bytesTotal": written, "docsTotal": 0},
            "usageSeconds": seconds
        }
    }))
    .unwrap()
}

#[sqlx::test(migrations = "../../supabase/migrations")]
async fn generates_invoices_from_exact_daily_rollups(pool: sqlx::PgPool) {
    let client = catalog_stats::test_util::connect_emulator("estuary-local").await;
    let prefix = "billingio/";
    catalog_stats::test_util::delete_rows_with_prefix(&client, prefix)
        .await
        .unwrap();
    catalog_stats::test_util::delete_rows_with_prefix(&client, "billingempty/")
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenants (tenant, data_tiers, usage_tiers, recurring_usd_cents, trial_start) VALUES ('billingio/', ARRAY[50], ARRAY[14], 1000, '2026-08-02'), ('billingempty/', ARRAY[50], ARRAY[14], 500, NULL)")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO internal.billing_adjustments (tenant, billed_month, usd_cents, authorizer, detail) VALUES ('billingio/', '2026-08-01T00:00:00Z', 123, 'test', 'adjustment'), ('billingio/', '2026-07-01T00:00:00Z', 999, 'test', 'other month')")
        .execute(&pool).await.unwrap();
    catalog_stats::test_util::seed_rows(
        &client,
        &[
            (
                catalog_stats::Grain::Daily,
                stats(
                    prefix,
                    "2026-08-01T00:00:00Z",
                    400_000_000,
                    600_000_000,
                    1800,
                ),
            ),
            (
                catalog_stats::Grain::Daily,
                stats(prefix, "2026-08-02T00:00:00Z", 100_000_000, 0, 3600),
            ),
            (
                catalog_stats::Grain::Daily,
                stats(prefix, "2026-09-01T00:00:00Z", 9_000_000_000, 0, 0),
            ),
            (
                catalog_stats::Grain::Daily,
                stats(
                    "billingio/task",
                    "2026-08-01T00:00:00Z",
                    9_000_000_000,
                    0,
                    0,
                ),
            ),
            (
                catalog_stats::Grain::Monthly,
                stats(prefix, "2026-08-01T00:00:00Z", 9_000_000_000, 0, 0),
            ),
        ],
    )
    .await
    .unwrap();
    let selected = vec![
        prefix.to_string(),
        "billingempty/".to_string(),
        prefix.to_string(),
    ];
    let got = billing_types::invoices::generate(
        &pool,
        &client,
        2026,
        chrono::Month::August,
        Some(&selected),
    )
    .await
    .unwrap();
    // Independently seed PostgreSQL with the same daily usage, then compare
    // the complete invoice JSON for this fixture.
    sqlx::query("INSERT INTO catalog_stats (catalog_name, grain, ts, flow_document, bytes_read_by_me, bytes_written_by_me, usage_seconds) VALUES ('billingio/', 'daily', '2026-08-01T00:00:00Z', '{}', 400000000, 600000000, 1800), ('billingio/', 'daily', '2026-08-02T00:00:00Z', '{}', 100000000, 0, 3600)")
        .execute(&pool).await.unwrap();
    let rust_invoice = serde_json::to_value(&got[1]).unwrap();
    let matches_sql: bool = sqlx::query_scalar("SELECT internal.billing_report_202308('billingio/'::public.catalog_prefix, '2026-08-01T00:00:00Z'::timestamptz) = $1::jsonb")
        .bind(sqlx::types::Json(&rust_invoice)).fetch_one(&pool).await.unwrap();
    assert!(
        matches_sql,
        "Rust invoice calculation differs from SQL: {rust_invoice}"
    );
    assert_eq!(got[0].billed_prefix, "billingempty/");
    assert_eq!(got[0].recurring_fee, 500);
    assert!(got[0].daily_usage.is_empty());
    sqlx::query(
        "INSERT INTO tenants (tenant)
         SELECT ('billinggap' || n || '/')::public.catalog_tenant
         FROM generate_series(1, 499) AS n",
    )
    .execute(&pool)
    .await
    .unwrap();
    let all = billing_types::invoices::generate(&pool, &client, 2026, chrono::Month::August, None)
        .await
        .unwrap();
    assert_eq!(all.len(), 501);
    assert!(
        all.windows(2)
            .all(|pair| pair[0].billed_prefix < pair[1].billed_prefix)
    );
    assert_eq!(
        serde_json::to_value(all.last().unwrap()).unwrap(),
        serde_json::to_value(&got[1]).unwrap()
    );
    assert!(
        billing_types::invoices::generate(
            &pool,
            &client,
            2026,
            chrono::Month::August,
            Some(&["missing/".into()])
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("missing/")
    );
}

#[sqlx::test(migrations = "../../supabase/migrations")]
async fn propagates_bigtable_read_failure(pool: sqlx::PgPool) {
    sqlx::query("INSERT INTO tenants (tenant) VALUES ('billingfailure/')")
        .execute(&pool)
        .await
        .unwrap();
    let client = catalog_stats::test_util::connect_emulator("nonexistent-instance").await;
    let result = billing_types::invoices::generate(
        &pool,
        &client,
        2026,
        chrono::Month::August,
        Some(&["billingfailure/".into()]),
    )
    .await;
    assert!(result.is_err());
}
