use crate::integration_tests::harness::TestHarness;
use models::status::AlertType;

/// Runs through user onboarding, from signup, to free trial, to entering a
/// credit card, to the end of the trial. Asserts that the tenant controller
/// sends the proper emails at each step.
#[tokio::test]
async fn test_tenant_alerts_happy_path() {
    let mut harness = TestHarness::init("new_user_onboarding").await;

    let _user_id = harness.setup_tenant("deer").await;
    let task_id: models::Id = sqlx::query_scalar!(
        r#"update tenants
        set stripe_customer_id = 'cus_deer'
        where tenant = 'deer/'
        returning controller_task_id as "task_id!: models::Id""#,
    )
    .fetch_one(&harness.pool)
    .await
    .expect("failed to record the tenant's Stripe customer");

    run_tenant_controller(task_id, &mut harness).await;
    assert_eq!(
        open_alert_types(&harness).await,
        [AlertType::MissingPaymentMethod]
    );
    let missing_payment_alert = harness
        .assert_alert_firing(
            "deer/alerts/missing_payment_method",
            AlertType::MissingPaymentMethod,
        )
        .await;
    missing_payment_alert.assert_emails_sent(&[]);

    let today = chrono::Utc::now().date_naive();
    set_free_trial_start_date(&mut harness, "deer/", today).await;
    run_tenant_controller(task_id, &mut harness).await;
    assert_eq!(
        open_alert_types(&harness).await,
        [AlertType::FreeTrial, AlertType::MissingPaymentMethod]
    );
    let free_trial_alert = harness
        .assert_alert_firing("deer/alerts/free_trial", AlertType::FreeTrial)
        .await;
    free_trial_alert.assert_emails_sent(&["deer@new_user_onboarding.test"]);
    assert_eq!(
        "Estuary Free Trial",
        free_trial_alert.notifications[0].subject
    );

    // The trial ends in one to four days, as months have 28 to 31 days.
    set_free_trial_start_date(
        &mut harness,
        "deer/",
        today.checked_sub_days(chrono::Days::new(27)).unwrap(),
    )
    .await;
    run_tenant_controller(task_id, &mut harness).await;
    assert_eq!(
        open_alert_types(&harness).await,
        [
            AlertType::FreeTrial,
            AlertType::FreeTrialEnding,
            AlertType::MissingPaymentMethod
        ]
    );
    let trial_ending_alert = harness
        .assert_alert_firing("deer/alerts/free_trial_ending", AlertType::FreeTrialEnding)
        .await;
    trial_ending_alert.assert_emails_sent(&["deer@new_user_onboarding.test"]);
    assert_eq!(
        "Next Up: Estuary Paid Tier",
        &trial_ending_alert.notifications[0].subject
    );

    set_mirrored_payment_method(&mut harness, Some("pm_deer")).await;
    run_tenant_controller(task_id, &mut harness).await;
    assert_eq!(
        open_alert_types(&harness).await,
        [AlertType::FreeTrial, AlertType::FreeTrialEnding]
    );
    let emails = harness
        .assert_alert_resolved(missing_payment_alert.alert.id)
        .await;
    emails.assert_emails_sent(&["deer@new_user_onboarding.test"]);
    assert_eq!(
        "Estuary: Thanks for Adding a Payment Method 🎉",
        &emails.notifications[0].subject
    );

    // The trial ended five to eight days ago.
    set_free_trial_start_date(
        &mut harness,
        "deer/",
        today.checked_sub_days(chrono::Days::new(36)).unwrap(),
    )
    .await;
    run_tenant_controller(task_id, &mut harness).await;
    assert_eq!(open_alert_types(&harness).await, []);
    let emails = harness
        .assert_alert_resolved(free_trial_alert.alert.id)
        .await;
    emails.assert_emails_sent(&["deer@new_user_onboarding.test"]);
    assert_eq!("Estuary: Paid Tier", &emails.notifications[0].subject);
    harness
        .assert_alert_resolved(trial_ending_alert.alert.id)
        .await
        .assert_emails_sent(&[]);

    // Removing the payment method after the trial has ended stalls it.
    set_mirrored_payment_method(&mut harness, None).await;
    run_tenant_controller(task_id, &mut harness).await;
    assert_eq!(
        open_alert_types(&harness).await,
        [AlertType::FreeTrialStalled, AlertType::MissingPaymentMethod]
    );
    harness
        .assert_alert_firing(
            "deer/alerts/free_trial_stalled",
            AlertType::FreeTrialStalled,
        )
        .await
        .assert_emails_sent(&["deer@new_user_onboarding.test"]);
}

async fn run_tenant_controller(task_id: models::Id, harness: &mut TestHarness) {
    harness.set_min_task_wake_at(task_id).await;
    assert_eq!(
        Some(task_id),
        harness
            .run_automation_task(automations::task_types::TENANT_CONTROLLER)
            .await,
        "tenant controller task must have run"
    );
}

async fn open_alert_types(harness: &TestHarness) -> Vec<AlertType> {
    sqlx::query_scalar!(
        r#"select alert_type as "alert_type!: AlertType"
        from alert_history
        where starts_with(catalog_name, 'deer/') and resolved_at is null
        order by alert_type::text"#,
    )
    .fetch_all(&harness.pool)
    .await
    .expect("failed to fetch open alerts")
}

async fn set_mirrored_payment_method(harness: &mut TestHarness, payment_method: Option<&str>) {
    sqlx::query!(
        r#"insert into stripe.customers (id, "invoice_settings/default_payment_method", flow_document)
        values ('cus_deer', $1, '{}')
        on conflict (id) do update set "invoice_settings/default_payment_method" = $1"#,
        payment_method,
    )
    .execute(&harness.pool)
    .await
    .expect("failed to update stripe.customers");
}

async fn set_free_trial_start_date(
    harness: &mut TestHarness,
    tenant: &str,
    when: chrono::NaiveDate,
) {
    sqlx::query!(
        r#"update tenants
        set trial_start = $2
        where tenant = $1
        returning 1 as "must_exist!: bool""#,
        tenant,
        when,
    )
    .fetch_one(&harness.pool)
    .await
    .expect("failed to set tenant free trial start date");
}
