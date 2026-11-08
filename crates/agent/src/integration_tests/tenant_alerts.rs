use crate::integration_tests::harness::TestHarness;
use control_plane_api::billing::BillingProvider;
use models::status::AlertType;

/// Runs through user onboarding, from signup, to free trial, to entering a
/// credit card, to the end of the trial. Asserts that the tenant controller
/// sends the proper emails at each step.
#[tokio::test]
async fn test_tenant_alerts_happy_path() {
    let mut harness = TestHarness::init("new_user_onboarding").await;

    let _user_id = harness.setup_tenant("deer").await;
    harness
        .billing_provider
        .add_customer("deer/", "cus_deer", None);
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

    harness
        .billing_provider
        .update_customer_default_payment_method(&"cus_deer".parse().unwrap(), Some("pm_deer"))
        .await
        .unwrap();
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
    harness
        .billing_provider
        .update_customer_default_payment_method(&"cus_deer".parse().unwrap(), None)
        .await
        .unwrap();
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
        .assert_emails_sent(&[]);
}

#[tokio::test]
async fn test_claimed_notification_after_silent_close() {
    let mut harness = TestHarness::init("claimed_notification_after_silent_close").await;
    harness.setup_tenant("acmeCo").await;
    let controller_id: models::Id =
        sqlx::query_scalar("select controller_task_id from tenants where tenant = 'acmeCo/'")
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    run_tenant_controller(controller_id, &mut harness).await;

    let server = automations::Server::new().register(
        crate::alerts::AlertNotifications::new(
            "http://estuary.test/",
            harness.alert_sender.clone(),
        )
        .unwrap(),
    );
    let mut permit = std::sync::Arc::new(tokio::sync::Semaphore::new(1))
        .acquire_owned()
        .await
        .unwrap();
    let heartbeat_timeout = std::time::Duration::from_secs(10);
    let mut tasks = automations::server::dequeue_tasks(
        &mut permit,
        &harness.pool,
        &server,
        &[automations::task_types::ALERT_NOTIFICATIONS.0],
        heartbeat_timeout,
    )
    .await
    .unwrap();
    assert_eq!(tasks.len(), 1);
    let task = tasks.pop().unwrap();
    let alert_id = task.task.id;

    // The claimed notifier has not read the alert when its task is deleted.
    sqlx::query("update tenants set plan_kind = 'contract' where tenant = 'acmeCo/'")
        .execute(&harness.pool)
        .await
        .unwrap();
    run_tenant_controller(controller_id, &mut harness).await;
    let err = automations::executors::poll_task(task, heartbeat_timeout)
        .await
        .expect_err("the deleted task cannot persist its outcome");
    assert!(matches!(
        err.downcast_ref::<sqlx::Error>(),
        Some(sqlx::Error::RowNotFound)
    ));
    assert!(harness.alert_sender.take_sent().await.is_empty());

    let state: (bool, bool) = sqlx::query_as(
        "select resolved_at is not null, exists(select 1 from internal.tasks where task_id = $1) from alert_history where id = $1",
    )
    .bind(alert_id)
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    assert_eq!(state, (true, false));
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
