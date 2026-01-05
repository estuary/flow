use crate::{alerts::EvaluatorMessage, integration_tests::harness::TestHarness};
use models::status::AlertType;

/// Runs through user onboarding, from signup, to free trial, to entering a
/// credit card. Asserts that we send the proper emails at each step.
#[tokio::test]
async fn test_tenant_alerts_happy_path() {
    let mut harness = TestHarness::init("new_user_onboarding").await;

    let _user_id = harness.setup_tenant("deer").await;

    let alert_task_id = harness
        .run_automation_task(automations::task_types::TENANT_ALERT_EVALS)
        .await
        .expect("tenant alerts task must have run");

    let state: serde_json::Value = harness.get_task_state(alert_task_id).await;

    insta::assert_json_snapshot!(state, { ".last_evaluation_time" => "[redacted]" }, @r#"
    {
      "failures": 0,
      "last_evaluation_time": "[redacted]",
      "last_result": {
        "fired": {
          "missing_payment_method": 1
        },
        "view_evaluated": {
          "free_trial": 1,
          "missing_payment_method": 1
        }
      },
      "open_alerts": {
        "missing_payment_method": 1
      },
      "paused_at": null
    }
    "#);

    let missing_payment_alert = harness
        .assert_alert_firing(
            "deer/alerts/missing_payment_method",
            AlertType::MissingPaymentMethod,
        )
        .await;
    missing_payment_alert.assert_emails_sent(&[]);

    let today = chrono::Utc::now().date_naive();
    set_free_trial_start_date(&mut harness, "deer/", today).await;

    let state = eval_tenant_alerts(alert_task_id, &mut harness).await;
    insta::assert_json_snapshot!(state, { ".last_evaluation_time" => "[redacted]" }, @r#"
    {
      "failures": 0,
      "last_evaluation_time": "[redacted]",
      "last_result": {
        "fired": {
          "free_trial": 1
        },
        "starting_open": {
          "missing_payment_method": 1
        },
        "view_evaluated": {
          "free_trial": 1,
          "missing_payment_method": 1
        }
      },
      "open_alerts": {
        "free_trial": 1,
        "missing_payment_method": 1
      },
      "paused_at": null
    }
    "#);

    let free_trial_emails = harness
        .assert_alert_firing("deer/alerts/free_trial", AlertType::FreeTrial)
        .await;
    free_trial_emails.assert_emails_sent(&["deer@new_user_onboarding.test"]);
    assert_eq!(
        "Estuary Free Trial",
        free_trial_emails.notifications[0].subject
    );

    set_free_trial_start_date(
        &mut harness,
        "deer/",
        today.checked_sub_days(chrono::Days::new(25)).unwrap(),
    )
    .await;
    let state = eval_tenant_alerts(alert_task_id, &mut harness).await;
    insta::assert_json_snapshot!(state, { ".last_evaluation_time" => "[redacted]" }, @r#"
    {
      "failures": 0,
      "last_evaluation_time": "[redacted]",
      "last_result": {
        "fired": {
          "free_trial_ending": 1
        },
        "starting_open": {
          "free_trial": 1,
          "missing_payment_method": 1
        },
        "view_evaluated": {
          "free_trial": 1,
          "free_trial_ending": 1,
          "missing_payment_method": 1
        }
      },
      "open_alerts": {
        "free_trial": 1,
        "free_trial_ending": 1,
        "missing_payment_method": 1
      },
      "paused_at": null
    }
    "#);

    let emails = harness
        .assert_alert_firing("deer/alerts/free_trial_ending", AlertType::FreeTrialEnding)
        .await;
    emails.assert_emails_sent(&["deer@new_user_onboarding.test"]);
    assert_eq!("Estuary Flow: Paid Tier", &emails.notifications[0].subject);

    sqlx::query(
        r#"insert into stripe.customers (
          name,
          "invoice_settings/default_payment_method",
          id,
          flow_document
        ) values ('deer/', 'some-non-null-value', 'test-deer-id', '{}');"#,
    )
    .execute(&harness.pool)
    .await
    .expect("updating stripe.customers");

    let state = eval_tenant_alerts(alert_task_id, &mut harness).await;
    insta::assert_json_snapshot!(state, { ".last_evaluation_time" => "[redacted]" }, @r#"
    {
      "failures": 0,
      "last_evaluation_time": "[redacted]",
      "last_result": {
        "resolved": {
          "missing_payment_method": 1
        },
        "starting_open": {
          "free_trial": 1,
          "free_trial_ending": 1,
          "missing_payment_method": 1
        },
        "view_evaluated": {
          "free_trial": 1,
          "free_trial_ending": 1,
          "missing_payment_method": 1
        }
      },
      "open_alerts": {
        "free_trial": 1,
        "free_trial_ending": 1,
        "missing_payment_method": 0
      },
      "paused_at": null
    }
    "#);

    let emails = harness
        .assert_alert_resolved(missing_payment_alert.alert.id)
        .await;
    emails.assert_emails_sent(&["deer@new_user_onboarding.test"]);
    assert_eq!(
        "Estuary: Thanks for Adding a Payment Method🎉",
        &emails.notifications[0].subject
    );

    set_free_trial_start_date(
        &mut harness,
        "deer/",
        today.checked_sub_days(chrono::Days::new(36)).unwrap(),
    )
    .await;
    let state = eval_tenant_alerts(alert_task_id, &mut harness).await;
    insta::assert_json_snapshot!(state, { ".last_evaluation_time" => "[redacted]" }, @r#"
    {
      "failures": 0,
      "last_evaluation_time": "[redacted]",
      "last_result": {
        "resolved": {
          "free_trial": 1,
          "free_trial_ending": 1
        },
        "starting_open": {
          "free_trial": 1,
          "free_trial_ending": 1
        },
        "view_evaluated": {
          "free_trial": 1,
          "missing_payment_method": 1
        }
      },
      "open_alerts": {
        "free_trial": 0,
        "free_trial_ending": 0
      },
      "paused_at": null
    }
    "#);
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

async fn eval_tenant_alerts(task_id: models::Id, harness: &mut TestHarness) -> serde_json::Value {
    harness
        .send_automation_message(task_id, models::Id::zero(), EvaluatorMessage::ManualTrigger)
        .await;
    harness
        .run_automation_task(automations::task_types::TENANT_ALERT_EVALS)
        .await
        .expect("tenant alerts task must have run");
    harness.get_task_state(task_id).await
}
