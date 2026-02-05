mod auto_discover_failed;
mod background_publication_failed;
mod data_movement_stalled;
mod free_trial;
mod free_trial_ending;
mod free_trial_stalled;
mod missing_payment_method;
mod shard_failed;

use anyhow::Context;
use chrono::{DateTime, Utc};
use models::status::AlertType;
use std::collections::BTreeMap;

/// Represents the state of an alert that has fired, and may or may not be
/// resolved.
#[derive(Clone, Debug)]
pub struct AlertState {
    pub alert_id: models::Id,
    pub catalog_name: models::Name,
    pub alert_type: AlertType,
    pub fired_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub arguments: BTreeMap<String, serde_json::Value>,
    pub resolved_arguments: BTreeMap<String, serde_json::Value>,
}

impl AlertState {
    fn get_effective_arguments<'a>(&'a self) -> &'a BTreeMap<String, serde_json::Value> {
        if self.resolved_at.is_some() && !self.resolved_arguments.is_empty() {
            &self.resolved_arguments
        } else {
            &self.arguments
        }
    }

    fn template_data<'a>(
        &'a self,
        body_template_name: &'a str,
        dashboard_base_url: &'a str,
    ) -> anyhow::Result<Vec<EmailTemplateData<'a>>> {
        let arguments = self.get_effective_arguments();
        let Some(recipients) = arguments.get("recipients").filter(|r| !r.is_null()) else {
            return Ok(Vec::new());
        };
        let emails: Vec<Recipient> =
            serde_json::from_value(recipients.clone()).context("deserializing alert recipients")?;

        let templates = emails
            .into_iter()
            .map(|recipient| EmailTemplateData {
                recipient,
                arguments,
                body_template_name,
                dashboard_base_url,
                catalog_name: self.catalog_name.as_str(),
                alert_type: self.alert_type,
                fired_at: self.fired_at,
                resolved_at: self.resolved_at,
            })
            .collect();
        Ok(templates)
    }
}

/// An individual recipient of an alert
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Recipient {
    pub email: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_name: Option<String>,
}

/// This struct gets passed to the template that renders the email subjects and
/// bodies. It borrows data from `AlertState`.
#[derive(Clone, Debug, serde::Serialize)]
struct EmailTemplateData<'a> {
    recipient: Recipient,
    catalog_name: &'a str,
    body_template_name: &'a str,
    dashboard_base_url: &'a str,
    alert_type: AlertType,
    fired_at: DateTime<Utc>,
    resolved_at: Option<DateTime<Utc>>,
    arguments: &'a BTreeMap<String, serde_json::Value>,
}

impl<'a> EmailTemplateData<'a> {
    fn into_recipient(self) -> Recipient {
        self.recipient
    }
}

fn template_names(alert_type: AlertType, resolved: bool) -> (String, String) {
    let suffix = if resolved { "resolved" } else { "fired" };
    (
        format!("{alert_type}-subject-{suffix}"),
        format!("{alert_type}-body-{suffix}"),
    )
}

#[derive(Debug)]
pub struct NotificationEmail {
    pub idempotency_key: String,
    pub recipient: Recipient,
    pub subject: String,
    pub body: String,
}

#[derive(Debug)]
pub struct Renderer {
    dashboard_base_url: String,
    hb: handlebars::Handlebars<'static>,
}

impl Renderer {
    pub fn try_new(dashboard_base_url: String) -> anyhow::Result<Renderer> {
        anyhow::ensure!(
            dashboard_base_url.ends_with("/"),
            "dashboard_base_url must end with a '/'"
        );
        anyhow::ensure!(
            dashboard_base_url.starts_with("http"),
            "dashboard_base_url must include the http:// or https:// scheme"
        );
        let mut hb = handlebars::Handlebars::new();
        hb.set_strict_mode(true);

        if cfg!(test) {
            hb.set_dev_mode(true);
        }

        // Register common wrapper template as a partial
        register_common_templates(&mut hb)?;

        auto_discover_failed::register_templates(&mut hb)?;
        background_publication_failed::register_templates(&mut hb)?;
        data_movement_stalled::register_templates(&mut hb)?;
        free_trial::register_templates(&mut hb)?;
        free_trial_stalled::register_templates(&mut hb)?;
        free_trial_ending::register_templates(&mut hb)?;
        missing_payment_method::register_templates(&mut hb)?;
        shard_failed::register_templates(&mut hb)?;

        Ok(Renderer {
            dashboard_base_url,
            hb,
        })
    }

    /// Renders an email notification for the given alert state, if sending a
    /// notification is appropriate, based on the type of alert and whether it's
    /// resolved or not. A return value of `None` indicates that no email should
    /// be sent, even if users are subscribed to that alert type and there are
    /// `recipients` in the arguments. It's also possible for this to return
    /// `Some`, but for the `NotificationEmail` `recipients` to be empty. This
    /// indicates that an email normally _would_ be sent, but nobody was
    /// subscribed to the alert type. In that case, we still render the email so
    /// that we can log the subject and the fact that nobody was subscribed.
    pub fn render_emails(&self, alert: &AlertState) -> anyhow::Result<Vec<NotificationEmail>> {
        let stage = alert.resolved_at.map(|_| "resolved").unwrap_or("fired");
        let (subject_template_name, body_template_name) =
            template_names(alert.alert_type, alert.resolved_at.is_some());

        // Sending an alert notification is optional at either the firing or resolved stage.
        // If there's no subject template, then we take it to mean that we don't want to send
        // an email.
        if self
            .hb
            .get_template(subject_template_name.as_str())
            .is_none()
        {
            return Ok(Vec::new());
        }

        let all_args = alert.template_data(&body_template_name, &self.dashboard_base_url)?;
        let mut emails = Vec::with_capacity(all_args.len());
        for (index, template_args) in all_args.into_iter().enumerate() {
            let subject = self
                .hb
                .render(subject_template_name.as_str(), &template_args)
                .with_context(|| format!("rendering subject template '{subject_template_name}'"))?;

            // If a subject template was present, then a body template is required.
            let body = self
                .hb
                .render("email_wrapper", &template_args)
                .with_context(|| format!("rendering body template '{subject_template_name}'"))?;

            // The idempotency key must be unique for each email that we wish to send, so we use
            // the alert id, stage, and the index of the recipient within the recipients array.
            let idempotency_key = format!("{}-{}-{}", alert.alert_id, stage, index);
            let recipient = template_args.into_recipient();

            emails.push(NotificationEmail {
                idempotency_key,
                recipient,
                subject,
                body,
            })
        }

        Ok(emails)
    }
}

fn register_common_templates<'a>(registry: &mut handlebars::Handlebars<'a>) -> anyhow::Result<()> {
    // Common email wrapper template that's used by all alert types, so we have consistent styling.
    // Note that this html was generated by an LLM, from the original `mjml` template in the legacy
    // alerts edge function. That legacy template used the `mjml-browser` library to render html
    // with styling that matches our UI. This LLM-translation doesn't match that _perfectly_,
    // but was considered good enough to allow getting rid of `mjml`.
    registry.register_template_string(
        "email_wrapper",
        r#"<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body { margin: 0; padding: 0; font-family: Ubuntu, Helvetica, Arial, sans-serif; }
        .email-container { max-width: 600px; margin: 0 auto; }
        .email-content { padding: 20px; }
        .logo { text-align: center; padding: 20px 0; }
        .divider { border-top: 1px dashed grey; margin: 20px 0; }
        .dear-line { font-size: 20px; color: #512d0b; font-weight: bold; margin-bottom: 10px; }
        .body-text { font-size: 17px; line-height: 1.4; margin-bottom: 10px; }
        .signature { font-size: 14px; color: #000000; margin-top: 15px; }
        .identifier { background-color: #dadada; padding: 2px 3px; border-radius: 2px; font-family: monospace; font-weight: bold; }
        a { color: #5072EB; text-decoration: none; }
        .button {
            display: inline-block;
            background-color: #5072EB;
            color: white;
            padding: 12px 24px;
            border-radius: 4px;
            text-decoration: none;
            margin: 25px 0 0 0;
            font-weight: 400;
            font-size: 17px;
        }
        ul { margin: 10px 0; padding-left: 20px; }
        li { margin: 5px 0; }
    </style>
</head>
<body>
    <div class="email-container">
        <div class="logo">
            <img src="https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//estuary_logo_comfy_14513ca434/estuary_logo_comfy_14513ca434.jpg" alt="Estuary Logo" style="max-width: 200px;">
        </div>
        <div class="divider"></div>
        <div class="email-content">
            {{#if full_name}}
            <div class="dear-line">Dear {{full_name}},</div>
            {{/if}}
            {{> (lookup this "body_template_name")}}
            <div class="signature">
                Thanks,<br>
                Estuary Team
            </div>
        </div>
    </div>
</body>
</html>"#,
    ).context("registering email_wrapper template")?;

    // Helper partial for rendering a catalog name identifier
    registry
        .register_template_string(
            "catalog_identifier",
            r#"<a class="identifier">{{catalog_name}}</a>"#,
        )
        .context("registering catalog_identifier partial")?;

    // Helper partial for task details URL
    registry.register_template_string(
        "spec_dashboard_overview_url",
        r#"{{dashboard_base_url}}{{arguments.spec_type}}s/details/overview?catalogName={{catalog_name}}"#,
    ).context("registering task_url partial")?;

    registry.register_template_string(
        "dashboard_billing_url",
        r#"{{dashboard_base_url}}admin/billing"#,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    enum State {
        Fired,
        Resolved,
        ResolvedWith(serde_json::Value),
    }

    fn make_alert(
        alert_type: AlertType,
        catalog_name: &str,
        arguments: serde_json::Value,
        state: State,
    ) -> AlertState {
        let fired_at = chrono::Utc
            .with_ymd_and_hms(2024, 1, 15, 10, 30, 0)
            .unwrap();

        let arguments: BTreeMap<String, serde_json::Value> =
            serde_json::from_value(arguments).expect("invalid arguments");
        let (resolved_at, resolved_arguments): (
            Option<DateTime<Utc>>,
            BTreeMap<String, serde_json::Value>,
        ) = match state {
            State::ResolvedWith(args) => {
                let resolved_args: BTreeMap<String, serde_json::Value> =
                    serde_json::from_value(args).expect("invalid resolved_arguments");
                (Some(fired_at + chrono::Duration::hours(6)), resolved_args)
            }
            State::Resolved => (
                Some(fired_at + chrono::Duration::hours(2)),
                Default::default(),
            ),
            State::Fired => (None, Default::default()),
        };

        AlertState {
            alert_id: models::Id::from_hex("0102030405060708").unwrap(),
            catalog_name: models::Name::new(catalog_name),
            alert_type,
            fired_at,
            resolved_at,
            arguments,
            resolved_arguments,
        }
    }

    fn test_single_email(
        alert_type: AlertType,
        catalog_name: &str,
        arguments: serde_json::Value,
        state: State,
    ) -> NotificationEmail {
        let renderer = Renderer::try_new("http://dashboard.estuary.test/".to_string()).unwrap();
        let alert = make_alert(alert_type, catalog_name, arguments, state);
        let mut emails = renderer
            .render_emails(&alert)
            .expect("failed to render alert");
        assert_eq!(1, emails.len(), "expected a single email to be returned");
        emails.pop().unwrap()
    }

    fn expect_no_email(
        alert_type: AlertType,
        catalog_name: &str,
        arguments: serde_json::Value,
        state: State,
    ) {
        let renderer = Renderer::try_new("http://dashboard.estuary.test/".to_string()).unwrap();
        let alert = make_alert(alert_type, catalog_name, arguments, state);
        let emails = renderer
            .render_emails(&alert)
            .expect("failed to render alert");
        assert!(emails.is_empty(), "expected no email to be sent");
    }

    fn assert_email(
        email: &NotificationEmail,
        expect_recipient: Recipient,
        expect_idempotency_key: &str,
        expect_subject: &str,
    ) {
        assert_eq!(expect_recipient, email.recipient, "recipient mismatch");
        assert_eq!(
            expect_idempotency_key, email.idempotency_key,
            "idempotency key mismatch"
        );
        assert_eq!(expect_subject, email.subject, "subject mismatch");
    }

    const EXPECT_IDEMPOTENCY_KEY_FIRED: &str = "0102030405060708-fired-0";
    const EXPECT_IDEMPOTENCY_KEY_RESOLVED: &str = "0102030405060708-resolved-0";

    fn user_a() -> Recipient {
        Recipient {
            email: String::from("user-a@example.com"),
            full_name: Some(String::from("Foo Bar")),
        }
    }
    fn user_b() -> Recipient {
        Recipient {
            email: String::from("user-b@example.com"),
            full_name: None,
        }
    }

    #[test]
    fn test_auto_discover_failed_fired() {
        let email = test_single_email(
            AlertType::AutoDiscoverFailed,
            "acmeCo/test/capture",
            json!({
                "recipients": [ user_a() ],
                "spec_type": "capture",
            }),
            State::Fired,
        );

        assert_email(
            &email,
            user_a(),
            EXPECT_IDEMPOTENCY_KEY_FIRED,
            "Estuary Flow: Auto-discover failed for capture acmeCo/test/capture",
        );
        insta::assert_snapshot!("auto_discover_failed_fired_body", email.body);
    }

    #[test]
    fn test_auto_discover_failed_resolved() {
        let email = test_single_email(
            AlertType::AutoDiscoverFailed,
            "acmeCo/test/capture",
            json!({
                "recipients": [user_a()],
                "spec_type": "capture",
            }),
            State::Resolved,
        );

        assert_email(
            &email,
            user_a(),
            EXPECT_IDEMPOTENCY_KEY_RESOLVED,
            "Estuary Flow: Auto-discover resolved for capture acmeCo/test/capture",
        );
        insta::assert_snapshot!("auto_discover_failed_resolved_body", email.body);
    }

    #[test]
    fn test_background_publication_failed_fired() {
        let email = test_single_email(
            AlertType::BackgroundPublicationFailed,
            "acmeCo/test/capture",
            json!({
                "recipients": [ user_a() ],
                "spec_type": "capture",
            }),
            State::Fired,
        );

        assert_email(
            &email,
            user_a(),
            EXPECT_IDEMPOTENCY_KEY_FIRED,
            "Estuary Flow: Automated background publication failed for capture acmeCo/test/capture",
        );
        insta::assert_snapshot!("background_publication_failed_fired_body", email.body);
    }

    #[test]
    fn test_background_publication_failed_resolved() {
        let email = test_single_email(
            AlertType::BackgroundPublicationFailed,
            "acmeCo/test/capture",
            json!({
                "recipients": [user_a()],
                "spec_type": "capture",
            }),
            State::Resolved,
        );

        assert_email(
            &email,
            user_a(),
            EXPECT_IDEMPOTENCY_KEY_RESOLVED,
            "Estuary Flow: Automated background publication alert resolved for capture acmeCo/test/capture",
        );
        insta::assert_snapshot!("background_publication_failed_resolved_body", email.body);
    }

    #[test]
    fn test_data_movement_stalled_fired() {
        let email = test_single_email(
            AlertType::DataMovementStalled,
            "acmeCo/test/materialization",
            json!({
                "recipients": [user_b()],
                "spec_type": "materialization",
                "evaluation_interval": "04:00:00",
                "bytes_processed": 0,
            }),
            State::Fired,
        );

        assert_email(
            &email,
            user_b(),
            EXPECT_IDEMPOTENCY_KEY_FIRED,
            "Estuary Flow: Alert for materialization acmeCo/test/materialization",
        );
        insta::assert_snapshot!("data_movement_stalled_fired_body", email.body);
    }

    #[test]
    fn test_data_movement_stalled_resolved() {
        let email = test_single_email(
            AlertType::DataMovementStalled,
            "acmeCo/test/materialization",
            json!({
                "recipients": [user_b()],
                "spec_type": "materialization",
                "evaluation_interval": "02:00:00",
                "bytes_processed": 0,
            }),
            State::Resolved,
        );

        assert_email(
            &email,
            user_b(),
            EXPECT_IDEMPOTENCY_KEY_RESOLVED,
            "Estuary Flow: Alert resolved for materialization acmeCo/test/materialization",
        );
        insta::assert_snapshot!("data_movement_stalled_resolved_body", email.body);
    }

    #[test]
    fn test_free_trial_fired_with_credit_card() {
        let email = test_single_email(
            AlertType::FreeTrial,
            "acmeCo/",
            json!({
                "recipients": [user_a()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "has_credit_card": true,
            }),
            State::Fired,
        );

        assert_email(
            &email,
            user_a(),
            EXPECT_IDEMPOTENCY_KEY_FIRED,
            "Estuary Free Trial",
        );
        insta::assert_snapshot!("free_trial_fired_with_cc_body", email.body);
    }

    #[test]
    fn test_free_trial_fired_without_credit_card() {
        let email = test_single_email(
            AlertType::FreeTrial,
            "acmeCo/",
            json!({
                "recipients": [user_a()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "has_credit_card": false,
            }),
            State::Fired,
        );

        assert_email(
            &email,
            user_a(),
            EXPECT_IDEMPOTENCY_KEY_FIRED,
            "Estuary Free Trial",
        );
        insta::assert_snapshot!("free_trial_fired_without_cc_body", email.body);
    }

    #[test]
    fn test_free_trial_resolved_with_credit_card() {
        let email = test_single_email(
            AlertType::FreeTrial,
            "acmeCo/",
            json!({
                "recipients": [user_a()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "has_credit_card": false,
            }),
            State::ResolvedWith(json!({
                "recipients": [user_b()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "has_credit_card": true,
            })),
        );

        assert_email(
            &email,
            user_b(),
            EXPECT_IDEMPOTENCY_KEY_RESOLVED,
            "Estuary Flow: Paid Tier",
        );
        insta::assert_snapshot!("free_trial_resolved_with_cc_body", email.body);
    }

    #[test]
    fn test_free_trial_resolved_without_credit_card() {
        let email = test_single_email(
            AlertType::FreeTrial,
            "acmeCo/",
            json!({
                "recipients": [user_b()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "has_credit_card": false,
            }),
            State::ResolvedWith(json!({
                "recipients": [user_a()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "has_credit_card": false,
            })),
        );

        assert_email(
            &email,
            user_a(),
            EXPECT_IDEMPOTENCY_KEY_RESOLVED,
            "Estuary Paid Tier: Enter Payment Info to Continue Access",
        );
        insta::assert_snapshot!("free_trial_resolved_without_cc_body", email.body);
    }

    #[test]
    fn test_free_trial_ending_fired_with_credit_card() {
        let email = test_single_email(
            AlertType::FreeTrialEnding,
            "acmeCo/",
            json!({
                "recipients": [user_a()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "has_credit_card": true,
            }),
            State::Fired,
        );

        assert_email(
            &email,
            user_a(),
            EXPECT_IDEMPOTENCY_KEY_FIRED,
            "Estuary Flow: Paid Tier",
        );
        insta::assert_snapshot!("free_trial_ending_fired_with_cc_body", email.body);
    }

    #[test]
    fn test_free_trial_ending_fired_without_credit_card() {
        let email = test_single_email(
            AlertType::FreeTrialEnding,
            "acmeCo/",
            json!({
                "recipients": [user_b()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "has_credit_card": false,
            }),
            State::Fired,
        );

        assert_email(
            &email,
            user_b(),
            EXPECT_IDEMPOTENCY_KEY_FIRED,
            "Estuary Flow: Paid Tier",
        );
        insta::assert_snapshot!("free_trial_ending_fired_without_cc_body", email.body);
    }

    #[test]
    fn test_free_trial_ending_resolved() {
        expect_no_email(
            AlertType::FreeTrialEnding,
            "acmeCo/",
            json!({
                "recipients": [user_b()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "has_credit_card": false,
            }),
            State::ResolvedWith(json!({
                "recipients": [user_b()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "has_credit_card": true,
            })),
        );
    }

    #[test]
    fn test_free_trial_stalled_fired() {
        let email = test_single_email(
            AlertType::FreeTrialStalled,
            "acmeCo/",
            json!({
                "recipients": [user_b()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
            }),
            State::Fired,
        );

        assert_email(
            &email,
            user_b(),
            EXPECT_IDEMPOTENCY_KEY_FIRED,
            "Free Tier Grace Period for acmeCo/: No CC 💳❌",
        );
        insta::assert_snapshot!("free_trial_stalled_fired_body", email.body);
    }

    #[test]
    fn test_free_trial_stalled_resolved() {
        let email = test_single_email(
            AlertType::FreeTrialStalled,
            "acmeCo/",
            json!({
                "recipients": [user_a()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
            }),
            State::Resolved,
        );

        assert_email(
            &email,
            user_a(),
            EXPECT_IDEMPOTENCY_KEY_RESOLVED,
            "Free Tier Grace Period for acmeCo/: CC Entered 💳✅",
        );
        insta::assert_snapshot!("free_trial_stalled_resolved_body", email.body);
    }

    #[test]
    fn test_missing_payment_method_fired() {
        expect_no_email(
            AlertType::MissingPaymentMethod,
            "acmeCo/",
            json!({
                "recipients": [user_b()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "plan_state": "free_tier",
            }),
            State::Fired,
        );
    }

    #[test]
    fn test_missing_payment_method_resolved_free_trial() {
        let email = test_single_email(
            AlertType::MissingPaymentMethod,
            "acmeCo/",
            json!({
                "recipients": [user_b()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "plan_state": "free_tier",
            }),
            State::ResolvedWith(json!({
                "recipients": [user_a()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "plan_state": "free_trial",
            })),
        );

        assert_email(
            &email,
            user_a(),
            EXPECT_IDEMPOTENCY_KEY_RESOLVED,
            "Estuary: Thanks for Adding a Payment Method🎉",
        );
        insta::assert_snapshot!(
            "missing_payment_method_resolved_free_trial_body",
            email.body
        );
    }

    #[test]
    fn test_missing_payment_method_resolved_paid() {
        let email = test_single_email(
            AlertType::MissingPaymentMethod,
            "acmeCo/",
            json!({
                "recipients": [user_a()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "plan_state": "free_tier",
            }),
            State::ResolvedWith(json!({
                "recipients": [user_b()],
                "tenant": "acmeCo/",
                "trial_start": "2024-01-15",
                "trial_end": "2024-02-14",
                "plan_state": "paid",
            })),
        );

        assert_email(
            &email,
            user_b(),
            EXPECT_IDEMPOTENCY_KEY_RESOLVED,
            "Estuary: Thanks for Adding a Payment Method🎉",
        );
        insta::assert_snapshot!("missing_payment_method_resolved_paid_body", email.body);
    }

    #[test]
    fn test_shard_failed_fired() {
        let email = test_single_email(
            AlertType::ShardFailed,
            "acmeCo/test/capture",
            json!({
                "recipients": [user_a()],
                "spec_type": "capture",
                "first_ts": "2024-01-15T10:00:00Z",
                "last_ts": "2024-01-15T12:00:00Z",
                "count": 5,
                "error": "THIS MUST BE IGNORED IN THE EMAIL",
            }),
            State::Fired,
        );

        assert_email(
            &email,
            user_a(),
            EXPECT_IDEMPOTENCY_KEY_FIRED,
            "Estuary Flow: Task failure detected for capture acmeCo/test/capture",
        );
        insta::assert_snapshot!("shard_failed_fired_body", email.body);
    }

    #[test]
    fn test_shard_failed_resolved() {
        let email = test_single_email(
            AlertType::ShardFailed,
            "acmeCo/test/capture",
            json!({
                "recipients": [user_b()],
                "spec_type": "capture",
                "first_ts": "2024-01-15T10:00:00Z",
                "last_ts": "2024-01-15T12:00:00Z",
                "count": 5,
                "error": "THIS MUST BE IGNORED IN THE EMAIL",
            }),
            State::Resolved,
        );

        assert_email(
            &email,
            user_b(),
            EXPECT_IDEMPOTENCY_KEY_RESOLVED,
            "Estuary Flow: Task failure resolved for capture acmeCo/test/capture",
        );
        insta::assert_snapshot!("shard_failed_resolved_body", email.body);
    }

    #[test]
    fn test_multiple_emails() {
        // Tests rendering multiple emails at a time, and asserts that we've generated correct idempotency keys.
        let renderer = Renderer::try_new("http://dashboard.estuary.test/".to_string()).unwrap();

        let fired_alert = make_alert(
            AlertType::ShardFailed,
            "acmeCo/test/materialization",
            json!({
                "recipients": [user_a(), user_b()],
                "spec_type": "materialization",
                "first_ts": "2024-01-15T10:00:00Z",
                "last_ts": "2024-01-15T12:00:00Z",
                "count": 6,
                "error": "THIS MUST BE IGNORED IN THE EMAIL",
            }),
            State::Fired,
        );

        let emails = renderer
            .render_emails(&fired_alert)
            .expect("failed to render emails");
        assert_eq!(2, emails.len());

        assert_eq!(&user_a(), &emails[0].recipient);
        assert_eq!("0102030405060708-fired-0", &emails[0].idempotency_key);

        assert_eq!(&user_b(), &emails[1].recipient);
        assert_eq!("0102030405060708-fired-1", &emails[1].idempotency_key);

        let resolved_alert = make_alert(
            AlertType::ShardFailed,
            "acmeCo/test/materialization",
            json!({
                "recipients": [user_a(), user_b()],
                "spec_type": "materialization",
                "first_ts": "2024-01-15T10:00:00Z",
                "last_ts": "2024-01-15T12:00:00Z",
                "count": 6,
                "error": "THIS MUST BE IGNORED IN THE EMAIL",
            }),
            // Switch the order of recipients
            State::ResolvedWith(json!({
                "recipients": [user_b(), user_a()],
                "spec_type": "materialization",
                "first_ts": "2024-01-15T10:00:00Z",
                "last_ts": "2024-01-15T12:00:00Z",
                "count": 6,
                "error": "THIS MUST BE IGNORED IN THE EMAIL",
            })),
        );

        let emails = renderer
            .render_emails(&resolved_alert)
            .expect("failed to render emails");
        assert_eq!(2, emails.len());

        assert_eq!(&user_b(), &emails[0].recipient);
        assert_eq!("0102030405060708-resolved-0", &emails[0].idempotency_key);

        assert_eq!(&user_a(), &emails[1].recipient);
        assert_eq!("0102030405060708-resolved-1", &emails[1].idempotency_key);
    }

    #[test]
    fn test_no_recipients() {
        expect_no_email(
            AlertType::AutoDiscoverFailed,
            "acmeCo/capture",
            json!({
                "recipients": [],
                "first_ts": "2024-01-15T10:00:00Z",
                "last_ts": "2024-01-15T12:00:00Z",
                "count": 6,
                "error": "THIS MUST BE IGNORED IN THE EMAIL",
            }),
            State::Fired,
        );
        expect_no_email(
            AlertType::AutoDiscoverFailed,
            "acmeCo/capture",
            json!({
                "first_ts": "2024-01-15T10:00:00Z",
                "last_ts": "2024-01-15T12:00:00Z",
                "count": 6,
                "error": "THIS MUST BE IGNORED IN THE EMAIL",
            }),
            State::Fired,
        );
    }
}
