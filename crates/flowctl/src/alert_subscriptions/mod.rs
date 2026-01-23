use crate::output::{self, JsonCell, to_table_row};
use itertools::Itertools;

use crate::graphql::*;
use anyhow::Context;
use clap::builder::{PossibleValuesParser, TypedValueParser};
use models::status::AlertType;

#[derive(Debug, clap::Args)]
pub struct List {
    #[clap(long)]
    pub prefix: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct SubscribeArgs {
    /// The email address you wish to subscribe. Provided as a plain address
    /// only, like `foo@example.test`. Defaults to the email address of the
    /// logged in user, if known.
    #[clap(long)]
    pub email: Option<String>,
    /// The catalog prefix of the alerts you wish to subscribe to. This can be
    /// the tenant prefix (e.g. `acmeCo/`) or something more specific like
    /// `acmeCo/prod/sources/`.
    ///
    /// This is optional only if the user has access to only a single catalog
    /// prefix, in which case we'll use that prefix by default.
    #[clap(long)]
    pub prefix: Option<String>,

    /// Add a subscription only for the specific alert type given by this
    /// argument. May be provided multiple times in order to subscribe to
    /// multiple alert types. If not provided, a comprehensive default list of
    /// alert types will be used.
    #[clap(long, value_parser =
    PossibleValuesParser::new(alert_type_values()).map(|s|
    AlertType::from_str(&s).unwrap()))]
    pub alert_type: Option<Vec<AlertType>>,

    #[clap(long)]
    pub detail: Option<String>,
}

fn alert_type_values() -> Vec<&'static str> {
    AlertType::all().into_iter().map(|ty| ty.name()).collect()
}

#[derive(Debug, clap::Args)]
pub struct UnsubscribeArgs {
    /// The catalog prefix of the alert subscription you wish to update.
    /// This is optional only if the user has access to only a single catalog
    /// prefix, in which case we'll use that prefix by default.
    #[clap(long)]
    pub prefix: Option<String>,
    /// The email address you wish to unsubscribe. Provided as a plain address
    /// only, like `foo@example.test`. Defaults to the email address of the
    /// logged in user, if known.
    #[clap(long)]
    pub email: Option<String>,
    /// Remove only the specified alert type, leaving subscriptions for other
    /// alert types in place. May be provided multiple times in order to
    /// unsubscribe from multiple alert types. If not provided, then all alert
    /// types will be unsubscribed.
    #[clap(long, value_parser = PossibleValuesParser::new(alert_type_values()).map(|s| AlertType::from_str(&s).unwrap()))]
    pub alert_type: Option<Vec<AlertType>>,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    /// Lists all the alert subscriptions under the given prefix
    List(List),
    /// Subscribe to start receiving alerts, or alter an existing subscription to change which alert types it applies to
    Subscribe(SubscribeArgs),
    /// Stop receiving alerts, either for a subset of alert types or everything
    Unsubscribe(UnsubscribeArgs),
}

#[derive(Debug, clap::Args)]
pub struct AlertSubscriptions {
    #[clap(subcommand)]
    cmd: Command,
}

impl AlertSubscriptions {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        match &self.cmd {
            Command::List(list) => do_list(list, ctx).await,
            Command::Subscribe(sub) => do_subscribe(sub, ctx).await,
            Command::Unsubscribe(unsub) => do_unsubscribe(unsub, ctx).await,
        }
    }
}

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/alert_subscriptions/list-query.graphql",
    response_derives = "Serialize,Clone",
    variables_derives = "Clone",
    extern_enums("AlertType")
)]
struct ListAlertSubscriptions;

async fn do_list(list: &List, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let prefix = if let Some(pre) = list.prefix.clone() {
        pre
    } else {
        let prefixes =
            crate::get_default_prefix_arguments(ctx, models::Capability::Read, 1).await?;
        prefixes
            .into_iter()
            .next()
            .expect("get_default_prefix_arguments must return non-empty")
    };

    let vars = list_alert_subscriptions::Variables {
        prefix: models::Prefix::new(prefix),
    };

    let resp = post_graphql::<ListAlertSubscriptions>(&ctx.client, vars)
        .await
        .context("failed to fetch alert subscriptions")?;
    ctx.write_all(resp.alert_subscriptions, ())
}

impl output::CliOutput for list_alert_subscriptions::SelectAlertSubscription {
    type TableAlt = ();
    type CellValue = JsonCell;

    fn table_headers(_: Self::TableAlt) -> Vec<&'static str> {
        vec![
            "Catalog Prefix",
            "Deliver To",
            "Alert Types",
            "Created At",
            "Updated At",
            "Detail",
        ]
    }

    fn into_table_row(self, _: Self::TableAlt) -> Vec<Self::CellValue> {
        to_table_row(
            self,
            &[
                "/catalogPrefix",
                "/email",
                "/alertTypes",
                "/createdAt",
                "/updatedAt",
                "/detail",
            ],
        )
    }
}

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/alert_subscriptions/create-mutation.graphql",
    response_derives = "Serialize,Clone",
    variables_derives = "Clone",
    extern_enums("AlertType")
)]
struct CreateAlertSubscription;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/alert_subscriptions/update-mutation.graphql",
    response_derives = "Serialize,Clone,Debug",
    variables_derives = "Clone",
    extern_enums("AlertType")
)]
struct UpdateAlertSubscription;

async fn do_subscribe(create: &SubscribeArgs, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let auth_claims = ctx.require_control_claims()?;

    let email = create.email.clone().or(auth_claims.email.clone()).ok_or_else(||
        anyhow::anyhow!("no explicit email argument provided, and unable to determine the user email from the available authentication information. Please try logging in again, or pass an explicit email argument."))?;

    let prefix = if let Some(pre) = create.prefix.clone() {
        pre
    } else {
        let prefix_vec =
            crate::get_default_prefix_arguments(ctx, models::Capability::Read, 1).await?;
        prefix_vec
            .into_iter()
            .next()
            .expect("get_default_prefix_arguments must return non-empty list")
    };
    let detail = create.detail.clone().unwrap_or_else(|| {
        let user = auth_claims
            .email
            .clone()
            .unwrap_or_else(|| auth_claims.sub.to_string());
        format!("subscribed via flowctl by user {user}")
    });
    let alert_types_arg = create.alert_type.as_ref().map(|a| a.clone());

    // Fetch the existing alert subscriptions and see whether there's already a subsription for this email
    // and prefix combination.
    if let Some(mut existing) = get_existing_subscription(ctx, &prefix, &email).await? {
        // There's already a subscription, so let's see whether we need to update it.
        let mut desired_alert_types = create
            .alert_type
            .as_ref()
            .map(|tys| tys.clone())
            .unwrap_or_else(|| existing.alert_types.clone());
        desired_alert_types.extend_from_slice(&existing.alert_types);
        desired_alert_types.sort();
        desired_alert_types.dedup();

        existing.alert_types.sort();
        let update_types = desired_alert_types != existing.alert_types;
        let update_detail = create.detail.is_some() && create.detail != existing.detail;

        if update_types || update_detail {
            tracing::info!(%prefix, %email, ?desired_alert_types, current_alert_types = ?existing.alert_types, "will update existing alert subscription");

            let vars = update_alert_subscription::Variables {
                prefix: models::Prefix::new(&prefix),
                email: email.clone(),
                alert_types: Some(desired_alert_types.clone()),
                detail: Some(detail),
            };
            let resp = post_graphql::<UpdateAlertSubscription>(&ctx.client, vars).await?;

            println!(
                "updated alert subscription for catalog prefix: {prefix}, email: {email}, alert types: [{}]",
                resp.update_alert_subscription.alert_types.iter().join(", ")
            );
        } else {
            println!(
                "You are already subscribed to the following alert types for the prefix '{prefix}':\n[{}]",
                existing.alert_types.iter().join(", ")
            );
            return Ok(());
        }
    } else {
        let vars = create_alert_subscription::Variables {
            prefix: models::Prefix::new(&prefix),
            email: email.clone(),
            alert_types: alert_types_arg,
            detail: Some(detail),
        };

        tracing::debug!(%email, %prefix, "creating alert subscription");
        let resp = post_graphql::<CreateAlertSubscription>(&ctx.client, vars).await?;

        println!(
            "created alert subscription for catalog prefix: {prefix}, email: {email}, alert types: [{}]",
            resp.create_alert_subscription.alert_types.iter().join(", ")
        );
    }
    Ok(())
}

async fn get_existing_subscription(
    ctx: &mut crate::CliContext,
    prefix: &str,
    email: &str,
) -> anyhow::Result<Option<list_alert_subscriptions::SelectAlertSubscription>> {
    let list_vars = list_alert_subscriptions::Variables {
        prefix: models::Prefix::new(prefix),
    };
    let list_resp = post_graphql::<ListAlertSubscriptions>(&ctx.client, list_vars)
        .await
        .context("failed to fetch alert subscriptions")?;

    let sub = list_resp
        .alert_subscriptions
        .into_iter()
        .find(|sub| sub.catalog_prefix.as_str() == prefix && sub.email.as_deref() == Some(email));
    Ok(sub)
}

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/alert_subscriptions/delete-mutation.graphql",
    response_derives = "Serialize,Clone,Debug",
    variables_derives = "Clone",
    extern_enums("AlertType")
)]
struct DeleteSubscription;

async fn do_unsubscribe(args: &UnsubscribeArgs, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let email = resolve_email(&args.email, ctx)?;
    let prefix = if let Some(pre) = args.prefix.clone() {
        pre
    } else {
        let prefix_vec =
            crate::get_default_prefix_arguments(ctx, models::Capability::Read, 1).await?;
        prefix_vec
            .into_iter()
            .next()
            .expect("get_default_prefix_arguments must return non-empty list")
    };

    let Some(existing) = get_existing_subscription(ctx, &prefix, &email).await? else {
        anyhow::bail!(
            "no subscription exists for catalog prefix '{prefix}' and email '{email}'. Run `flowctl alert-subscriptions list` to see the current subscriptions"
        );
    };

    // Determine the desired alert types by removing all the `--alert-types`
    // values from the existing set of subscribed types. An empty vec means
    // we should remove the entire subscription.
    let desired_alert_types: Vec<AlertType> = if let Some(remove_types) = &args.alert_type {
        let mut desired = Vec::new();
        for current_type in existing.alert_types.iter().copied() {
            if !remove_types.contains(&current_type) {
                desired.push(current_type);
            }
        }

        // User provided --alert-types, but they were already unsubscribed to the given types
        if desired.len() == existing.alert_types.len() {
            println!(
                "existing subscription for prefix '{prefix}' and email '{email}' already excludes alert types: [{}]",
                remove_types.iter().join(", ")
            );
            return Ok(());
        }
        desired
    } else {
        Vec::new()
    };

    // Do we need to update the existing subscription, or delete it altogether?
    if desired_alert_types.is_empty() {
        // Delete the entire subscription, since there are not alert types desired
        let vars = delete_subscription::Variables {
            prefix: models::Prefix::new(&prefix),
            email: email.clone(),
        };
        let resp = post_graphql::<DeleteSubscription>(&ctx.client, vars).await?;
        tracing::debug!(?resp, "deleted alert subscription");

        println!(
            "successfully removed alert subscription for prefix: {prefix} and email: {email}\n\
            created at: {}, alert types: [{}], detail: '{}'",
            existing.created_at,
            existing.alert_types.iter().join(", "),
            existing.detail.as_deref().unwrap_or("<none>")
        );
    } else {
        // Update the subscription to set the desired alert types
        let vars = update_alert_subscription::Variables {
            prefix: models::Prefix::new(prefix.clone()),
            email: email.clone(),
            alert_types: Some(desired_alert_types),
            detail: None, // don't update the detail
        };
        let resp = post_graphql::<UpdateAlertSubscription>(&ctx.client, vars).await?;
        tracing::debug!(?resp, "updated alert subscription");
        let remaining_types = resp.update_alert_subscription.alert_types.iter().join(", ");
        let removed_types = args
            .alert_type
            .as_ref()
            .map(|val| val.iter().join(", "))
            .unwrap_or_default();
        println!(
            "successfully unsubscribed from alert types: {removed_types} for prefix: {prefix} and email: {email}\nstill subscribed to: [{remaining_types}]"
        );
    }

    Ok(())
}

fn resolve_email(
    email_arg: &Option<String>,
    ctx: &mut crate::CliContext,
) -> anyhow::Result<String> {
    let auth_claims = ctx.require_control_claims()?;
    email_arg.clone().or(auth_claims.email.clone()).ok_or_else(||
        anyhow::anyhow!("no explicit email argument provided, and unable to determine the user email from the available authentication information. Please try logging in again, or pass an explicit email argument."))
}
