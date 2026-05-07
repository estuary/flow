---
sidebar_position: 2
slug: /reference/notifications/
---

# Notifications

Estuary lets you configure email notifications to send out alerts for various events on your tenant.

To configure alert subscriptions:

* Navigate to the **Admin** section of Estuary's dashboard
* Select the [**Settings**](https://dashboard.estuary.dev/admin/settings) tab
* If you have access to more than one prefix, select your desired tenant from the **Prefix** dropdown
* Under the **Organization Notifications** section, click **Configure Notifications** to create a new subscription or **Edit** an existing one
* Enter your desired prefix, email, and [alert types](#alert-types)
* Save your alert subscription

You can create multiple alert subscriptions with different configurations to subscribe additional emails or direct specific alert types to certain addresses.

:::tip
Use a mailing list email rather than an individual's email for your alert subscriptions.
This helps provide team-wide visibility and avoids disruptions when individuals are unavailable.

Alternatively, you can use an email tied to [Slack](#send-alerts-to-slack) to send notifications to a Slack channel.
:::

Each task also displays its active and historical notifications within the dashboard.
From the capture, collection, or materialization details overview page, select the **Alerts** tab.
Active and historical notifications include the type of alert, when it was fired, any configured recipients, and alert details.

## Alert Types

Alerts are broken out into different categories. These categories cover different failure modes, unexpected behavior, and warnings.

One email may subscribe to multiple alert types.

### Data Movement Alerts

A user can select an interval for tracking zero data movement for a specific capture or materialization.

On the capture or materialization details page, select the **Alerts** tab. Under the **Notification Settings** card, select a time interval from the **Interval** dropdown. You must have already configured notifications in order for the alert to take effect. If you are not yet subscribed to notifications, an info box will appear prompting you to set up a subscription by clicking on `CLICK HERE`.

If your task does not receive any new documents with the selected timeframe, an email will be sent to any email addresses that are subscribed to the "Data Movement Stalled" alert type.

### Auto-Discover Alerts

If schema evolution features are turned on for a capture, Estuary periodically attempts to auto-discover any updates or new data resources for that capture.
If these discovers fail, such as when unable to authenticate with the source system, email addresses subscribed to this alert will be notified.

Learn more about [auto-discovery](/concepts/captures/#automatically-update-captures).

### Task Failure Alerts

[Task](/concepts/#tasks) failures can occur for a number of reasons, and are often related to issues with a capture or materialization's configuration.
For example, a task might fail when it encounters data with schema violations or when permissions have changed.

The connector will attempt to auto-recover before firing an alert.
While this helps reduce noise for task failures, you may still encounter false positives, where the task briefly encountered errors and then was able to recover in the next auto-discovery window.
You can check the current status of the connector in your dashboard.

If the task keeps failing, the alert type may progress from "Task Failed" to "Task Chronically Failing."
If the task remains in this chronically failing state and is unable to progress, the task may be disabled ("Task Auto-Disabled (Failing)") until you can address the root cause of the failure.

Additional details about the failure will be available in the connector's **Alerts** tab.

### Background Publication Failed Alerts

Triggers when an automated background process needs to publish a spec, but is unable to because of publication errors. Background publications are performed on all specs for a variety of reasons. For example, updating inferred schemas, or updating materialization bindings to match the source capture. When these publications fail, tasks are likely to stop functioning correctly until the issue can be addressed.

There are many different reasons why publications might fail, but some common scenarios are:

- A network error or misconfiguration between the connector and your source or destination system
- The credentials for connecting to a source or destination system have been changed or revoked
- A materialization requires a specific column, which no longer appears in the source collection schema
- There was an incompatible change to the data type of a particular field (like `string -> boolean`) and the materialization has `onIncompatibleSchemaChange: abort`

A recommended troubleshooting step is to try to publish the spec yourself, resolving any validation errors you encounter along the way. Once you're able to publish the spec, our background automation should be able to as well.

### Idle Task Alerts

"Task Idle" alerts trigger when **both** of the following are true:

* The task has not processed any data for an extended time period
* The task has not been modified recently

If the task remains in this idle state, the task may be disabled ("Task Auto-Disabled (Idle)").
You may publish a new version of the task to keep it from being disabled or re-enable the task when you want to use it again.

### Billing Alerts

All emails in the **Organization Notifications** table are automatically subscribed to billing alerts. Alerts are sent out for the following events:

* **Free Trial**: A tenant has started the free trial
* **Free Trial Ending**: Five days remain in a tenant's free trial
* **Free Trial Stalled**: A tenant's free trial has ended and no payment method has been added
* **Missing Payment Method**: No payment method is on file for a tenant

## Properties
| Property | Title | Description | Type |
|---|---|---|---|
| **`/catalogPrefix`**| Prefix | Subscribe to notifications for this tenant| string |
| **`/email`** | Email  | Alert the following email with all notifications | string |

## Send Alerts to Slack

You can send alert notifications to Slack via email.

Slack provides several methods to [send emails to Slack](https://slack.com/intl/en-au/help/articles/206819278-Send-emails-to-Slack), including creating a dedicated email address or using an add-on.
Depending on the method, you can route emails to a specific channel or conversation.

Whichever method you use, configure the associated email address as an Estuary notification recipient to receive alerts in Slack.

## Set Up Alerts in External Platforms

Beyond Estuary's built-in alerting capabilities, you can also configure your own alerts in platforms like Datadog and Grafana.
See Estuary's [OpenMetrics API](/reference/openmetrics-api) for available metrics and connection instructions.
