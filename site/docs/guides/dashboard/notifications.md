---
sidebar_position: 2
slug: /reference/notifications/
---

# Notifications

Estuary allows users to configure email notifications on specific tenants to send out alerts in various categories.

In the `Admin` section of Estuary's Web Application, navigate to the the `Settings` tab. Here you will be able to input your email to receive notifications from your tenant.

You can also see active and historical notifications for a specific connector.
From the capture or materialization details overview page, select the `Alerts` tab.
Active and historical notifications include the type of alert, when it was fired, and alert details.

## Data Movement Alerts

A user can select an interval for tracking zero data movement for a specific capture or materialization. On the capture or materialization details page, select the `Alerts` tab. Under the `Notification Settings` card, select a time interval from the dropdown labeled `Interval`. There is no need to save, but you must also have already configured notifications in order for the alert to take effect. If you are not yet subscribed to notifications, an info box will appear prompting you to set up a subscription by clicking on `CLICK HERE`.

If your task does not receive any new documents with the selected timeframe, an email will be sent to any email addresses that are subscribed to this tenant.

## Auto-Discover Alerts

If schema evolution features are turned on for a capture, Estuary periodically attempts to auto-discover any updates or new data resources for that capture.
If these discovers fail, such as when unable to authenticate with the source system, email addresses subscribed to this alert will be notified.

Learn more about [auto-discovery](/concepts/captures/#automatically-update-captures).

## Task Failure Alerts

[Task](/concepts/#tasks) failures can occur for a number of reasons, and are often related to issues with a capture or materialization's configuration.
For example, a task might fail when it encounters data with schema violations or when permissions have changed.

The connector will attempt to auto-recover before firing an alert.
While this helps reduce noise for task failures, you may still encounter false positives, where the task briefly encountered errors and then was able to recover in the next auto-discovery window.
You can check the current status of the connector in your dashboard.

Additional details about the failure will be available in the connector's `Alerts` tab.

## Background Publication Failed Alerts

Triggers when an automated background process needs to publish a spec, but is unable to because of publication errors. Background publications are peformed on all specs for a variety of reasons. For example, updating inferred schemas, or updating materialization bindings to match the source capture. When these publications fail, tasks are likely to stop functioning correctly until the issue can be addressed.

A recommended troubleshooting step is to try to publish the spec yourself, resolving any validation errors you encounter along the way. Once you're able to publish the spec, our background automation should be able to as well.

## Billing Alerts

Billing alerts are automatically subscribed to when a user inputs their email into the `Organization Notifications` table. Alerts will be sent out for the following events:

* **Free Tier Started**: A tenant has transitioned into the free trial
* **Free Trial Ending**: Five days are remaining in a tenant's free trial
* **Free Trial Ended**: A tenant's free trial has ended
* **Provided Payment Method**: A valid payment method has been provided for a tenant

## Properties
| Property | Title | Description | Type |
|---|---|---|---|
| **`/catalogPrefix`**| Prefix | Subscribe to notifications for this tenant| string |
| **`/email`** | Email  | Alert the following email with all notifications | string |
