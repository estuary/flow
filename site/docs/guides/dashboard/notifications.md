---
sidebar_position: 2
slug: /reference/notifications/
---

# Notifications

Estuary allows users to configure email notifications on specific tenants to send out alerts when a task hasn't received data within a time window and when billing information has been updated.

In the `Admin` section of the Flow Web Application, navigate to the the `Settings` tab. Here you will be able to input your email to receive notifications from your tenant.

## Data Movement Alerts

When navigating to the main view of a capture or a materialization, a user can select an interval for tracking zero data movement. Under the `Notification Settings` card, select a time interval from the dropdown labeled `Interval`. There is no need to save, but you must also have already configured notifications in order for the alert to take effect. If you are not yet subscribed to notifications, an info box will appear prompting you to set up a subscription by clicking on `CLICK HERE`.

If your task does not receive any new documents with the selected timeframe, an email will be sent to any email addresses that are subscribed to this tenant.

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
