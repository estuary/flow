---
sidebar_position: 12
---

# Mailchimp

This connector captures data from a Mailchimp account.

Three data resources are supported, each of which is mapped to a Flow collection: lists, campaigns, and email activity.

[`ghcr.io/estuary/airbyte-source-mailchimp:dev`](https://ghcr.io/estuary/airbyte-source-mailchimp:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.
You can find their documentation [here](https://docs.airbyte.com/integrations/sources/mailchimp),
but keep in mind that the two versions may be significantly different.

## Prerequisites

There are two ways to authenticate with MailChimp when capturing data: using OAuth, and with an API key.
Their prerequisites differ.

OAuth is recommended for simplicity in the Flow web app;
the API key method is the only supported method using the command line.

### Prerequisites for OAuth

:::caution Beta
OAuth implementation is under active development and is coming soon.
Use the API key method for now.
:::

* A Mailchimp account

### Prerequisites using an API key

* A Mailchimp account

* A Mailchimp [API key](https://mailchimp.com/developer/marketing/guides/quick-start/#generate-your-api-key)

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog spec YAML.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and YAML sample below provide configuration details specific to the Mailchimp source connector.

### Properties

#### Endpoint

The following properties reflect the API Key authentication method.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Authentication  | Authentication Type and Details | object | Required |
| **`/credentials/auth_type`** | Authentication Type | Authentication type. Set to `apikey`.  | string | Required |
| **`/credentials/apikey`** | API Key | Your Mailchimp API key | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Resource | Mailchimp `lists`, `campaigns`, or `email_activity` | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. Always set to `incremental`. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/airbyte-source-mailchimp:dev
          config:
            credentials:
              auth_type: apikey
              apikey: <secret>
      bindings:
        - resource:
            stream: lists
            syncMode: incremental
          target: ${PREFIX}/${COLLECTION_NAME}

        - resource:
            stream: campaigns
            syncMode: incremental
          target: ${PREFIX}/${COLLECTION_NAME}

        - resource:
            stream: email_activity
            syncMode: incremental
          target: ${PREFIX}/${COLLECTION_NAME}
```

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures)