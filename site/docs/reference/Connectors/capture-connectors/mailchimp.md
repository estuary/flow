
# Mailchimp

This connector captures data from a Mailchimp account.

Three data resources are supported, each of which is mapped to a Flow collection: lists, campaigns, and email activity.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-mailchimp:dev`](https://ghcr.io/estuary/source-mailchimp:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Prerequisites

There are two ways to authenticate with MailChimp when capturing data: using OAuth2, and manually, with an API key.
Their prerequisites differ.

OAuth is recommended for simplicity in the Flow web app;
the API key method is the only supported method using the command line.

### Using OAuth2 to authenticate with Mailchimp in the Flow web app

* A Mailchimp account

### Configuring the connector specification manually

* A Mailchimp account

* A Mailchimp [API key](https://mailchimp.com/developer/marketing/guides/quick-start/#generate-your-api-key)

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Mailchimp source connector.

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
        image: ghcr.io/estuary/source-mailchimp:dev
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

[Learn more about capture definitions.](../../../concepts/captures.md)
