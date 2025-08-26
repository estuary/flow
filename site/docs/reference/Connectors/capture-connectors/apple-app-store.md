
# Apple App Store

This connector captures data from [Apple App Store Connect API](https://developer.apple.com/documentation/appstoreconnectapi) into Flow collections.

It’s available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-apple-app-store:dev`](https://ghcr.io/estuary/source-apple-app-store:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported:

* [Customer Reviews for an App](https://developer.apple.com/documentation/appstoreconnectapi/get-v1-apps-_id_-customerreviews)
* [Analytics Reports](https://developer.apple.com/documentation/appstoreconnectapi/analytics)
  * [App Sessions Detailed Report](https://developer.apple.com/documentation/analytics-reports/app-sessions)
  * [App Crashes Report](https://developer.apple.com/documentation/analytics-reports/app-crashes)
  * [App Store Installations and Deletions Detailed Report](https://developer.apple.com/documentation/analytics-reports/app-installs)
  * [App Store Discovery and Engagement Detailed Report](https://developer.apple.com/documentation/analytics-reports/app-store-discovery-and-engagement)
  * [App Store Downloads Detailed Report](https://developer.apple.com/documentation/analytics-reports/app-download)


## Prerequisites

To use this connector, you'll need:

* An Apple Developer account with access to App Store Connect.
* App Store Connect API access approved by your Account Holder.
* An App Store Connect API key with Admin permissions (required for Analytics Reports).

## Authentication

An App Store Connect API key is required to authenticate the connector. The connector uses JWT (JSON Web Token) authentication with your private API key. To create an API key:

1. Sign in to [App Store Connect](https://appstoreconnect.apple.com/)
2. Navigate to Users and Access > Integrations
3. Create a new API key with **Admin** permissions (required for Analytics Reports)
4. Download the private key (.p8 file) - this can only be done once
5. Note your Key ID and Issuer ID

Please refer to Apple's [official documentation](https://developer.apple.com/documentation/appstoreconnectapi/creating-api-keys-for-app-store-connect-api) for detailed steps on creating API keys.

:::info
Admin-level permissions are required to access Analytics Reports. Only Account Holders can initially request API access.

:::

## Configuration

You configure connectors either in the Flow web app, or by directly editing a specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Apple App Store source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Credentials | Credentials for the Apple App Store Connect API | object | Required |
| **`/credentials/credentials_title`** | Authentication Method | Set to `Private App Credentials`. | string | Required |
| **`/credentials/key_id`** | Key ID | The Key ID for your App Store Connect API key. | string | Required |
| **`/credentials/issuer_id`** | Issuer ID | The Issuer ID from your App Store Connect account. | string | Required |
| **`/credentials/private_key`** | Private Key | The content of your App Store Connect API private key (.p8 file). | string | Required |
| `/app_ids` | App IDs | List of App IDs to capture data for. Leave empty to capture data for all apps. | array | Defaults to empty array |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string |    PT1H      |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-apple-app-store:dev
        config:
          credentials:
            credentials_title: "Private App Credentials"
            key_id: ABC123DEF4
            issuer_id: 12345678-1234-1234-1234-123456789012
            private_key: |
              -----BEGIN PRIVATE KEY-----
              PLACEHOLDER_PRIVATE_KEY_CONTENT_GOES_HERE
              REPLACE_WITH_ACTUAL_P8_FILE_CONTENTS
              -----END PRIVATE KEY-----
          app_ids: []
    bindings:
      - resource:
          name: app_reviews
          interval: PT1H
        target: ${PREFIX}/app_reviews
```
