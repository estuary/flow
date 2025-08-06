
# Google Play

This connector captures data from [monthly Google Play reports](https://support.google.com/googleplay/android-developer/answer/6135870#zippy=%2Cdownload-reports-using-a-client-library-and-service-account%2Csee-an-example-python) into Flow collections.

It’s available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-google-play:dev`](https://ghcr.io/estuary/source-google-play:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported:

* Crashes
* Installs
* Reviews


## Prerequisites

To use this connector, you'll need:

* A Google Play account with Account Owner or Admin permissions.
* A service account.

## Authentication

A service account key is required to authenticate the connector. Please refer to Google's [documentation](https://cloud.google.com/iam/docs/keys-create-delete#creating) on how to set up a service account. Inside the Google Play Console, ensure your service account's email address is invited and has the following permissions:
- View app information and download bulk reports (read-only)
- View financial data, order, and cancellation survey responses

:::info
Your Google Play account must have Account Owner or Admin permissions.

:::

## Configuration

You configure connectors either in the Flow web app, or by directly editing a specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Google Play source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/bucket`** | Start Date | The bucket containing your Google Play data. The bucket starts with pubsite_prod. For example, pubsite_prod_some_identifier. | string | Required |
| **`/credentials`** | Credentials | Credentials for the service | object |  |
| **`/credentials/credentials_title`** | Authentication Method | Set to `Google Service Account`. | string | Required |
| **`/credentials/service_account`** | Service Account JSON key | The service account key. | string | Required |
| `/start_date` | Start Date | The date from which you&#x27;d like to replicate data, in the format YYYY-MM-DDT00:00:00Z. All data updated after this date will be replicated. | string | Defaults to 30 days before the present |

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
        image: ghcr.io/estuary/source-google-play:dev
          config:
            bucket: pubsite_prod_0123456_my_bucket
            credentials:
                credentials_title: Google Service Account
                service_account: <secret>
            start_date: "2025-08-05T00:00:00Z"
      bindings:
        - resource:
            name: crashes
            interval: PT1H
          target: ${PREFIX}/crashes
```
