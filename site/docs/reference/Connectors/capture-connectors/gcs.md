---
sidebar_position: 4
---
# Google Cloud Storage

This connector captures data from a Google Cloud Storage (GCS) bucket.

[`ghcr.io/estuary/source-gcs:dev`](https://ghcr.io/estuary/source-gcs:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, either your GCS bucket must be public, or you must have access via a Google service account.

* For public buckets, verify that objects in the bucket are [publicly readable](https://cloud.google.com/storage/docs/access-control/making-data-public).
* For buckets accessed by a Google Service Account:
    * Ensure that the user has been assigned a [role](https://cloud.google.com/iam/docs/understanding-roles) with read access.
    * Create a [JSON service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating). Google's [Application Default Credentials](https://cloud.google.com/docs/authentication/production) will use this file for authentication.

## Configuration

There are various ways to configure connectors. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about these methods. The values and YAML sample in this section provide configuration details specific to the GCS source connector.

:::tip
You might use [prefixes](https://cloud.google.com/storage/docs/samples/storage-list-files-with-prefix) to organize your GCS bucket
in a way that emulates a directory structure.
This connector can use prefixes in two ways: first, to perform the [**discovery**](../../../concepts/connectors.md#flowctl-discover) phase of setup, and later, when the capture is running.

* You can specify a prefix in the endpoint configuration to limit the overall scope of data discovery.
* You're required to specify prefixes on a per-binding basis. This allows you to map each prefix to a distinct Flow collection,
and informs how the capture will behave in production.

To capture the entire bucket, omit `prefix` in the endpoint configuration and set `stream` to the name of the bucket.
:::

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/ascendingKeys` | Ascending keys | Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering. | boolean | `false` |
| **`/bucket`** | Bucket | Name of the Google Cloud Storage bucket | string | Required |
| `/googleCredentials` | Google service account | Service account JSON file. Required unless the bucket is public.| object |  |
| `/matchKeys` | Match Keys | Filter applied to all object keys under the prefix. If provided, only objects whose key (relative to the prefix) matches this regex will be read. For example, you can use &quot;.&#x2A;&#x5C;.json&quot; to only capture json files. | string |  |
| `/prefix` | Prefix | Prefix within the bucket to capture from. | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Prefix | Path to dataset in the bucket, formatted as `bucket-name/prefix-name` | string | Required |
| **`/syncMode`** | Sync mode | Connection method. Always set to `incremental`. | string | Required |

### Sample

A minimal capture definition will look like the following:

```yaml
captures:
  ${TENANT}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-gcs:dev
        config:
          bucket: my-bucket
    bindings:
      - resource:
          stream: my-bucket/${PREFIX}
          syncMode: incremental
        target: ${TENANT}/${COLLECTION_NAME}

```

Your capture definition may be more complex, with additional bindings for different GCS prefixes within the same bucket.

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures)

### Configure Google service account impersonation

As part of your Google IAM management, you may have configured one service account to [impersonate another service account](https://cloud.google.com/iam/docs/impersonating-service-accounts).
This is useful when you want to easily control multiple service accounts with only one set of keys.

You can configure this authorization model for a GCS capture in Flow using the GitOps workflow.
To do so, you'll enable sops encryption and impersonate the target account with JSON credentials.

Before you begin, make sure you're familiar with [how to encrypt credentials in Flow using sops](./../../../concepts/connectors.md#protecting-secrets).

Use the following sample as a guide to add the credentials JSON to the capture's endpoint configuration.
The sample uses the [encrypted suffix feature](../../../concepts/connectors.md#example-protect-portions-of-a-configuration) of sops to encrypt only the sensitive credentials, but you may choose to encrypt the entire configuration.

```yaml
config:
  bucket: <bucket-name>
  googleCredentials_sops:
    # URL containing the account to impersonate and the associated project
    service_account_impersonation_url: https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/<target-account>@<project>.iam.gserviceaccount.com:generateAccessToken
    # Credentials for the account that has been configured to impersonate the target
    source_credentials:
        client_email: <origin-account>@<anotherproject>.iam.gserviceaccount.com
        client_id: ...
        private_key: ...
        private_key_id: ...
        project_id: ...
        token_uri: https://oauth2.googleapis.com/token
        type: service_account
    type: impersonated_service_account
```
