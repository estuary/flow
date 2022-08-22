---
sidebar_position: 10
---

# Google Firestore

This connector captures data from your Google Firestore collections into Flow collections.

[`ghcr.io/estuary/source-firestore:dev`](https://ghcr.io/estuary/source-firestore:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

* A Google service account with:

    * Read access to your Firestore data, via [roles/datastore.viewer](https://cloud.google.com/datastore/docs/access/iam).
    You can assign this role when you [create the service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts#creating), or [add it to an existing service account](https://cloud.google.com/iam/docs/granting-changing-revoking-access#single-role).

    * A generated [JSON service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating) for the account.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the Flow specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Firestore source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/googleCredentials`** | Credentials | Google Cloud Service Account JSON credentials. | string | Required |
| **`/scan_interval`** | Scan Interval | How frequently to scan all collections to ensure consistency. [See supported values](https://pkg.go.dev/time#ParseDuration). To turn off scans use the value &#x27;never&#x27;. | string | Required, `"12h"` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Firestore collection from which a Flow collection is captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-firestore:dev
        config:
          googleCredentials:
            "type": "service_account",
            "project_id": "project-id",
            "private_key_id": "key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n",
            "client_email": "service-account-email",
            "client_id": "client-id",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://accounts.google.com/o/oauth2/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/service-account-email"
          scan_interval: "24h"
    bindings:
      - resource:
          stream: my_firestore_collection
          syncMode: incremental
        target: ${PREFIX}/my_firestore_collection
```