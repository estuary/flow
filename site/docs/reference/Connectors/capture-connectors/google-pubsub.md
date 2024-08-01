
# Google Cloud Pub/Sub

This connector captures messages in JSON format into Flow collections from
Google Cloud Pub/Sub topics.

During setup, this connect will discover all topics it has access to. Each
[capture binding](../../../concepts/README.md#resources-and-bindings) that is
enabled for a topic will automatically create a new subscription, and the
connector will read messages from that subscription.

## Prerequisites

To use this connector, you will need the following prerequisites:

* A Google Cloud Project with Pub/Sub enabled
* A Google Cloud Service Account with the "Pub/Sub Editor" roles in your GCP project
* A Service Account Key to authenticate into your Service Account

See the setup guide for more information about how to create the required resources.

### Service Account

To sync data from Pub/Sub, you need credentials for a Service Account with the
"Pub/Sub Editor" role.  This role grants the necessary permissions to discover
topics, create subscriptions to those topics, and read messages from the
subscriptions. It is recommended to create a dedicated Service Account to
facilitate permission management and auditing. However, if you already have a
Service Account with the correct permissions, you can use it.

Here's how to provision a suitable service account:

1. Follow Google Cloud Platform's instructions for [Creating a Service
   Account](https://cloud.google.com/iam/docs/service-accounts-create#creating).
2. Note down the ID of the service account you just created. Service Account IDs
   typically follow the format
   `<account-name>@<project-name>.iam.gserviceaccount.com`.
3. Follow Google Cloud Platform's instructions for [Granting IAM
   Roles](https://cloud.google.com/iam/docs/grant-role-console#grant_an_iam_role)
   to the new service account. The "principal" email address should be the ID of
   the service account you just created, and the role granted should be "Pub/Sub
   Editor".

### Service Account Key

Service Account Keys are used to authenticate as Google Service Accounts. To be
able to utilize the permissions granted to the Service Account in the previous
step, you'll need to provide its Service Account Key when creating the capture.
It is a good practice, though not required, to create a new key for Flow even if
you're reusing a preexisting account.

To create a new key for a service account, follow Google Cloud Platform's
instructions for [Creating a Service Account
Key](https://cloud.google.com/iam/docs/keys-create-delete#creating). Be sure to
create the key in JSON format. Once the linked instructions have been followed
you should have a key file, which will need to be uploaded to Flow when setting
up your capture.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the
catalog specification file. See
[connectors](../../../concepts/connectors.md#using-connectors) to learn more
about using connectors.

### Properties

#### Endpoint

| Property                | Title                | Description                                                              | Type   | Required/Default |
|-------------------------|----------------------|--------------------------------------------------------------------------|--------|------------------|
| **`/projectId`**        | Project ID           | Google Cloud Project ID that contains the PubSub topics.                 | string | Required         |
| **`/credentialsJson`**  | Service Account JSON | Google Cloud Service Account JSON credentials to use for authentication. | string | Required         |
| `/subscriptionPrefix`   | Subscription Prefix  | Prefix to prepend to the PubSub topics subscription names.               | string |                  |

#### Bindings

| Property     | Title | Description                               | Type   | Required/Default |
|--------------|-------|-------------------------------------------|--------|------------------|
| **`/topic`** | Topic | Name of the PubSub topic to subscribe to. | string | Required         |

### At-Least-Once Message Capture

Received messages are acknowledged to Pub/Sub after they have been durably
committed to your Flow collection. After Pub/Sub receives acknowledgement of
messages, it will not attempt to re-deliver messages to subscribers if
subscriptions are created with "exactly-once delivery", which this connector
does set when it creates subscriptions. Occasionally messages will be captured
to your Flow collection more than once if the connector is restarted after it
has durably committed the document to the collection but before it has
acknowledged the message to Pub/Sub.

In this way the committing of the message to your Flow collection is considered
a "side effect" of processing the message, and [Pub/Sub does not provide
guarantees around exactly-once side
effects](https://cloud.google.com/blog/products/data-analytics/cloud-pub-sub-exactly-once-delivery-feature-is-now-ga).

If you materialize the collections using standard updates, duplicate documents
will automatically be de-duplicated in the destination based on the ID of the
documents. Materializations that use [delta
updates](../../../concepts/materialization.md#delta-updates) need to consider
the potential for more than one document with the same ID.

### Message Format

Currently only messages with data in JSON format can be processed. Data in other
formats will cause errors with the capture connector. Support for other formats
is planned - reach out to support@estuary.dev if your use case requires
processing data in a different format.