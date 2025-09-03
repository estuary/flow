
# Google Cloud Pub/Sub

This connector materializes Flow collections into topics in Google Cloud Pub/Sub.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/materialize-google-pubsub:dev`](https://ghcr.io/estuary/materialize-google-pubsub:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* A [Google Cloud project](https://cloud.google.com/resource-manager/docs/creating-managing-projects#creating_a_project) with the Google Pub/Sub API [enabled](https://support.google.com/googleapi/answer/6158841?hl=en).
* Access to the project. Different items are required to configure access [using OAuth in the Flow web app](#oauth-authentication-using-the-flow-web-app) (recommended),
and [configuring manually](#manual-authentication).
* At least one Flow collection to materialize.

:::tip
If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.
:::

### OAuth authentication using the Flow web app

OAuth is the simplest authentication method, and is supported in the Flow web app. You'll need:

* A Google account with the role [`roles/pubsub.editor`](https://cloud.google.com/pubsub/docs/access-control#roles)
or equivalent for the Google Cloud project.
See the [Google IAM documentation](https://cloud.google.com/iam/docs/granting-changing-revoking-access#grant-single-role) to learn about granting roles.

You'll supply this account's username and password to authenticate.

### Manual authentication

Manual authentication is the only method supported when using flowctl to develop locally. You'll need:

* A Google service account with the role [`roles/pubsub.editor`](https://cloud.google.com/pubsub/docs/access-control#roles)
or equivalent for the Google Cloud project.
See the [Google IAM documentation](https://cloud.google.com/iam/docs/granting-changing-revoking-access#grant-single-role) to learn about granting roles.

* A JSON key for the service account.

See the [Google documentation](https://developers.google.com/identity/protocols/oauth2/service-account#creatinganaccount) for help creating a new service account and generating its key.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Google Cloud Pub/Sub materialization, which will direct one or more of your Flow collections to your desired Pub/Sub topics.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Authentication | Credentials used to authenticate with Google. | array, boolean, null, number, object, string | Required |
| `/credentials/auth_type` | Authentication type | Set to `Service` for manual authentication, or use OAuth in the web app.  | string |  |
| `/credentials/credentials_json` | Service Account JSON | The JSON key of the service account to use for authorization, if configuring manually. | string |  |
| **`/project_id`** | Google Cloud Project ID | Name of the project containing the PubSub topics for this materialization. | string | Required |

#### Bindings

:::caution
PubSub topics need a [default subscription](https://cloud.google.com/pubsub/docs/create-topic#properties_of_a_topic);
otherwise, delivered messages will be lost. Leave **Create with Default Subscription** set to the default, `true`,
unless you have a specific reason not to do so.
:::

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/create_default_subscription`** | Create with Default Subscription | Create a default subscription when creating the topic. Will be created as &quot;&lt;topic&gt;-sub&quot;. Has no effect if the topic already exists. | boolean | Required, `true` |
| `identifier` | Resource Binding Identifier | Optional identifier for the resource binding if creating a [multiplex topic](#multiplex-topics). Included as \"identifier\" attribute in published messages if specified. | string | |
| **`/topic`** | Topic Name | Name of the topic to publish materialized results to. | string | Required |

### Sample

This sample reflects the [manual authentication](#manual-authentication) method using the CLI.

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        config:
          credentials:
            auth_type: Service
            credentials_json: {secret}
          project_id: my_google_cloud_project
    bindings:
  	- resource:
        create_default_subscription: true
      	topic: my_new_topic
      source: ${PREFIX}/${source_collection}
```

## Multiplex topics

You can materialize multiple Flow collections to the same Pub/Sub topic. This is known as a **multiplex topic**.
You do so by adding the optional `identifier` field to the [binding configuration](#bindings).

When materializing to a multiplex topic, ensure that:

* The bindings you want to combine have the same `topic` name.
* Each binding pulls from a different Flow collection
* Each binding has a unique `identifier`. It can be anything you'd like.

The binding configuration will look similar to:

```yaml
bindings:
  - resource:
      identifier: one
      topic: multiplex-topic
    source: ${PREFIX}/source_collection_one
  - resource:
      identifier: two
      topic: multiplex-topic
    source: ${PREFIX}/source_collection_two
```

## Delta updates

Because Google Cloud Pub/Sub is a write-only event-streaming system, this connector uses only [delta updates](/concepts/materialization/#delta-updates).

## Message ordering

Google Cloud Pub/Sub manages message ordering using [ordering keys](https://cloud.google.com/pubsub/docs/ordering).

This connector sets the ordering key of published messages using the Flow [collection key](../../../concepts/collections.md#keys)
of the documents being being published.
Messages are published in order, on a per-key basis.
