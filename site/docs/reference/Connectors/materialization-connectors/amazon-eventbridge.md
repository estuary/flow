
# Amazon EventBridge

This connector materializes Estuary collections into events on an Amazon EventBridge event bus. Each document is published as an [EventBridge event](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-events.html) using the AWS [PutEvents](https://docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutEvents.html) API.

## Prerequisites

To use this connector, you'll need:

- An existing EventBridge event bus to publish to. The connector does not create the bus; it must exist before the materialization is applied. To use your account's default bus, set the bus name to `default`.

- An IAM user or role with the following [permissions](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazoneventbridge.html) on the target event bus:
  - `PutEvents`
  - `DescribeEventBus`

  These permissions should be specified with the `events:` prefix in an IAM policy document scoped to the bus ARN. For more details and examples, see [Using identity-based policies with Amazon EventBridge](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-use-identity-based.html) in the Amazon docs.

- AWS Credentials. One of the following types:
  - The AWS **access key** and **secret access key** for the user. See the [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding these credentials.
  - To authenticate using an AWS Role, you'll need the **region** and the **role arn**. Follow the steps in the [AWS IAM guide](/guides/iam-auth/aws.md) to setup the role.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the EventBridge materialization connector.

### Properties

#### Endpoint

| Property                | Title           | Description                                                                                                                                | Type                        | Required/Default |
| ----------------------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------- | ---------------- |
| **`/region`**           | AWS Region      | Region of the EventBridge event bus.                                                                                                       | string                      | Required         |
| **`/event_bus_name`**   | Event Bus Name  | Name or ARN of the EventBridge event bus to publish to. Use `default` for the account's default bus. Verified via `DescribeEventBus` on Apply. | string                  | Required, `default` |
| **`/credentials`**      | Authentication  | Credentials for authentication.                                                                                                            | [Credentials](#credentials) | Required         |
| `/advanced/endpoint`    | AWS Endpoint    | Override the AWS endpoint URL. Used to direct requests at a compatible API such as LocalStack.                                             | string                      |                  |

#### Credentials

Credentials for authenticating with AWS. Use one of the following sets of options:

| Property                                 | Title                 | Description                                          | Type   | Required/Default         |
| ---------------------------------------- | --------------------- | ---------------------------------------------------- | ------ | ------------------------ |
| **`/credentials/auth_type`**             | Auth Type             | Method to use for authentication.                    | string | Required: `AWSAccessKey` |
| **`/credentials/aws_access_key_id`**     | AWS Access Key ID     | AWS Access Key ID for publishing to EventBridge.     | string | Required                 |
| **`/credentials/aws_secret_access_key`** | AWS Secret Access Key | AWS Secret Access Key for publishing to EventBridge. | string | Required                 |

| Property                        | Title        | Description                    | Type   | Required/Default   |
| ------------------------------- | ------------ | ------------------------------ | ------ | ------------------ |
| **`/credentials/auth_type`**    | Auth Type    | Method to use for authentication. | string | Required: `AWSIAM` |
| **`/credentials/aws_role_arn`** | AWS Role ARN | IAM Role to assume.            | string | Required           |
| **`/credentials/aws_region`**   | AWS Region   | AWS Region to authenticate in. | string | Required           |

#### Bindings

Each binding publishes to the configured event bus. The `source` and `detail_type` values are written verbatim to the corresponding fields of every event produced by that binding, and can be used by [EventBridge rules](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-rules.html) to route events to downstream targets.

| Property            | Title        | Description                                                              | Type   | Required/Default                |
| ------------------- | ------------ | ------------------------------------------------------------------------ | ------ | ------------------------------- |
| **`/source`**       | Event Source | `Source` field set on every event published from this binding.           | string | Required, `estuary.flow`        |
| **`/detail_type`**  | Detail Type  | `DetailType` field set on every event published from this binding.       | string | Required, `Document Published`  |

### Sample

```yaml
materializations:
  ${PREFIX}/${MATERIALIZATION_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-eventbridge:v1
        config:
          region: us-east-1
          event_bus_name: default
          credentials:
            auth_type: AWSAccessKey
            aws_access_key_id: example-aws-access-key-id
            aws_secret_access_key: example-aws-secret-access-key
    bindings:
      - resource:
          source: estuary.flow
          detail_type: Document Published
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Event payload

Each materialized document is published as a single [EventBridge event](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-events.html). The connector populates the following fields on each `PutEventsRequestEntry`:

| EventBridge field | Value                                                                       |
| ----------------- | --------------------------------------------------------------------------- |
| `EventBusName`    | The bus configured at the endpoint level.                                   |
| `Source`          | The `source` configured on the binding (defaults to `estuary.flow`).        |
| `DetailType`      | The `detail_type` configured on the binding (defaults to `Document Published`). |
| `Detail`          | The full root document of the materialized collection, as a JSON string.    |

`Time` and `Resources` are not set; EventBridge assigns the event time on receipt. The materialized document's content is not modified — downstream rules and targets see exactly what was in the source collection.

You can publish multiple Estuary collections to the same event bus by adding multiple bindings to the materialization. Distinct `source` / `detail_type` values let downstream EventBridge rules route each collection's events independently.

## Delta updates

Because EventBridge is an event-streaming system that has no concept of stored rows or keys, this connector uses only [delta updates](/concepts/materialization/#delta-updates). Every document in the source collection results in one event published to the bus.

Delivery is at-least-once: in rare cases, a document may be published more than once. Downstream consumers should be prepared to deduplicate if exactly-once semantics are required.

## Document size limit

EventBridge limits each event's combined `Source` + `DetailType` + `Detail` payload to **256 KB**.
If a document in the source collection produces an entry exceeding this limit, the materialization will fail with an error identifying the offending binding.

If your documents are large, use [field selection](/guides/customize-materialization-fields/#field-selection-for-materializations) on the binding to project down to only the fields needed by downstream consumers.
