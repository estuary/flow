
# Tinybird

This connector materializes Flow collections as Kafka-compatible messages that a Tinybird Kafka consumer can read. [Tinybird](https://www.tinybird.co/) is a data platform for user-facing analytics.

## Prerequisites

To use this connector, you'll need:

* At least one Flow collection
* A Tinybird account

## Variants

This connector is a variant of the default Dekaf connector. For other integration options, see the main [Dekaf](dekaf.md) page.

## Setup

Provide an auth token when setting up the Dekaf connector. This can be a password of your choosing and will be used to authenticate consumers to your Kafka topics.

Once the connector is created, note the task name, such as `YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION`. You will use this as the username.

## Connecting Estuary Flow to Tinybird

In your Tinybird Workspace, create a new Data Source and use the Kafka Connector.

![Configure Estuary Flow Data Source](../../connector-images/tinybird-dekaf-connection.png)

To configure the connection details, use the following settings.

* Bootstrap servers: `dekaf.estuary-data.com`
* SASL Mechanism: `PLAIN`
* SASL Username: Your materialization task name, such as `YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION`
* SASL Password: Your materialization's auth token

Tick the "Decode Avro messages with Schema Registry" box, and use the following settings:

* URL: `https://dekaf.estuary-data.com`
* Username: The same as your SASL username
* Password: The same as your SASL password

![Configure Estuary Flow Schema Registry](../../connector-images/tinybird-schema-registry-setup.png)

Click Next and you will see a list of topics. These topics are the collections you added to your materialization.
Select the collection you want to ingest into Tinybird, and click Next.

Configure your consumer group as needed.

Finally, you will see a preview of the Data Source schema. Feel free to make any modifications as required, then click
Create Data Source.

This will complete the connection with Tinybird, and new data from the Estuary Flow collection will arrive in your
Tinybird Data Source in real-time.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Dekaf materialization, which will direct one or more of your Flow collections to your desired topics.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/token` | Auth Token | The password that Kafka consumers can use to authenticate to this task. | string | Required |
| `/strict_topic_names` | Strict Topic Names | Whether or not to expose topic names in a strictly Kafka-compliant format. | boolean | `false` |
| `/deletions` | Deletion Mode | Can choose between `kafka` or `cdc` deletion modes. | string | `kafka` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/topic_name` | Topic Name | Kafka topic name that Dekaf will publish under. | string | Required |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      dekaf:
        config:
          token: <auth-token>
          strict_topic_names: false
          deletions: kafka
        variant: tinybird
    bindings:
      - resource:
          topic_name: ${COLLECTION_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Configuring support for deletions

Many Flow connectors capture a stream of change data which can include deletions, represented by the [`_meta/op` field](/reference/deletions). By default, the schema that Tinybird infers from your data won't include support for these deletions documents. The reason for this is that we frequently don't include the entire document that got deleted, and instead simply include its key. This will violate non-null constraints that get inferred at dataflow creation time. You can configure deletions in two ways:

### Soft deletes

Soft deletes simply require relaxing the Tinybird schema to avoid quarantining deletion documents. To do this, you can either store the whole document in a JSON column and deal with extracting fields in the destination system, or you can manually mark all data fields as `Nullable(..)`.

#### Store whole document as JSON

If you want the raw data from your Flow collection to be ingested into Tinybird without dealing with schema issues, you can request that the full document be stored as JSON using the special `#.__value` pointer. Note that you still need to extract the key, as well as `_meta.op` which will contain one of `c`, `u`, or `d`:

```
SCHEMA >
    `__value` String `json:#.__value`,
    `_id` String `json:$._id`,
    `__offset` Int64,
    `_meta_op` String `json:$._meta.op`
ENGINE "ReplacingMergeTree"
ENGINE_SORTING_KEY "_id"
ENGINE_VER "__offset"
KAFKA_STORE_RAW_VALUE 'True'
```

:::note
Using the `ReplacingMergeTree` engine along with selecting `__offset` as the revision will have the effect of deduplicating your CDC stream by the provided `ENGINE_SORTING_KEY`, where the last write wins.

If you'd rather keep the entire changelog, you can instead use `MergeTree`. See [here](https://www.tinybird.co/docs/concepts/data-sources#supported-engines-and-settings) for more details on the various options available to you.
:::

:::note
The field `_id` here represents your [collection's key](/concepts/collections/#keys), which is **not** always `_id`.
:::

:::note
If your collection key contains multiple fields, you can instead take advantage of the fact that Dekaf extracts and serializes your key into the Kafka record's key field, which is accessible in Tinybird via the special `__key` field.

To do so, set `ENGINE_SORTING_KEY "__key"`.
:::

#### Mark extracted fields as nullable

If you want to extract certain fields from your documents, you must explicitly mark them all as nullable in order to avoid rejecting deletion documents which don't contain the same data as a create or update document.

```
SCHEMA >
    `_id` String `json:$._id`,
    `_meta_op` String `json:$._meta.op`,
    `example_field` Nullable(String) `json:$.example`
ENGINE "ReplacingMergeTree"
ENGINE_SORTING_KEY "__key"
ENGINE_VER "__offset"
```

:::warning
Array fields do not support being marked as nullable, so you will not be able to extract array fields here.
:::

### Hard deletes

Instead of exposing deletion events for you to handle on your own, hard deletes cause deleted documents (identified by their unique key) to be deleted from the Tinybird dataflow entirely.

#### Enable Dekaf's `cdc` deletions mode.

This will change its default behavior of emitting deletions as Kafka null-value'd records, to emitting the full deletion document plus a special `/_meta/is_deleted` field which we'll use in a moment.

To enable this setting in the UI, expand the **Deletion Mode** option in your materialization's Endpoint Config. Choose `cdc` from the dropdown menu.

In the schema, this would be the `deletions` setting:

```yaml
endpoint:
  dekaf:
    config:
      deletions: cdc
```

#### Set up your schema for soft deletes

Pick one of the two options from above:

- [Store whole document as JSON](#store-whole-document-as-json)
- [Mark extracted fields as nullable](#mark-extracted-fields-as-nullable)

Then, you can extract the `/_meta/is_deleted` field, and configure the `ReplacingMergeTree` engine's `ENGINE_IS_DELETED` flag to use it:

```
SCHEMA >
    `__value` String `json:#.__value`,
    `_meta_is_deleted` UInt8 `json:$._meta.is_deleted`,
    `_meta_op` String `json:$._meta.op`
ENGINE "ReplacingMergeTree"
ENGINE_SORTING_KEY "__key"
ENGINE_VER "__offset"
ENGINE_IS_DELETED "_meta_is_deleted"
```

Now, the last piece to the puzzle is to add the `FINAL` keyword to any Tinybird query targeting this datasource. For example, if you create a pipe that looks like this:

**Node 1**

```SQL
SELECT * FROM your_datasource FINAL
```

**Node 2**

```SQL
SELECT * FROM your_top_node WHERE _meta_op = 'd'
```

You should find no rows returned, indicating that the deleted rows were correctly filtered out.
