---
sidebar_position: 2
---
# Configuring Task Shards

For some catalog tasks, it's helpful to control the behavior of [shards](../concepts/advanced/shards.md).
You do this by adding the `shards` configuration to the capture or materialization configuration.

## Properties

| Property | Title | Description | Type |
|---|---|---|---|
| `/disable` | Disable | Disable processing of the task's shards. | Boolean |
| `/logLevel` | Log level | Log levels may currently be \"error\", \"warn\", \"info\", \"debug\", or \"trace\". If not set, the effective log level is \"info\". | String |
| `/maxTxnDuration` | Maximum transaction duration | This duration upper-bounds the amount of time during which a transaction may process documents before it must initiate a commit. Note that it may take some additional time for the commit to complete after it is initiated. The shard may run for less time if there aren't additional ready documents for it to process. If not set, the maximum duration defaults to one second for captures and derivations, and 5 minutes for materializations. | String |
| `/minTxnDuration` | Minimum transaction duration | This duration lower-bounds the amount of time during which a transaction must process documents before it must flush and commit. It may run for more time if additional documents are available. The default value is zero seconds. | String |

For more information about these controls and when you might need to use them, see:

* [Transactions](../concepts/advanced/shards.md#transactions)
* [Log level](../concepts/advanced/logs-stats.md#log-level)

## Sample

```yaml
materializations:
  acmeCo/snowflake-materialization:
    endpoint:
      connector:
        config:
          account: acmeCo
          database: acmeCo_db
          password: secret
          cloud_provider: aws
          region: us-east-1
          schema: acmeCo_flow_schema
          user: snowflake_user
          warehouse: acmeCo_warehouse
        image: ghcr.io/estuary/materialize-snowflake:dev
    bindings:
    - resource:
        table: anvils
      source: acmeCo/anvils
    shards:
      logLevel: debug
      minTxnDuration: 30s
      maxTxnDuration: 4m
```
