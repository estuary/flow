
# Customizing Alerts

You can configure alert thresholds and scope subscriptions to customize when and how you receive notifications.
You may manage these customization options using [`flowctl`](/guides/get-started-with-flowctl), Estuary's CLI tool.

## Scope notifications per environment

Subscriptions are scoped by catalog prefix, and a subscription can target a sub-prefix such as a single environment (for example `acmeCo/prod/`). Subscriptions are additive: an alert notifies every subscription whose prefix is a parent of the failing task. To route environments differently, create a separate subscription for each prefix.

For example, you can page an on-call address for `acmeCo/prod/` but send `acmeCo/dev/` alerts to a team list.

```bash
# Subscribe an address to Task Failed alerts for one environment
flowctl alerts subscriptions subscribe --prefix acmeCo/prod/ --email oncall@example.com --alert-type shard_failed

# List current subscriptions under a prefix
flowctl alerts subscriptions list --prefix acmeCo/

# Remove a single alert type from a subscription (the others stay in place)
flowctl alerts subscriptions unsubscribe --prefix acmeCo/ --email oncall@example.com --alert-type shard_failed
```

Catalog prefixes must end in `/`.

## Alert configurations

Alert conditions can be tuned per prefix or per task with `flowctl alerts configs`.
Configure alerts to reduce noise, or to apply different sensitivity to different environments.

A more specific prefix overrides a broader one, field by field; any value you don't set inherits the default.
This lets you set tenant-wide behavior with per-subprefix or per-task exceptions.

Alerts that can be configured include:
* [`shardFailed`](/reference/notifications/#task-failure-alerts) (for task failures)
   * `taskChronicallyFailing`
* [`dataMovementStalled`](/reference/notifications/#data-movement-alerts)
* [`taskIdle`](/reference/notifications/#idle-task-alerts)

### Opt in/out

Each configurable alert includes a `.enabled` setting.
This allows you to granularly manage whether certain subprefixes or tasks fire these alerts or not.

For example, you can keep [task failure](/reference/notifications/#task-failure-alerts) alerts enabled for your tenant overall while disabling it for a development-environment subprefix or a particular noisy task.

```shell
# Configure a default setting for a tenant by using a base-level prefix
# Alert types are already enabled by default
flowctl alerts configs update --prefix acmeCo/ --set shardFailed.enabled=true

# Opt out of notifications for an alert type for a specific subprefix
flowctl alerts configs update --prefix acmeCo/dev/ --set shardFailed.enabled=false
```

### Configure alert thresholds

Some alert configuration options are tunable **thresholds**.
You can specify a certain number of failures or certain timeframe before an alert type fires.

As an example, the [Task Failed](/reference/notifications/#task-failure-alerts) alert fires after a number of failures within a rolling window. The defaults are 3 failures within 8 hours.
Both of these defaults are configurable.

```bash
# Require 6 failures within 8 hours before alerting, for everything under acmeCo/
flowctl alerts configs update --prefix acmeCo/ --set shardFailed.condition.failures=6 --set shardFailed.condition.per=8h

# Apply a higher tolerance to a development environment
flowctl alerts configs update --prefix acmeCo/dev/ --set shardFailed.condition.failures=20

# View configured thresholds under a prefix
flowctl alerts configs list --prefix acmeCo/
```

Available threshold configurations include:

| Setting | Description | Default |
| --- | --- | --- |
| `dataMovementStalled.condition.stalledFor` | Timeframe where no new data has been received |  |
| `shardFailed.condition.failures` | Failures in the last configured length of time | `3` |
| `shardFailed.condition.per` | Timeframe for task failures | `8h` |
| `taskChronicallyFailing.condition.failingFor` | Timeframe a task has been repeatedly failing | `30d` |
| `taskIdle.condition.idleFor` | Length of time a task has been idle | `30d` |
