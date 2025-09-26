---
slug: /reference/openmetrics-api/
---

# OpenMetrics API

Estuary's OpenMetrics API exposes detailed metrics data on your captures, derivations, and materializations. This allows you to track your Estuary Flow usage and pipeline activity in depth.
Integrating the API with monitoring platforms like Prometheus or Datadog also allows you to implement alerts with greater specificity than Estuary currently offers natively.

## Using the OpenMetrics API

The OpenMetrics API consists of one main endpoint:

```https://agent-api-1084703453822.us-central1.run.app/api/v1/metrics/{prefix}/```

The prefix may be for your entire tenant (such as `acmeCo/`) or a subset (such as `acmeCo/sub/path/`).

:::warning
Don't forget the trailing slash on your tenant or subpath: it's part of your prefix.
:::

To authenticate the API, you will need an [Estuary refresh token](/guides/how_to_generate_refresh_token). You can generate one in the [Admin panel](https://dashboard.estuary.dev/admin/api) of your dashboard.

See the sections below for specific instructions on working with the API in relation to:
* [Prometheus](#prometheus)
* [Datadog](#datadog)
* [As a custom integration](#custom-integration)

### Prometheus

[Prometheus](https://prometheus.io/) is an open-source monitoring solution. Integrating the OpenMetrics API with Prometheus lets you track your Estuary metrics over time.

To use the OpenMetrics API with Prometheus, configure your `prometheus.yml` file to include Estuary's information. For example:

```yaml
global:
  scrape_interval: 1m

scrape_configs:
  - job_name: estuary
    scheme: https
    bearer_token: REFRESH_TOKEN
    metrics_path: /api/v1/metrics/PREFIX/
    static_configs:
      - targets: [agent-api-1084703453822.us-central1.run.app]
```

Make sure to replace `REFRESH_TOKEN` and `PREFIX` with your generated refresh token and desired prefix.

To try this locally with Docker, you can run:

```bash
docker run --rm -it -p "9090:9090" -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus:latest
```

You will then be able to view a Prometheus dashboard at http://localhost:9090/ to start graphing your metrics.

![Prometheus dashboard](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//openmetrics_prometheus_3fb3fe6bfa/openmetrics_prometheus_3fb3fe6bfa.png)

### Datadog

[Datadog](https://www.datadoghq.com/) is a cloud-monitoring-as-a-service platform. Like with Prometheus, you can integrate with Datadog to track your Estuary metrics over time. To do so, though, you will need some additional information related to your Datadog account.

To use the OpenMetrics API with Datadog, add the API endpoint to your `datadog.yaml` agent config. For example:

```yaml
init_config: {}

instances:
  - openmetrics_endpoint: https://agent-api-1084703453822.us-central1.run.app/api/v1/metrics/PREFIX/
    namespace: estuary
    min_collection_interval: 60
    headers:
      Authorization: Bearer REFRESH_TOKEN
    metrics:
      - ".*"
```

Make sure to replace `REFRESH_TOKEN` and `PREFIX` with your generated refresh token and desired prefix.

You can try running an agent locally with Docker using:

```bash
docker run --rm -it -v $(pwd)/datadog.yaml:/etc/datadog-agent/conf.d/openmetrics.d/conf.yaml:ro \
  -e DD_API_KEY=YOUR_DATADOG_API_KEY \
  -e DD_SITE=YOUR_DATADOG_SITE \
  -e DD_HOSTNAME=YOUR_HOSTNAME \
  -p "9090:9090" \
  gcr.io/datadoghq/agent:latest
```

Note that you will need to include environment variables for:
* Your Datadog API key
* Your Datadog site
* Your Datadog host name

![Datadog dashboard](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//openmetrics_datadog_62a6b99a88/openmetrics_datadog_62a6b99a88.png)

For more information on configuring your Datadog agent and retrieving the required data, see [Datadog's docs](https://docs.datadoghq.com/agent/).

### Custom integration

You can also access the OpenMetrics API directly or as part of a custom integration. To do so, use the endpoint with your prefix as the URL and use your refresh token as an `Authorization: Bearer` header.

For example, using `curl` this would look like:

```bash
curl --location 'https://agent-api-1084703453822.us-central1.run.app/api/v1/metrics/PREFIX/' \
--header 'Authorization: Bearer REFRESH_TOKEN'
```

The output will contain the current totals for various tasks under the prefix, separated by metric name. The response uses the following format:

```
# HELP logged_warnings_total Total log lines at level WARN, by task
# TYPE logged_warnings_total counter
logged_warnings_total{task="acmeCo/prefix/source-oracle"} 12
logged_warnings_total{task="acmeCo/prefix/materialize-mongodb"} 5
# HELP logged_errors_total Total log lines at level ERROR, by task
# TYPE logged_errors_total counter
{...}
# HELP materialized_out_docs_total Total number of post-reduce documents stored to the target, by task and source collection
# TYPE materialized_out_docs_total counter
materialized_out_docs_total{task="acmeCo/prefix/dekaf-tinybird",collection="acmeCo/prefix/collection"} 1580
# EOF
```

If you implement your own polling, note that the recommended interval for most use cases is one minute.

## Available metrics

Estuary currently tracks the following metrics in the OpenMetrics API. Metrics fall in one of two categories:

* **Counters:** return the total related to that metric (eg. total number of docs or bytes)
* **Gauges:** return the latest publication timestamp

| Name | Type | Description |
| --- | --- | --- |
| `captured_in_bytes_total` | Counter | Total number of pre-combine bytes captured by the connector, by task and target collection |
| `captured_in_docs_total` | Counter | Total number of pre-combine documents captured by the connector, by task and target collection |
| `captured_out_bytes_total` | Counter | Total number of post-combine bytes captured by the connector, by task and target collection |
| `captured_out_docs_total` | Counter | Total number of post-combine documents captured by the connector, by task and target collection |
| `derived_in_bytes_total` | Counter | Total number of pre-reduce bytes read from the source collection, by task, source collection, and transform |
| `derived_in_docs_total` | Counter | Total number of pre-reduce documents read from the source collection, by task, source collection, and transform |
| `derived_last_source_published_at_time_seconds` | Gauge | Publication timestamp of the most recent source collection document that was processed by the derivation, given as seconds since the unix epoch |
| `derived_out_bytes_total` | Counter | Total number of post-combine bytes published by derivation transforms, by task |
| `derived_out_docs_total` | Counter | Total number of post-combine documents published by derivation transforms, by task |
| `derived_yield_bytes_total` | Counter | Total number of pre-combine bytes published by derivation transforms, by task |
| `derived_yield_docs_total` | Counter | Total number of pre-combine documents published by derivation transforms, by task |
| `logged_errors_total` | Counter | Total log lines at level ERROR, by task |
| `logged_failures_total` | Counter | Total log lines indicating task failure, by task |
| `logged_warnings_total` | Counter | Total log lines at level WARN, by task |
| `materialized_in_bytes_total` | Counter | Total number of pre-reduce bytes read from the source collection, by task and source collection |
| `materialized_in_docs_total` | Counter | Total number of pre-reduce documents read from the source collection, by task and source collection |
| `materialized_last_source_published_at_time_seconds` | Gauge | Publication timestamp of the most recent source collection document that was materialized, given as seconds since the unix epoch |
| `materialized_load_bytes_total` | Counter | Total number of pre-reduce bytes loaded from the target, by task and source collection |
| `materialized_load_docs_total` | Counter | Total number of pre-reduce documents loaded from the target, by task and source collection |
| `materialized_out_bytes_total` | Counter | Total number of post-reduce bytes stored to the target, by task and source collection |
| `materialized_out_docs_total` | Counter | Total number of post-reduce documents stored to the target, by task and source collection |
| `read_by_me_bytes_total` | Counter | Total number of collection bytes read by this task, by task |
| `read_by_me_docs_total` | Counter | Total number of collection documents read by this task, by task |
| `read_from_me_bytes_total` | Counter | Total number of collection bytes read from this source, by collection |
| `read_from_me_docs_total` | Counter | Total number of collection documents read from this source, by collection |
| `txn_count_total` | Counter | Total number of transactions processed by this task, by task |
| `usage_seconds_total` | Counter | Total number of billable seconds of connector usage time, by task |
| `written_by_me_bytes_total` | Counter | Total number of collection bytes written by this task, by task |
| `written_by_me_docs_total` | Counter | Total number of collection documents written by this task, by task |
| `written_to_me_bytes_total` | Counter | Total number of collection bytes written to this target, by collection |
| `written_to_me_docs_total` | Counter | Total number of collection documents written to this target, by collection |

