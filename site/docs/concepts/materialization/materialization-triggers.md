---
slug: /concepts/materialization-triggers/
---

# Materialization Triggers

Triggers let you fire webhook requests whenever a materialization commits a
transaction. You can use them to notify downstream services that new data has
been materialized — for example, to kick off a dbt run, send a Slack message,
or call a custom API.

Triggers are configured on the materialization itself and fire once per
committed transaction. Each trigger sends an HTTP request whose URL, method,
headers, and JSON body you define. The body is a
[Handlebars](https://handlebarsjs.com/) template that can reference transaction
metadata and secret header values.

## How triggers work

Estuary processes data in
[transactions](/concepts/advanced/shards/#transactions). When a materialization
transaction commits, the runtime renders each configured trigger's payload
template with variables from the transaction, then sends the resulting JSON body
to the trigger's URL.

Triggers are delivered with **at-least-once** semantics. The trigger parameters
are persisted to durable storage as part of the transaction commit, so if the
materialization restarts before delivery completes, the triggers will be
re-delivered on recovery.

All configured triggers fire concurrently after each transaction is acknowledged
by the connector. If a trigger receives a retryable error (5xx, 408, or 429), it
is retried with exponential backoff for up to `maxAttempts` total attempts.
Non-retryable client errors (other 4xx status codes) fail immediately without
retry.

:::note
Triggers fire once per materialization transaction, not once per document. A
single transaction may contain documents from multiple bindings. The
`collection_names` template variable lists which collections contributed
documents to the transaction.
:::

## Specification

Triggers are defined under the `triggers` key of a materialization
specification:

```yaml
materializations:
  acmeCo/example/database-views:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-postgres:dev
        config: path/to/connector-config.yaml
    bindings:
      - source: acmeCo/example/collection
        resource: { table: example_table }

    # Webhook triggers fired after each committed transaction.
    # Optional, type: object
    triggers:
      config:
        - # URL of the webhook endpoint.
          # Required, type: string
          url: "https://example.com/webhook"
          # HTTP method for the request.
          # Optional. Default: POST. One of: POST, PUT, PATCH.
          method: POST
          # HTTP headers to include in the request. Header values are
          # encrypted at rest and can be referenced in the payload template
          # as {{headers.HeaderName}}, making them useful for secrets like
          # API keys that need to appear in the request body.
          # Optional, type: object
          headers:
            Authorization: "Bearer my-secret-token"
          # Handlebars template for the JSON request body. See "Template
          # variables" below for available variables.
          # Required, type: string
          payloadTemplate: |
            {
              "materialization": "{{materialization_name}}",
              "collections": [{{#each collection_names}}"{{this}}"{{#unless @last}}, {{/unless}}{{/each}}],
              "publishedAtMin": "{{flow_published_at_min}}",
              "publishedAtMax": "{{flow_published_at_max}}",
              "runId": "{{flow_run_id}}"
            }
          # Request timeout in seconds.
          # Optional. Default: 30.
          timeoutSecs: 30
          # Maximum number of delivery attempts (including the initial attempt).
          # Optional. Default: 3.
          maxAttempts: 3
```

## Properties

| Property | Title | Description | Type | Default |
|---|---|---|---|---|
| **`/triggers/config`** | Trigger Configurations | List of webhook triggers to fire when new data is materialized. | array | |
| **`/triggers/config/*/url`** | URL | URL of the webhook endpoint. Must be a valid URL. | string | |
| **`/triggers/config/*/method`** | HTTP Method | HTTP method for the request. One of `POST`, `PUT`, or `PATCH`. | string | `POST` |
| **`/triggers/config/*/headers`** | Headers | HTTP headers to include in the request. Values are encrypted at rest. | object | |
| **`/triggers/config/*/payloadTemplate`** | Payload Template | Handlebars template that renders to the JSON request body. | string | |
| **`/triggers/config/*/timeoutSecs`** | Timeout | Request timeout in seconds. Must be greater than 0. | integer | `30` |
| **`/triggers/config/*/maxAttempts`** | Max Attempts | Maximum number of delivery attempts (including the initial attempt). | integer | `3` |

## Template variables

The payload template is rendered using [Handlebars](https://handlebarsjs.com/)
with the following variables:

| Variable | Description | Example |
|---|---|---|
| `{{materialization_name}}` | Full name of the materialization. | `acmeCo/example/database-views` |
| `{{collection_names}}` | Array of collection names that contributed documents to this transaction. Use `{{#each collection_names}}` to iterate. | `["acmeCo/example/collection"]` |
| `{{connector_image}}` | Docker image of the materialization connector. | `ghcr.io/estuary/materialize-postgres:dev` |
| `{{flow_published_at_min}}` | Earliest document publish timestamp across all bindings in the transaction (RFC 3339). | `2024-01-15T08:30:00Z` |
| `{{flow_published_at_max}}` | Latest document publish timestamp across all bindings in the transaction (RFC 3339). | `2024-01-15T08:31:00Z` |
| `{{flow_run_id}}` | Unique identifier for this trigger invocation (UUID v4). | `a1b2c3d4-e5f6-7890-abcd-ef1234567890` |
| `{{headers.Name}}` | Value of the header named `Name` from the trigger's `headers` configuration. Useful for injecting secrets into the payload body. | `Bearer my-secret-token` |

Templates run in strict mode: referencing an undefined variable is an error.
Values are not HTML-escaped, so the rendered output is raw JSON.

## Examples

### Notify a Slack channel

Slack [incoming webhooks](https://api.slack.com/messaging/webhooks) accept a
simple JSON body with a `text` field:

```yaml
triggers:
  config:
    - url: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
      payloadTemplate: |
        {
          "text": "Materialization {{materialization_name}} committed new data from {{#each collection_names}}{{this}}{{#unless @last}}, {{/unless}}{{/each}}"
        }
```

### Trigger a dbt Cloud job

```yaml
triggers:
  config:
    - url: "https://cloud.getdbt.com/api/v2/accounts/12345/jobs/67890/run/"
      headers:
        Authorization: "Token my-dbt-api-token"
      payloadTemplate: |
        {
          "cause": "Triggered by Estuary materialization {{materialization_name}}",
          "git_sha": "HEAD"
        }
```

### Trigger a dbt Core job via GitHub Actions

For dbt Core, you can use a trigger to kick off a [GitHub Actions
`workflow_dispatch`](https://docs.github.com/en/actions/writing-workflows/choosing-when-your-workflow-runs/events-that-trigger-workflows#workflow_dispatch)
workflow that runs your dbt project.

First, create a workflow in your dbt repository at
`.github/workflows/dbt-run.yml`:

```yaml
name: dbt run
on:
  workflow_dispatch:

jobs:
  dbt:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install dbt-core dbt-postgres  # or your adapter
      - run: dbt build --profiles-dir ./profiles
```

Then configure a trigger that calls the GitHub API to dispatch the workflow.
The `ref` field specifies which branch to run against:

```yaml
triggers:
  config:
    - url: "https://api.github.com/repos/YOUR_ORG/YOUR_DBT_REPO/actions/workflows/dbt-run.yml/dispatches"
      headers:
        Authorization: "Bearer ghp_your_personal_access_token"
      payloadTemplate: |
        {
          "ref": "main"
        }
```

:::tip
You can use a [GitHub fine-grained personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-fine-grained-personal-access-token)
with the **Actions (read and write)** permission scoped to just your dbt
repository.
:::

### Include secret headers in the payload body

Header values are encrypted at rest but are available in the template context
under `{{headers.Name}}`. This lets you pass secrets into the request body
without storing them in plaintext in the template:

```yaml
triggers:
  config:
    - url: "https://api.example.com/notify"
      headers:
        X-Api-Key: "sk-secret-key-value"
      payloadTemplate: |
        {
          "apiKey": "{{headers.X-Api-Key}}",
          "materialization": "{{materialization_name}}",
          "runId": "{{flow_run_id}}"
        }
```

### Multiple triggers per materialization

You can configure multiple triggers. They all fire concurrently after each
transaction:

```yaml
triggers:
  config:
    - url: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
      payloadTemplate: |
        {"text": "New data from {{materialization_name}}"}
    - url: "https://cloud.getdbt.com/api/v2/accounts/123/jobs/456/run/"
      headers:
        Authorization: "Token dbt-token"
      payloadTemplate: |
        {"cause": "Estuary trigger {{flow_run_id}}"}
```

## Delivery guarantees and retry behavior

Triggers provide **at-least-once** delivery. The trigger parameters are durably
persisted as part of the materialization's transaction commit. If the
materialization process crashes after committing but before completing delivery,
triggers are re-fired on recovery.

:::warning
Because delivery is at-least-once, your webhook endpoint should be prepared
to receive duplicate requests. You can use `{{flow_run_id}}` as an
idempotency key to deduplicate on the receiving side.
:::

When a trigger request fails:

- **5xx**, **408 Request Timeout**, and **429 Too Many Requests** responses are
  retried with exponential backoff (1s, 2s, 4s, ..., capped at 30s) for up to
  `maxAttempts` total attempts.
- **Other 4xx** responses are considered non-retryable client errors and fail
  immediately.
- If all attempts are exhausted, the trigger is considered failed and an
  error is logged.

:::warning
A trigger failure (exhausted attempts or a non-retryable error) is treated as a
**transaction failure** and will cause the materialization task to restart. This
means an unreachable or misconfigured webhook endpoint will block data flow for
the entire materialization until the issue is resolved. Make sure your webhook
endpoints are reliable, and set `maxAttempts` high enough to ride out transient
failures.
:::

## Encryption of secrets

Header values are automatically encrypted at rest using
[SOPS](https://github.com/getsops/sops). When you publish a materialization
with triggers, header values are encrypted by the config-encryption service
before being stored.

SOPS protects the integrity of encrypted configurations with an HMAC. The
following fields are covered by this integrity check and **cannot be modified**
after initial publication without re-entering all secret header values:

- `url`
- `method`
- `headers` (keys and encrypted values)

The remaining fields (`payloadTemplate`, `timeoutSecs`, `maxAttempts`) are
excluded from the SOPS integrity check, so you can modify them freely without
needing to re-enter your secret header values.
