---
description: How to configure endpoints for materializations
---

# Endpoint configurations

An [endpoint](../../../concepts/catalog-entities/materialization.md#endpoints) is an external system from which a Flow collection may be captured, or to which a Flow collection may be materialized. This page deals with materialization endpoints specifically; to learn about capture endpoints, go [here.](../captures/endpoint-configurations.md)&#x20;

Endpoints are objects within the materialization definition. In most cases, they are powered by connectors. They use the following entity structure:

```yaml
materializations:
  my/materialization/name:
    # Required, type: object
    endpoint:
      # The value of an endpoint must be an object that defines only one of the following
      # top-level properties. Each of these top-level properties corresponds to a specific type of
      # external system that you want to connect to, and holds system-dependent connection
      # information. Each object here also permits any additional properties, which
      # will simply be ignored.
      specific_endpoint_type:
        specific_endpoint_configuration: value
        specific_endpoint_configuration2: value2

  my/other/name:
    endpoint:
      # As a concrete example, SQLite only requires a `path` key as its configuration.
      sqlite:
        path: db/hello-world.db

```

Flow currently supports the configurations [listed in the connector documentation](../../../concepts/connectors.md#materialization-connectors). Required values for some of these are provided below, as well as external documentation for each system.&#x20;

{% hint style="info" %}
Complete configuration details for all connectors are coming soon to the documentation.
{% endhint %}

### Snowflake configuration

Snowflake is supported as a materialization endpoint. Learn more about [configuring Snowflake](https://pkg.go.dev/github.com/snowflakedb/gosnowflake#Config).

```yaml
# A Snowflake endpoint configuration
# To be nested under <endpoint> in catalog spec.
snowflake:
  # Required, type: string
  account: exampleAccount
  # Required, type: string
  database: exampleDB
  # Required, type: string
  password: examplePass
  # type: string
  region: exampleRegion
  # type: string
  role: exampleRole
  # Required, type: string
  schema: exampleSchema
  # Required, type: string
  user: exampleUser
  # type: string
  warehouse: exampleWh
```

### PostgreSQL configuration

PostgreSQL is supported as a materialization endpoint. Learn more about [configuring PostgreSQL](https://pkg.go.dev/github.com/lib/pq#hdr-Connection\_String\_Parameters).

```yaml
# A PostgreSql endpoint configuration
# To be nested under <endpoint> in catalog spec.
postgres:
  # Host address of the database.
  # Required, type: string
  host: exampleHost

  # Connection password.
  # Required, type: string
  password: examplePW

  # Connection user.
  # Required, type: string
  user: exampleUser

  # Logical Database.
  # type: string
  dbname: exampleDB

  # Port of the database.
  # default: 5432, uint16 => 0
  port: examplePort
```

### SQLite configuration

SQLite is supported as a materialization endpoint. Learn more about [configuring SQLite](https://github.com/mattn/go-sqlite3#connection-string).

```yaml
# A SQLite endpoint configuration
# To be nested under <endpoint> in catalog spec.
sqlite:
  # An absolute or relative URI to the SQLite database. This will be
  # created automatically if it doesn't exist. It may include query
  # parameters that will be interpreted by SQLite.
  # Required, type: string
  path: example.db?_journal=WAL
```

### Webhook configuration

HTTP(s) webhooks are supported as materialization endpoints.

```yaml
# A webhook endpoint configuration
webhook:
  # Required, type: string
  address: http://localhost:9000/
```

