---
sidebar_position: 1
---
# Postgres (Heroku)
This connector captures data from Postgres into Flow collections.  It is specifically for DBs that don't support write ahead logs.

It is available for use in the Flow web application. For local development or open-source workflows, ghcr.io/estuary/source-postgres:dev provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites
Before setting up the Postgres source connector, make sure you have the following:

* Postgres Version: Use Postgres v9.3.x or above for non-CDC workflows and Postgres v10 or above for CDC workflows.
* SSL Enabled: Ensure SSL is enabled in your environment.

## Setup
Follow the steps below to set up the Postgres connector:

1. Log into the Heroku UI and extract your Username and Password.

### Set up the Postgres Connector in Estuary Flow

To configure the Postgres source connector:

1. Log into your Estuary Flow account.
2. Navigate to Captures.
3. Choose "Postgres (Heroku)" from the connector search.
4. Enter the Host, Port, DB Name and password for your Postgres database from step 1 above.
5. List the Schemas you want to sync if applicable.
6. Select "require" from the SSL Mode options (Heroku mandates it).
7. On the next page, select a cursor field for each collection.  Note that your cursor field currently has to be either a timestamp or string.

## Configuration
You configure connectors either in the Flow web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Postgres (Heroku) source connector.

### Properties

#### Endpoint
| Property         | Title         | Description               | Type   | Required/Default |
| ---------------- | ------------- | ------------------------- | ------ | ---------------- |
| `/host`          | Host          | Hostname of the database. | string | Required         |
| `/port`          | Port          | Port of the database      | string | Default          |
| `/database_name` | Database Name | Name of the database      | string | Required         |


#### Bindings

| Property        | Title     | Description                                                             | Type   | Required/Default |
| --------------- | --------- | ----------------------------------------------------------------------- | ------ | ---------------- |
| **`/stream`**   | Stream    | Resource of your Postgres Tables from which collections are captured. | string | Required         |
| **`/syncMode`** | Sync Mode | Connection method.                                                      | string | Required         |

### Sample

```json
{
  "properties": {
    "replication_method": {
      "oneOf": null,
      "type": "object",
      "properties": {
        "method": {
          "type": "string",
          "default": "Standard",
          "const": "Standard"
        }
      },
      "default": {
        "method": "Standard"
      }
    },
    "jdbc_url_params": {
      "description": "Additional properties to pass to the JDBC URL string when connecting to the database formatted as 'key=value' pairs separated by the symbol '&'. (Eg. key1=value1&key2=value2&key3=value3). For more information see https://jdbc.postgresql.org/documentation/head/connect.html"
    },
    "ssl_mode": {
      "description": "SSL connection modes. Read more at https://jdbc.postgresql.org/documentation/head/ssl-client.html",
      "oneOf": [
        {
          "additionalProperties": true,
          "description": "Disables encryption of communication between Flow and source database.",
          "properties": {
            "mode": {
              "const": "disable",
              "order": 0,
              "type": "string"
            }
          },
          "required": [
            "mode"
          ],
          "title": "disable"
        },
        {
          "additionalProperties": true,
          "description": "Enables encryption only when required by the source database.",
          "properties": {
            "mode": {
              "const": "allow",
              "order": 0,
              "type": "string"
            }
          },
          "required": [
            "mode"
          ],
          "title": "allow"
        },
        {
          "additionalProperties": true,
          "description": "Allows unencrypted connection only if the source database does not support encryption.",
          "properties": {
            "mode": {
              "const": "prefer",
              "order": 0,
              "type": "string"
            }
          },
          "required": [
            "mode"
          ],
          "title": "prefer"
        },
        {
          "additionalProperties": true,
          "description": "Always require encryption. If the source database server does not support encryption, connection will fail.",
          "properties": {
            "mode": {
              "const": "require",
              "order": 0,
              "type": "string"
            }
          },
          "required": [
            "mode"
          ],
          "title": "require"
        },
        {
          "additionalProperties": true,
          "description": "Always require encryption and verifies that the source database server has a valid SSL certificate.",
          "properties": {
            "ca_certificate": {
              "airbyte_secret": true,
              "description": "CA certificate",
              "multiline": true,
              "order": 1,
              "title": "CA Certificate",
              "type": "string"
            },
            "client_certificate": {
              "airbyte_secret": true,
              "always_show": true,
              "description": "Client certificate",
              "multiline": true,
              "order": 2,
              "title": "Client Certificate",
              "type": "string"
            },
            "client_key": {
              "airbyte_secret": true,
              "always_show": true,
              "description": "Client key",
              "multiline": true,
              "order": 3,
              "title": "Client Key",
              "type": "string"
            },
            "client_key_password": {
              "airbyte_secret": true,
              "description": "Password for keystorage. If you do not add it - the password will be generated automatically.",
              "order": 4,
              "title": "Client key password",
              "type": "string"
            },
            "mode": {
              "const": "verify-ca",
              "order": 0,
              "type": "string"
            }
          },
          "required": [
            "mode",
            "ca_certificate"
          ],
          "title": "verify-ca"
        },
        {
          "additionalProperties": true,
          "description": "This is the most secure mode. Always require encryption and verifies the identity of the source database server.",
          "properties": {
            "ca_certificate": {
              "airbyte_secret": true,
              "description": "CA certificate",
              "multiline": true,
              "order": 1,
              "title": "CA Certificate",
              "type": "string"
            },
            "client_certificate": {
              "airbyte_secret": true,
              "always_show": true,
              "description": "Client certificate",
              "multiline": true,
              "order": 2,
              "title": "Client Certificate",
              "type": "string"
            },
            "client_key": {
              "airbyte_secret": true,
              "always_show": true,
              "description": "Client key",
              "multiline": true,
              "order": 3,
              "title": "Client Key",
              "type": "string"
            },
            "client_key_password": {
              "airbyte_secret": true,
              "description": "Password for keystorage. If you do not add it - the password will be generated automatically.",
              "order": 4,
              "title": "Client key password",
              "type": "string"
            },
            "mode": {
              "const": "verify-full",
              "order": 0,
              "type": "string"
            }
          },
          "required": [
            "mode",
            "ca_certificate"
          ],
          "title": "verify-full"
        }
      ]
    }
  }
}
```

## Supported Cursors
The supported cursors for incremental sync are:

* TIMESTAMP
* TIMESTAMP_WITH_TIMEZONE
* TIME
* TIME_WITH_TIMEZONE
* DATE
* BIT
* BOOLEAN
* TINYINT/SMALLINT
* INTEGER
* BIGINT
* FLOAT/DOUBLE
* REAL
* NUMERIC/DECIMAL
* CHAR/NCHAR/NVARCHAR/VARCHAR/LONGVARCHAR
* BINARY/BLOB

## Limitations
The Postgres source connector has the following limitations:

* Schema Size: Schemas larger than 4MB are not supported.
* Schema Alteration: The connector does not alter the schema present in your database. The destination may alter the schema, depending on its configuration.
* Schema Evolution: Adding/removing tables without resetting the entire connection is supported. Resetting a single table within the connection without resetting the rest of the destination tables is also supported. Changing a column data type or removing a column might break connections.
* Xmin Replication Mode: Xmin replication is supported for cursor-less replication.
* Temporary File Size Limit: Larger tables may encounter temporary file size limit errors. You may need to increase the temp_file_limit.
