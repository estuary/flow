# OracleDB (Flashback)
This connector captures data from OracleDB into Flow collections using [Oracle Flashback](https://www.oracle.com/database/technologies/flashback/).

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-oracle-flashback:dev`](https://ghcr.io/estuary/source-oracle-flashback:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites
* Oracle 11g or above
* Allow connections from Estuary Flow to your Oracle database (if they exist in separate VPCs)
* Create a dedicated read-only Estuary Flow user with access to all tables needed for replication

## Setup
Follow the steps below to set up the OracleDB connector.

### Create a Dedicated User

Creating a dedicated database user with read-only access is recommended for better permission control and auditing.

1. To create the user, run the following commands against your database:

```sql
CREATE USER estuary_flow_user IDENTIFIED BY <your_password_here>;
GRANT CREATE SESSION TO estuary_flow_user;
```

2. Next, grant the user read-only access to the relevant tables. The simplest way is to grant read access to all tables in the schema as follows:

```sql
GRANT SELECT ANY TABLE TO estuary_flow_user;
GRANT FLASHBACK ANY TABLE to estuary_flow_user;
```

3. Alternatively, you can be more granular and grant access to specific tables in different schemas:

```sql
GRANT SELECT ON "<schema_a>"."<table_1>" TO estuary_flow_user;
GRANT FLASHBACK ON "<schema_a>"."<table_1>" to estuary_flow_user;
GRANT SELECT ON "<schema_b>"."<table_2>" TO estuary_flow_user;
GRANT FLASHBACK ON "<schema_b>"."<table_2>" TO estuary_flow_user;

-- In this case you need to also grant access to metadata views
GRANT SELECT ON V$DATABASE TO estuary_flow_user;
```

4. Finally you need to grant the user access to read metadata from the database:

```sql
GRANT SELECT_CATALOG_ROLE TO estuary_flow_user;
```

5. Your database user should now be ready for use with Estuary Flow.

### Recommended Database Configuration

In order to use Flashback and ensure consistency of data, we recommend setting the `UNDO_RETENTION` configuration to at least 7 days, or at minimum a couple of days. See [UNDO_RETENTION](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/UNDO_RETENTION.html) in Oracle docs. Example query to set retention to 2 days:

```sql
ALTER SYSTEM SET UNDO_RETENTION = 172800;
```

### Include Schemas for Discovery
In your Oracle configuration, you can specify the schemas that Flow should look at when discovering tables. The schema names are case-sensitive and will default to the upper-cased user if empty. If the user does not have access to the configured schemas, no tables will be discovered.

## Configuration
You configure connectors either in the Flow web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the OracleDB Flashback source connector.

To allow secure connections via SSH tunneling:
  * Follow the guide to [configure an SSH server for tunneling](/guides/connect-network/)
  * When you configure your connector as described in the [configuration](#configuration) section above, including the additional `networkTunnel` configuration to enable the SSH tunnel. See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

### Properties

#### Endpoint
| Property                                    | Title                           | Description                                                                                                                                                                                                                                                                     | Type    | Required/Default |
| -----------                                 | --------                        | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------  | ---------------- |
| `/address`                                  | Address                         | The connection string for connecting to the database, either in the format of host:port/SID or a connect descriptor such as (address=(protocol=tcps)(host=...)...)                                                                                                              | string  | Required         |
| `/credentials`                              | Credentials                     | Credentials for authenticating with the database. Wallet and Username & Password authentication are supported.                                                                                                                                                                  | object  |                  |
| `/credentials/credentials_title`            | Credentials Title               | Authentication method to use, one of "Wallet" or "Username & Password"                                                                                                                                                                                                          | string  | Required         |
| `/credentials/username`                     | Username                        | The username which is used to access the database.                                                                                                                                                                                                                              | string  | Required         |
| `/credentials/password`                     | Password                        | The password associated with the username.                                                                                                                                                                                                                                      | string  | Required         |
| `/credentials/tnsnames`                     | tnsnames                        | The tnsnames.ora file from the wallet.                                                                                                                                                                                                                                          | string  |                  |
| `/credentials/ewallet`                      | ewallet                         | The ewallet.pem file from the wallet.                                                                                                                                                                                                                                           | string  |                  |
| `/credentials/wallet_password`              | Wallet Password                 | Password of the wallet, if protected.                                                                                                                                                                                                                                           | string  |                  |
| `/advanced/backfill_chunk_size`             | Backfill Chunk Size             | The number of rows which should be fetched from the database in a single backfill query.                                                                                                                                                                                        | integer | `50000`          |
| `/advanced/skip_flashback_retention_checks` | Skip Flashback Retention Checks | Skip Flashback retention checks. Use this cautiously as we cannot guarantee consistency if Flashback retention is not sufficient.                                                                                                                                               | integer | `false`          |
| `/advanced/default_interval`                | Default Interval                | Default interval between updates for all resources. Can be overwritten by each resource.                                                                                                                                                                                        | integer | `PT5M`           |


#### Bindings

| Property        | Title     | Description                                                                                                         | Type      | Required/Default |
| -------         | ------    | ------                                                                                                              | --------- | --------         |
| **`/name`**     | Name      | The table name                                                                                                      | string    | Required         |
| **`/schema`**   | Schema    | In Oracle tables reside in a schema that points to the user that owns the table.                                    | string    | Required         |
| **`/interval`** | Interval  | Interval between updates for this resource                                                                          | string    | `PT5M`           |


### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-oracle-flashback:dev
        config:
          address: "database-1.ukqdmzdqvvsz.us-east-2.rds.amazonaws.com:1521/ORCL"
          user: "flow_capture"
          credentials:
            credentials_title: Username & Password
            username: ADMIN
            password: secret
          networkTunnel:
            sshForwarding:
              privateKey: -----BEGIN RSA PRIVATE KEY-----\n...
              sshEndpoint: ssh://ec2-user@19.220.21.33:22

    bindings:
      - resource:
          name: ${TABLE_NAME}
          schema: ${TABLE_NAMESPACE}
          interval: PT5M
        target: ${PREFIX}/${COLLECTION_NAME}
```
