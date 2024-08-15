# OracleDB
This connector captures data from OracleDB into Flow collections using [Oracle Logminer](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html#GUID-2555A155-01E3-483E-9FC6-2BDC2D8A4093).

It is available for use in the Flow web application. For local development or open-source workflows, `ghcr.io/estuary/source-oracle:dev` provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

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
```

3. Alternatively, you can be more granular and grant access to specific tables in different schemas:

```sql
GRANT SELECT ON "<schema_a>"."<table_1>" TO estuary_flow_user;
GRANT SELECT ON "<schema_b>"."<table_2>" TO estuary_flow_user;
```

4. Create a watermarks table:
```sql
CREATE TABLE estuary_flow_user.FLOW_WATERMARKS(SLOT varchar(1000) PRIMARY KEY, WATERMARK varchar(4000));
```

5. Finally you need to grant the user access to use logminer, read metadata from the database and write to the watermarks table:

```sql
GRANT SELECT_CATALOG_ROLE TO estuary_flow_user;
GRANT EXECUTE_CATALOG_ROLE TO estuary_flow_user;
GRANT SELECT ON V$DATABASE TO estuary_flow_user;
GRANT SELECT ANY TABLE TO estuary_flow_user;
GRANT SELECT ON V$LOG TO estuary_flow_user;
GRANT LOGMINING TO estuary_flow_user;

GRANT INSERT, UPDATE ON estuary_flow_user.FLOW_WATERMARKS TO estuary_flow_user;
```

5. Enable supplemental logging:

For normal instances use:
```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

For Amazon RDS instances use:
```sql
BEGIN rdsadmin.rdsadmin_util.alter_supplemental_logging(p_action => 'ADD', p_type   => 'ALL'); end;
```

6. Create a watermarks table and grant permis

6. Your database user should now be ready for use with Estuary Flow.

### Include Schemas for Discovery
In your Oracle configuration, you can specify the schemas that Flow should look at when discovering tables. The schema names are case-sensitive. If the user does not have access to a certain schema, no tables from that schema will be discovered.

## Configuration
You configure connectors either in the Flow web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the OracleDB source connector.

To allow secure connections via SSH tunneling:
  * Follow the guide to [configure an SSH server for tunneling](/guides/connect-network/)
  * When you configure your connector as described in the [configuration](#configuration) section above, including the additional `networkTunnel` configuration to enable the SSH tunnel. See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

### Properties

#### Endpoint
| Property                           | Title                  | Description                                                                                                                                                                                                                                                                                                                                     | Type    | Required/Default               |
| -----------                        | --------               | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                 | ------  | ----------------               |
| `/address`                         | Address                | The host or host:port at which the database can be reached.                                                                                                                                                                                                                                                                                     | string  | Required                       |
| `/user`                            | Username               | The database user to authenticate as.                                                                                                                                                                                                                                                                                                           | string  | Required                       |
| `/password`                        | Password               | Password for the specified database user.                                                                                                                                                                                                                                                                                                       | string  | Required                       |
| `/database`                        | Database               | Logical database name to capture from. Defaults to ORCL.                                                                                                                                                                                                                                                                                        | string  | Required                       |
| `/historyMode`                     | History Mode           | Capture change events without reducing them to a final state.                                                                                                                                                                                                                                                                                   | boolean | `false`                        |
| `/advanced/skip_backfills`         | Skip Backfills         | A comma-separated list of fully-qualified table names which should not be backfilled.                                                                                                                                                                                                                                                           | string  |                                |
| `/advanced/watermarksTable`        | Watermarks Table       | The name of the table used for watermark writes during backfills. Must be fully-qualified in '&lt;schema&gt;.table' form.                                                                                                                                                                                                                       | string  | `&lt;USER&gt;.FLOW_WATERMARKS` |
| `/advanced/backfill_chunk_size`    | Backfill Chunk Size    | The number of rows which should be fetched from the database in a single backfill query.                                                                                                                                                                                                                                                        | integer | `50000`                        |
| `/advanced/incremental_chunk_size` | Incremental Chunk Size | The number of rows which should be fetched from the database in a single incremental query.                                                                                                                                                                                                                                                     | integer | `10000`                        |
| `/advanced/incremental_scn_range`  | Incremental SCN Range  | The SCN range captured at every iteration.                                                                                                                                                                                                                                                                                                      | integer | `50000`                        |
| `/advanced/dictionary_mode`        | Dictionary Mode        | How should dictionaries be used in Logminer: one of online or extract. When using online mode schema changes to the table may break the capture but resource usage is limited. When using extract mode schema changes are handled gracefully but more resources of your database (including disk) are used by the process. Defaults to extract. | string  | `extract`                      |
| `/advanced/discover_schemas`       | Discover Schemas       | If this is specified only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas.                                                                                                                                                                                              | string  |                                |
| `/advanced/node_id`                | Node ID                | Node ID for the capture. Each node in a replication cluster must have a unique 32-bit ID. The specific value doesn't matter so long as it is unique. If unset or zero the connector will pick a value.                                                                                                                                          | integer |                                |


#### Bindings

| Property         | Title     | Description                                                                                | Type   | Required/Default |
|------------------|-----------|--------------------------------------------------------------------------------------------|--------|------------------|
| **`/namespace`** | Namespace | The [owner/schema](https://docs.oracle.com/database/121/CNCPT/intro.htm#CNCPT940) of the table.                                                         | string | Required         |
| **`/stream`**    | Stream    | Table name.                                                                                | string | Required         |


### Sample

```json
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-oracle:dev
        config:
          address: database-1.ukqdmzdqvvsz.us-east-2.rds.amazonaws.com:1521
          user: "flow_capture"
          password: secret
          database: ORCL
          historyMode: false
          advanced:
            incremental_scn_range: 50000
            dictionary_mode: extract
          networkTunnel:
            sshForwarding:
              privateKey: -----BEGIN RSA PRIVATE KEY-----\n...
              sshEndpoint: ssh://ec2-user@19.220.21.33:22
          
    bindings:
      - resource:
          namespace: ${TABLE_NAMESPACE}
          stream: ${TABLE_NAME}
        target: ${PREFIX}/${COLLECTION_NAME}
```

### Dictionary Modes

Oracle writes redo log files using triplet object ID, data object ID and object versions to identify different objects in the database, rather than their name. This applies to table names as well as column names. When reading data from the redo log files using Logminer, a "dictionary" is used to translate the object identification data into user-facing names of those objects. When interacting with the database directly an _online_ dictionary, which is essentially the latest dictionary that knows how to translate currently existing table and column names is used by the database and by Logminer, however when capturing historical data, it is possible that the names of these objects or even their identifiers have changed (due to an `ALTER TABLE` statement for example). In these instances the _online_ dictionary will be insufficient for translating the object identifiers into names and Logminer will complain about a dictionary mismatch.

To resolve this issue, it is possible to _extract_ a dictionary into the redo log files themselves, so that when there are schema changes, Logminer can automatically handle using the appropriate dictionary for the time period an event is from. This operation however uses CPU and RAM, as well as consuming disk over time.

Using Estuary's Oracle connector you get to choose which mode to operate it:

1. To extract the dictionary into the redo log files, the `extract` mode can be used (this is the default mode). Be aware that this mode leads to more resource usage (CPU, RAM and disk).
2. To always use the online dictionary, the `online` mode can be used. This mode is more efficient, but it cannot handle schema changes in tables, so only use this mode with caution and when table schemas are known not to change.

### Incremental SCN Range and Events Rate

At every iteration, the connector fetches changes in a specific SCN (System Change Number) range, this is roughly equivalent to a specific time range. Depending on how many events happen on the captured tables in a database (by default, a 50,000 range is captured in each iteration), the `advanced.incremental_scn_range` option can be updated to fit your needs:

1. If the database processes a large amount of events per unit of time, the connector and/or the database may experience resource shortages while trying to process the data. For example you may see the error `PGA memory used by the instance exceeds PGA_AGGREGATE_LIMIT` which indicates that the memory usage of the database instance has hit a limit. This can happen if too many events are being processed in one iteration. In these cases we recommend lowering the SCN range until the database and the connector are able to handle the load.
2. If the database does not have many events per time unit, a higher value can help with faster processing, although this is usually not necessary.

## Troubleshooting

1. If you see the following error when trying to connect:
```
ORA-01950: no privileges on tablespace 'USERS'
```

The SQL command below may resolve the issue:
```sql
ALTER USER estuary_flow_user QUOTA UNLIMITED ON USERS;
```

## Known Limitations

1. Table and column names longer than 30 characters are not supported by Logminer, and thus they are also not supported by this connector.
