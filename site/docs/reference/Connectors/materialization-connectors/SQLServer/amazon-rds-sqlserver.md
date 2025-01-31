---
description: This connector materializes Flow collections into tables in a Amazon RDS for SQL Server database.
---

# Amazon RDS for SQL Server

This connector materializes Flow collections into tables in a Microsoft SQLServer database.

It is available for use in the Flow web application. For local development or
open-source workflows,
[`ghcr.io/estuary/materialize-sqlserver:dev`](https://ghcr.io/estuary/materialize-sqlserver:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

- A SQLServer database to which to materialize, and user credentials.
  - SQLServer 2017 and later are supported
  - The connector will create new tables in the database per your specification,
    so user credentials must have access to create new tables.
- At least one Flow collection

## Setup Amazon RDS for SQL Server

1. Allow connections between the database and Estuary Flow. There are two ways to do this: by granting direct access to Flow's IP or by creating an SSH tunnel.

   1. To allow direct access:

      - [Modify the database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html), setting **Public accessibility** to **Yes**.
      - Edit the VPC security group associated with your database, or create a new VPC security group and associate it as described in [the Amazon documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html#Overview.RDSSecurityGroups.Create).Create a new inbound rule and a new outbound rule that allow all traffic from the [Estuary Flow IP addresses](/reference/allow-ip-addresses).

   2. To allow secure connections via SSH tunneling:
      - Follow the guide to [configure an SSH server for tunneling](../../../../../guides/connect-network/)
      - When you configure your connector as described in the [configuration](#configuration) section above, including the additional `networkTunnel` configuration to enable the SSH tunnel. See [Connecting to endpoints on secure networks](../../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

2. In your SQL client, connect to your instance as the default `sqlserver` user and issue the following commands.

```sql
USE <database>;
-- Create user and password for use with the connector.
CREATE LOGIN flow_materialize WITH PASSWORD = 'secret';
CREATE USER flow_materialize FOR LOGIN flow_materialize;
-- Grant control on the database to flow_materialize
GRANT CONTROL ON DATABASE::<database> TO flow_materialize;
```

3. In the [RDS console](https://console.aws.amazon.com/rds/), note the instance's Endpoint and Port. You'll need these for the `address` property when you configure the connector.

## Connecting to SQLServer

1. Allow connections between the database and Estuary Flow. There are two ways to do this: by granting direct access to Flow's IP or by creating an SSH tunnel.

   1. To allow direct access:

      - [Modify the database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html), setting **Public accessibility** to **Yes**.
      - Edit the VPC security group associated with your database, or create a new VPC security group and associate it as described in [the Amazon documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html#Overview.RDSSecurityGroups.Create). Create a new inbound rule and a new outbound rule that allow all traffic from the [Estuary Flow IP addresses](/reference/allow-ip-addresses).

   2. To allow secure connections via SSH tunneling:
      - Follow the guide to [configure an SSH server for tunneling](../../../../../guides/connect-network/)
      - When you configure your connector as described in the [configuration](#configuration) section above, including the additional `networkTunnel` configuration to enable the SSH tunnel. See [Connecting to endpoints on secure networks](../../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

2. In your SQL client, connect to your instance as the default `sqlserver` user and issue the following commands.

```sql
USE <database>;
-- Create user and password for use with the connector.
CREATE LOGIN flow_materialize WITH PASSWORD = 'Secret123!';
CREATE USER flow_materialize FOR LOGIN flow_materialize;
-- Grant control on the database to flow_materialize
GRANT CONTROL ON DATABASE::<database> TO flow_materialize;
```

3. In the [RDS console](https://console.aws.amazon.com/rds/), note the instance's Endpoint and Port. You'll need these for the `address` property when you configure the connector.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a SQLServer materialization, which will direct one or more of your Flow collections to your desired tables, or views, in the database.

### Properties

#### Endpoint

| Property        | Title    | Description                                                                                | Type   | Required/Default |
| --------------- | -------- | ------------------------------------------------------------------------------------------ | ------ | ---------------- |
| **`/database`** | Database | Name of the logical database to materialize to.                                            | string | Required         |
| **`/address`**  | Address  | Host and port of the database. If only the host is specified, port will default to `3306`. | string | Required         |
| **`/password`** | Password | Password for the specified database user.                                                  | string | Required         |
| **`/user`**     | User     | Database user to connect as.                                                               | string | Required         |

#### Bindings

| Property         | Title        | Description                                                                                                        | Type    | Required/Default |
| ---------------- | ------------ | ------------------------------------------------------------------------------------------------------------------ | ------- | ---------------- |
| **`/table`**     | Table        | Table name to materialize to. It will be created by the connector, unless the connector has previously created it. | string  | Required         |
| `/delta_updates` | Delta Update | Should updates to this table be done via delta updates.                                                            | boolean | `false`          |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-sqlserver:dev
        config:
          database: flow
          address: localhost:5432
          password: flow
          user: flow
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Delta updates

This connector supports both standard (merge) and [delta updates](../../../../concepts/materialization.md#delta-updates).
The default is to use standard updates.

## Reserved words

SQLServer has a list of reserved words that must be quoted in order to be used as an identifier.
Flow considers all the reserved words in the official [SQLServer documentation](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/reserved-keywords-transact-sql?view=sql-server-2017).

These reserved words are listed in the table below. Flow automatically quotes fields that are in this list.

| Reserved words |               |               |               |               |
| -------------- | ------------- | ------------- | ------------- | ------------- |
| absolute       | connect       | else          | intersect     | on            |
| action         | connection    | end           | intersection  | only          |
| ada            | constraint    | end-exec      | interval      | open          |
| add            | constraints   | equals        | into          | opendatasourc |
| admin          | constructor   | errlvl        | is            | openquery     |
| after          | contains      | escape        | isolation     | openrowset    |
| aggregate      | containstable | every         | iterate       | openxml       |
| alias          | continue      | except        | join          | operation     |
| all            | convert       | exception     | key           | option        |
| allocate       | corr          | exec          | kill          | or            |
| alter          | corresponding | execute       | language      | order         |
| and            | count         | exists        | large         | ordinality    |
| any            | covar_pop     | exit          | last          | out           |
| are            | covar_samp    | external      | lateral       | outer         |
| array          | create        | extract       | leading       | output        |
| as             | cross         | false         | left          | over          |
| asc            | cube          | fetch         | less          | overlaps      |
| asensitive     | cume_dist     | file          | level         | overlay       |
| assertion      | current       | fillfactor    | like          | pad           |
| asymmetric     | current_catal | filter        | like_regex    | parameter     |
| at             | current_date  | first         | limit         | parameters    |
| atomic         | current_defau | float         | lineno        | partial       |
| authorization  | current_path  | for           | ln            | partition     |
| avg            | current_role  | foreign       | load          | pascal        |
| backup         | current_schem | fortran       | local         | path          |
| before         | current_time  | found         | localtime     | percent       |
| begin          | current_times | free          | localtimestam | percent_rank  |
| between        | current_trans | freetext      | locator       | percentile_co |
| binary         | current_user  | freetexttable | lower         | percentile_di |
| bit            | cursor        | from          | map           | pivot         |
| bit_length     | cycle         | full          | match         | plan          |
| blob           | data          | fulltexttable | max           | position      |
| boolean        | database      | function      | member        | position_rege |
| both           | date          | fusion        | merge         | postfix       |
| breadth        | day           | general       | method        | precision     |
| break          | dbcc          | get           | min           | prefix        |
| browse         | deallocate    | global        | minute        | preorder      |
| bulk           | dec           | go            | mod           | prepare       |
| by             | decimal       | goto          | modifies      | preserve      |
| call           | declare       | grant         | modify        | primary       |
| called         | default       | group         | module        | print         |
| cardinality    | deferrable    | grouping      | month         | prior         |
| cascade        | deferred      | having        | multiset      | privileges    |
| cascaded       | delete        | hold          | names         | proc          |
| case           | deny          | holdlock      | national      | procedure     |
| cast           | depth         | host          | natural       | public        |
| catalog        | deref         | hour          | nchar         | raiserror     |
| char           | desc          | identity      | nclob         | range         |
| char_length    | describe      | identity_inse | new           | read          |
| character      | descriptor    | identitycol   | next          | reads         |
| character_len  | destroy       | if            | no            | readtext      |
| check          | destructor    | ignore        | nocheck       | real          |
| checkpoint     | deterministic | immediate     | nonclustered  | reconfigure   |
| class          | diagnostics   | in            | none          | recursive     |
| clob           | dictionary    | include       | normalize     | ref           |
| close          | disconnect    | index         | not           | references    |
| clustered      | disk          | indicator     | null          | referencing   |
| coalesce       | distinct      | initialize    | nullif        | regr_avgx     |
| collate        | distributed   | initially     | numeric       | regr_avgy     |
| collation      | domain        | inner         | object        | regr_count    |
| collect        | double        | inout         | occurrences_r | regr_intercep |
| column         | drop          | input         | octet_length  | regr_r2       |
| commit         | dump          | insensitive   | of            | regr_slope    |
| completion     | dynamic       | insert        | off           | regr_sxx      |
| compute        | each          | int           | offsets       | regr_sxy      |
| condition      | element       | integer       | old           | regr_syy      |
| relative       | semanticsimil | structure     | truncate      | window        |
| release        | semanticsimil | submultiset   | try_convert   | with          |
| replication    | sensitive     | substring     | tsequal       | within        | group |
| restore        | sequence      | substring_reg | uescape       | within        |
| restrict       | session       | sum           | under         | without       |
| result         | session_user  | symmetric     | union         | work          |
| return         | set           | system        | unique        | write         |
| returns        | sets          | system_user   | unknown       | writetext     |
| revert         | setuser       | table         | unnest        | xmlagg        |
| revoke         | shutdown      | tablesample   | unpivot       | xmlattributes |
| right          | similar       | temporary     | update        | xmlbinary     |
| role           | size          | terminate     | updatetext    | xmlcast       |
| rollback       | smallint      | textsize      | upper         | xmlcomment    |
| rollup         | some          | than          | usage         | xmlconcat     |
| routine        | space         | then          | use           | xmldocument   |
| row            | specific      | time          | user          | xmlelement    |
| rowcount       | specifictype  | timestamp     | using         | xmlexists     |
| rowguidcol     | sql           | timezone_hour | value         | xmlforest     |
| rows           | sqlca         | timezone_minu | values        | xmliterate    |
| rule           | sqlcode       | to            | var_pop       | xmlnamespaces |
| save           | sqlerror      | top           | var_samp      | xmlparse      |
| savepoint      | sqlexception  | trailing      | varchar       | xmlpi         |
| schema         | sqlstate      | tran          | variable      | xmlquery      |
| scope          | sqlwarning    | transaction   | varying       | xmlserialize  |
| scroll         | start         | translate     | view          | xmltable      |
| search         | state         | translate_reg | waitfor       | xmltext       |
| second         | statement     | translation   | when          | xmlvalidate   |
| section        | static        | treat         | whenever      | year          |
| securityaudit  | statistics    | trigger       | where         | zone          |
| select         | stddev_pop    | trim          | while         |
| semantickeyph  | stddev_samp   | true          | width_bucket  |

## Changelog

The changelog includes a list of breaking changes made to this connector. Backwards-compatible changes are not listed.

**Proceed with caution when editing materializations created with previous versions of this connector;
editing always upgrades your materialization to the latest connector version.**

#### V1: 2023-09-01

- First version
