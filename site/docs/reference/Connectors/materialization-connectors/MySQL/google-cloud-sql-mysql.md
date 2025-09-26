---
description: This connector materializes Flow collections into tables in a Google Cloud SQL for MySQL database.
---

# Google Cloud SQL for MySQL

This connector materializes Flow collections into tables in a MySQL database.

It is available for use in the Flow web application. For local development or
open-source workflows,
[`ghcr.io/estuary/materialize-mysql:dev`](https://ghcr.io/estuary/materialize-mysql:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

- A MySQL database to which to materialize, and user credentials.
  - MySQL versions 5.7 and later are supported
  - The connector will create new tables in the database per your specification,
    so user credentials must have access to create new tables.
  - The `local_infile` global variable must be enabled. You can enable this
    setting by running `SET GLOBAL local_infile = true` in your database.
- At least one Flow collection

## Setup

### Connecting Directly to Google Cloud SQL

1. [Enable public IP on your database](https://cloud.google.com/sql/docs/mysql/configure-ip#add) and add the [Estuary Flow IP addresses](/reference/allow-ip-addresses) as authorized IP addresses.

### Connect With SSH Tunneling

To allow SSH tunneling to a database instance hosted on Google Cloud, you must set up a virtual machine (VM).

1. Begin by finding your public SSH key on your local machine.
   In the `.ssh` subdirectory of your user home directory,
   look for the PEM file that contains the private SSH key. Check that it starts with `-----BEGIN RSA PRIVATE KEY-----`,
   which indicates it is an RSA-based file.

   - If no such file exists, generate one using the command:

   ```console
      ssh-keygen -m PEM -t rsa
   ```

   - If a PEM file exists, but starts with `-----BEGIN OPENSSH PRIVATE KEY-----`, convert it with the command:

   ```console
      ssh-keygen -p -N "" -m pem -f /path/to/key
   ```

   - If your Google login differs from your local username, generate a key that includes your Google email address as a comment:

   ```console
      ssh-keygen -m PEM -t rsa -C user@domain.com
   ```

2. [Create and start a new VM in GCP](https://cloud.google.com/compute/docs/instances/create-start-instance), [choosing an image that supports OS Login](https://cloud.google.com/compute/docs/images/os-details#user-space-features).

3. [Add your public key to the VM](https://cloud.google.com/compute/docs/connect/add-ssh-keys).

4. [Reserve an external IP address](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address) and connect it to the VM during setup.
   Note the generated address.

:::tip Configuration Tip
To configure the connector, you must specify the database address in the format
`host:port`. (You can also supply `host` only; the connector will use the port `3306` by default, which is correct in many cases.)
You can find the host and port in the following locations in each platform's console:

- Google Cloud SQL: host as Private IP Address; port is always `3306`. You may need to [configure private IP](https://cloud.google.com/sql/docs/mysql/configure-private-ip) on your database.
:::

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a MySQL materialization, which will direct one or more of your Flow collections to your desired tables, or views, in the database.

### Properties

#### Endpoint

| Property                    | Title                  | Description                                                                                                                                                                                                                                                                                                                         | Type   | Required/Default |
| --------------------------- | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/database`**             | Database               | Name of the logical database to materialize to.                                                                                                                                                                                                                                                                                     | string | Required         |
| **`/address`**              | Address                | Host and port of the database. If only the host is specified, port will default to `3306`.                                                                                                                                                                                                                                          | string | Required         |
| **`/password`**             | Password               | Password for the specified database user.                                                                                                                                                                                                                                                                                           | string | Required         |
| **`/user`**                 | User                   | Database user to connect as.                                                                                                                                                                                                                                                                                                        | string | Required         |
| `/timezone`                 | Timezone               | Timezone to use when materializing datetime columns. Should normally be left blank to use the database's 'time_zone' system variable. Only required if the 'time_zone' system variable cannot be read. Must be a valid IANA time zone name or +HH:MM offset. Takes precedence over the 'time_zone' system variable if both are set. | string |                  |
| `/advanced`                 | Advanced Options       | Options for advanced users. You should not typically need to modify these.                                                                                                                                                                                                                                                          | object |                  |
| `/advanced/sslmode`         | SSL Mode               | Overrides SSL connection behavior by setting the 'sslmode' parameter.                                                                                                                                                                                                                                                               | string |                  |
| `/advanced/ssl_server_ca`   | SSL Server CA          | Optional server certificate authority to use when connecting with custom SSL mode                                                                                                                                                                                                                                                   | string |                  |
| `/advanced/ssl_client_cert` | SSL Client Certificate | Optional client certificate to use when connecting with custom SSL mode.                                                                                                                                                                                                                                                            | string |                  |
| `/advanced/ssl_client_key`  | SSL Client Key         | Optional client key to use when connecting with custom SSL mode.                                                                                                                                                                                                                                                                    | string |                  |

### Setting the MySQL time zone

MySQL's [`time_zone` server system variable](https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_time_zone) is set to `SYSTEM` by default.

If you intend to materialize collections including fields of with `format: date-time` or `format: time`,
and `time_zone` is set to `SYSTEM`,
Flow won't be able to detect the time zone and convert datetimes to the
appropriate timezone when materializing.
To avoid this, you must explicitly set the time zone for your database.

You can:

- Specify a numerical offset from UTC.

  - For MySQL version 8.0.19 or higher, values from `-13:59` to `+14:00`, inclusive, are permitted.
  - Prior to MySQL 8.0.19, values from `-12:59` to `+13:00`, inclusive, are permitted

- Specify a named timezone in [IANA timezone format](https://www.iana.org/time-zones).

For example, if you're located in New Jersey, USA, you could set `time_zone` to `-05:00` or `-04:00`, depending on the time of year.
Because this region observes daylight savings time, you'd be responsible for changing the offset.
Alternatively, you could set `time_zone` to `America/New_York`, and time changes would occur automatically.

If using IANA time zones, your database must include time zone tables. [Learn more in the MySQL docs](https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html).

:::tip Materialize Timezone Configuration
If you are unable to set the `time_zone` in the database and need to materialize
collections with date-time or time fields, the materialization can be configured
to assume a time zone using the `timezone` configuration property (see above). The `timezone` configuration property can be set as a numerical offset or IANA timezone format.
:::

#### SSL Mode

Possible values:

- `disabled`: A plain unencrypted connection is established with the server
- `preferred`: Only use SSL connection if the server asks for it
- `required`: Connect using an SSL connection, but do not verify the server's
  certificate.
- `verify_ca`: Connect using an SSL connection, and verify the server's
  certificate against the given SSL Server CA, but does not verify the server's
  hostname. This option is most commonly used when connecting to an
  IP address which does not have a hostname to be verified. When using this mode, SSL Server
  CA must be provided.
- `verify_identity`: Connect using an SSL connection, verify the server's
  certificate and the server's hostname. This is the most secure option. When using this mode, SSL Server
  CA must be provided.

Optionally, SSL Client Certificate and Key can be provided if necessary to
authorize the client.

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
        image: ghcr.io/estuary/materialize-mysql:dev
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

### Setup

1. Allow connections between the database and Estuary Flow. There are two ways to do this: by granting direct access to Flow's IP or by creating an SSH tunnel.

   1. To allow direct access:

      - [Enable public IP on your database](https://cloud.google.com/sql/docs/mysql/configure-ip#add) and add the [Estuary Flow IP addresses](/reference/allow-ip-addresses) as authorized IP addresses.

   2. To allow secure connections via SSH tunneling:
      - Follow the guide to [configure an SSH server for tunneling](/guides/connect-network/)
      - When you configure your connector as described in the [configuration](#configuration) section above, including the additional `networkTunnel` configuration to enable the SSH tunnel. See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

2. Configure your connector as described in the [configuration](#configuration) section above,
   with the addition of the `networkTunnel` stanza to enable the SSH tunnel, if using.
   See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
   for additional details and a sample.

:::tip Configuration Tip
To configure the connector, you must specify the database address in the format
`host:port`. (You can also supply `host` only; the connector will use the port `3306` by default, which is correct in many cases.)
You can find the host in the GCP console as "Private IP Address". The port is always `3306`. You may need to [configure private IP](https://cloud.google.com/sql/docs/mysql/configure-private-ip) on your database.
:::

3. Create the `flow_materialize` user with `All` privileges on your database. This user will need the ability to create and update the `flow_materializations` table.

```sql
CREATE USER IF NOT EXISTS flow_materialize
  IDENTIFIED BY 'secret'
  COMMENT 'User account for Flow MySQL data materialization';
GRANT ALL PRIVELEGES ON <database>.* TO 'flow_materialize';
```

4. In the Cloud Console, note the instance's host under Public IP Address. Its port will always be `3306`.
   Together, you'll use the host:port as the `address` property when you configure the connector.

## Delta updates

This connector supports both standard (merge) and [delta updates](/concepts/materialization/#delta-updates).
The default is to use standard updates.

## Date & times

Date and time fields that are part of collections, which specify a `format:
date-time` for the field, are automatically converted to UTC and
persisted as UTC `DATETIME` in MySQL.

## Reserved words

MySQL has a list of reserved words that must be quoted in order to be used as an identifier.
Flow considers all the reserved words in the official [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/keywords.html).

These reserved words are listed in the table below. Flow automatically quotes fields that are in this list.

| Reserved words |               |               |               |               |
| -------------- | ------------- | ------------- | ------------- | ------------- |
| accessible     | clone         | describe      | float         | int           |
| account        | close         | description   | float4        | int1          |
| action         | coalesce      | des_key_file  | float8        | int2          |
| active         | code          | deterministic | flush         | int3          |
| add            | collate       | diagnostics   | following     | int4          |
| admin          | collation     | directory     | follows       | int8          |
| after          | column        | disable       | for           | integer       |
| against        | columns       | discard       | force         | intersect     |
| aggregate      | column_format | disk          | foreign       | interval      |
| algorithm      | column_name   | distinct      | format        | into          |
| all            | comment       | distinctrow   | found         | invisible     |
| alter          | commit        | div           | from          | invoker       |
| always         | committed     | do            | full          | io            |
| analyse        | compact       | double        | fulltext      | io_after_gtid |
| analyze        | completion    | drop          | function      | io_before_gti |
| and            | component     | dual          | general       | io_thread     |
| any            | compressed    | dumpfile      | generate      | ipc           |
| array          | compression   | duplicate     | generated     | is            |
| as             | concurrent    | dynamic       | geomcollectio | isolation     |
| asc            | condition     | each          | geometry      | issuer        |
| ascii          | connection    | else          | geometrycolle | iterate       |
| asensitive     | consistent    | elseif        | get           | join          |
| at             | constraint    | empty         | get_format    | json          |
| attribute      | constraint_ca | enable        | get_master_pu | json_table    |
| authenticatio  | constraint_na | enclosed      | get_source_pu | json_value    |
| autoextend_si  | constraint_sc | encryption    | global        | key           |
| auto_incremen  | contains      | end           | grant         | keyring       |
| avg            | context       | ends          | grants        | keys          |
| avg_row_lengt  | continue      | enforced      | group         | key_block_siz |
| backup         | convert       | engine        | grouping      | kill          |
| before         | cpu           | engines       | groups        | lag           |
| begin          | create        | engine_attrib | group_replica | language      |
| between        | cross         | enum          | gtid_only     | last          |
| bigint         | cube          | error         | handler       | last_value    |
| binary         | cume_dist     | errors        | hash          | lateral       |
| binlog         | current       | escape        | having        | lead          |
| bit            | current_date  | escaped       | help          | leading       |
| blob           | current_time  | event         | high_priority | leave         |
| block          | current_times | events        | histogram     | leaves        |
| bool           | current_user  | every         | history       | left          |
| boolean        | cursor        | except        | host          | less          |
| both           | cursor_name   | exchange      | hosts         | level         |
| btree          | data          | exclude       | hour          | like          |
| buckets        | database      | execute       | hour_microsec | limit         |
| bulk           | databases     | exists        | hour_minute   | linear        |
| by             | datafile      | exit          | hour_second   | lines         |
| byte           | date          | expansion     | identified    | linestring    |
| cache          | datetime      | expire        | if            | list          |
| call           | day           | explain       | ignore        | load          |
| cascade        | day_hour      | export        | ignore_server | local         |
| cascaded       | day_microseco | extended      | import        | localtime     |
| case           | day_minute    | extent_size   | in            | localtimestam |
| catalog_name   | day_second    | factor        | inactive      | lock          |
| chain          | deallocate    | failed*login* | index         | locked        |
| challenge_res  | dec           | false         | indexes       | locks         |
| change         | decimal       | fast          | infile        | logfile       |
| changed        | declare       | faults        | initial       | logs          |
| channel        | default       | fetch         | initial_size  | long          |
| char           | default_auth  | fields        | initiate      | longblob      |
| character      | definer       | file          | inner         | longtext      |
| charset        | definition    | file_block_si | inout         | loop          |
| check          | delayed       | filter        | insensitive   | low_priority  |
| checksum       | delay_key_wri | finish        | insert        | master        |
| cipher         | delete        | first         | insert_method | master_auto_p |
| class_origin   | dense_rank    | first_value   | install       | master_bind   |
| client         | desc          | fixed         | instance      | master_compre |
| master_connec  | never         | preserve      | restrict      | source_host   |
| master_delay   | new           | prev          | resume        | source_log_fi |
| master_heartb  | next          | primary       | retain        | source_log_po |
| master_host    | no            | privileges    | return        | source_passwo |
| master_log_fi  | nodegroup     | privilege_che | returned_sqls | source_port   |
| master_log_po  | none          | procedure     | returning     | source_public |
| master_passwo  | not           | process       | returns       | source*retry* |
| master_port    | nowait        | processlist   | reuse         | source_ssl    |
| master_public  | no_wait       | profile       | reverse       | source_ssl_ca |
| master*retry*  | no_write_to_b | profiles      | revoke        | source_ssl_ca |
| master_server  | nth_value     | proxy         | right         | source_ssl_ce |
| master_ssl     | ntile         | purge         | rlike         | source_ssl_ci |
| master_ssl_ca  | null          | quarter       | role          | source_ssl_cr |
| master_ssl_ca  | nulls         | query         | rollback      | source_ssl_cr |
| master_ssl_ce  | number        | quick         | rollup        | source_ssl_ke |
| master_ssl_ci  | numeric       | random        | rotate        | source_ssl_ve |
| master_ssl_cr  | nvarchar      | range         | routine       | source_tls_ci |
| master_ssl_cr  | of            | rank          | row           | source_tls_ve |
| master_ssl_ke  | off           | read          | rows          | source_user   |
| master_ssl_ve  | offset        | reads         | row_count     | source_zstd_c |
| master_tls_ci  | oj            | read_only     | row_format    | spatial       |
| master_tls_ve  | old           | read_write    | row_number    | specific      |
| master_user    | on            | real          | rtree         | sql           |
| master_zstd_c  | one           | rebuild       | savepoint     | sqlexception  |
| match          | only          | recover       | schedule      | sqlstate      |
| maxvalue       | open          | recursive     | schema        | sqlwarning    |
| max_connectio  | optimize      | redofile      | schemas       | sql_after_gti |
| max_queries_p  | optimizer_cos | redo_buffer_s | schema_name   | sql_after_mts |
| max_rows       | option        | redundant     | second        | sql_before_gt |
| max_size       | optional      | reference     | secondary     | sql_big_resul |
| max_updates_p  | optionally    | references    | secondary_eng | sql_buffer_re |
| max_user_conn  | options       | regexp        | secondary_eng | sql_cache     |
| medium         | or            | registration  | secondary_loa | sql_calc_foun |
| mediumblob     | order         | relay         | secondary_unl | sql_no_cache  |
| mediumint      | ordinality    | relaylog      | second_micros | sql_small_res |
| mediumtext     | organization  | relay_log_fil | security      | sql_thread    |
| member         | others        | relay_log_pos | select        | sql_tsi_day   |
| memory         | out           | relay_thread  | sensitive     | sql_tsi_hour  |
| merge          | outer         | release       | separator     | sql_tsi_minut |
| message_text   | outfile       | reload        | serial        | sql_tsi_month |
| microsecond    | over          | remote        | serializable  | sql_tsi_quart |
| middleint      | owner         | remove        | server        | sql_tsi_secon |
| migrate        | pack_keys     | rename        | session       | sql_tsi_week  |
| minute         | page          | reorganize    | set           | sql_tsi_year  |
| minute_micros  | parser        | repair        | share         | srid          |
| minute_second  | partial       | repeat        | show          | ssl           |
| min_rows       | partition     | repeatable    | shutdown      | stacked       |
| mod            | partitioning  | replace       | signal        | start         |
| mode           | partitions    | replica       | signed        | starting      |
| modifies       | password      | replicas      | simple        | starts        |
| modify         | password_lock | replicate*do* | skip          | stats_auto_re |
| month          | path          | replicate*do* | slave         | stats_persist |
| multilinestri  | percent_rank  | replicate_ign | slow          | stats*sample* |
| multipoint     | persist       | replicate_ign | smallint      | status        |
| multipolygon   | persist_only  | replicate_rew | snapshot      | stop          |
| mutex          | phase         | replicate_wil | socket        | storage       |
| mysql_errno    | plugin        | replicate_wil | some          | stored        |
| name           | plugins       | replication   | soname        | straight_join |
| names          | plugin_dir    | require       | sounds        | stream        |
| national       | point         | require_row_f | source        | string        |
| natural        | polygon       | reset         | source_auto_p | subclass_orig |
| nchar          | port          | resignal      | source_bind   | subject       |
| ndb            | precedes      | resource      | source_compre | subpartition  |
| ndbcluster     | preceding     | respect       | source_connec | subpartitions |
| nested         | precision     | restart       | source_delay  | super         |
| network_names  | prepare       | restore       | source_heartb | suspend       |
| swaps          | timestampdiff | undo_buffer_s | utc_date      | when          |
| switches       | tinyblob      | unicode       | utc_time      | where         |
| system         | tinyint       | uninstall     | utc_timestamp | while         |
| table          | tinytext      | union         | validation    | window        |
| tables         | tls           | unique        | value         | with          |
| tablespace     | to            | unknown       | values        | without       |
| table_checksu  | trailing      | unlock        | varbinary     | work          |
| table_name     | transaction   | unregister    | varchar       | wrapper       |
| temporary      | trigger       | unsigned      | varcharacter  | write         |
| temptable      | triggers      | until         | variables     | x509          |
| terminated     | true          | update        | varying       | xa            |
| text           | truncate      | upgrade       | vcpu          | xid           |
| than           | type          | url           | view          | xml           |
| then           | types         | usage         | virtual       | xor           |
| thread_priori  | unbounded     | use           | visible       | year          |
| ties           | uncommitted   | user          | wait          | year_month    |
| time           | undefined     | user_resource | warnings      | zerofill      |
| timestamp      | undo          | use_frm       | week          | zone          |
| timestampadd   | undofile      | using         | weight_string |

## Changelog

The changelog includes a list of breaking changes made to this connector. Backwards-compatible changes are not listed.

**Proceed with caution when editing materializations created with previous versions of this connector;
editing always upgrades your materialization to the latest connector version.**

#### V1: 2023-08-21

- First version
