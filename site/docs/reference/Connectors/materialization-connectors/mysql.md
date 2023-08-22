# MySQL

This connector materializes Flow collections into tables in a MySQL database.

It is available for use in the Flow web application. For local development or
open-source workflows,
[`ghcr.io/estuary/materialize-mysql:dev`](https://ghcr.io/estuary/materialize-mysql:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* A MySQL database to which to materialize, and user credentials.
  * MySQL versions 5.7 and later are supported
  * The connector will create new tables in the database per your specification,
    so user credentials must have access to create new tables.
  * The `local_infile` global variable must be enabled. You can enable this
    setting by running `SET GLOBAL local_infile = true` in your database.
* At least one Flow collection

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a MySQL materialization, which will direct one or more of your Flow collections to your desired tables, or views, in the database.

### Properties

#### Endpoint

| Property                    | Title                  | Description                                                                                | Type   | Required/Default |
|-----------------------------|------------------------|--------------------------------------------------------------------------------------------|--------|------------------|
| **`/database`**             | Database               | Name of the logical database to materialize to.                                            | string | Required         |
| **`/address`**              | Address                | Host and port of the database. If only the host is specified, port will default to `3306`. | string | Required         |
| **`/password`**             | Password               | Password for the specified database user.                                                  | string | Required         |
| **`/user`**                 | User                   | Database user to connect as.                                                               | string | Required         |
| `/advanced`                 | Advanced Options       | Options for advanced users. You should not typically need to modify these.                 | object |                  |
| `/advanced/sslmode`         | SSL Mode               | Overrides SSL connection behavior by setting the 'sslmode' parameter.                      | string |                  |
| `/advanced/ssl_server_ca`   | SSL Server CA          | Optional server certificate authority to use when connecting with custom SSL mode          | string |                  |
| `/advanced/ssl_client_cert` | SSL Client Certificate | Optional client certificate to use when connecting with custom SSL mode.                   | string |                  |
| `/advanced/ssl_client_key`  | SSL Client Key         | Optional client key to use when connecting with custom SSL mode.                           | string |                  |

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
 
#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/additional_table_create_sql` | Additional Table Create SQL | Additional SQL statement(s) to be run in the same transaction that creates the table. | string |  |
| `/delta_updates` | Delta Update | Should updates to this table be done via delta updates. | boolean | `false` |
| **`/table`** | Table | Table name to materialize to. It will be created by the connector, unless the connector has previously created it. | string | Required |

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

## MySQL on managed cloud platforms

In addition to standard MySQL, this connector supports cloud-based MySQL instances.
To connect securely, you must use an SSH tunnel.

Google Cloud Platform, Amazon Web Service, and Microsoft Azure are currently supported.
You may use other cloud platforms, but Estuary doesn't guarantee performance.


### Setup

You must configure your database to allow connections from Estuary.
The recommended method is to whitelist Estuary Flow's IP address.

* **Amazon RDS and Amazon Aurora**: Edit the VPC security group associated with your database instance, or create a new VPC security group and associate it with the database instance.
   * [Modify the instance](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html), choosing **Publicly accessible** in the **Connectivity** settings.

   * Refer to the [steps in the Amazon documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html#Overview.RDSSecurityGroups.Create).
   Create a new inbound rule and a new outbound rule that allow all traffic from the IP address `34.121.207.128`.

* **Google Cloud SQL**: [Enable public IP on your database](https://cloud.google.com/sql/docs/mysql/configure-ip#add) and add `34.121.207.128` as an authorized IP address.

* **Azure Database For MySQL**: Create a new [firewall rule](https://learn.microsoft.com/en-us/azure/mysql/single-server/how-to-manage-firewall-using-portal) that grants access to the IP address `34.121.207.128`.

Alternatively, you can allow secure connections via SSH tunneling. To do so:

1. Refer to the [guide](../../../../guides/connect-network/) to configure an SSH server on the cloud platform of your choice.

2. Configure your connector as described in the [configuration](#configuration) section above,
with the additional of the `networkTunnel` stanza to enable the SSH tunnel, if using.
See [Connecting to endpoints on secure networks](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
for additional details and a sample.

:::tip Configuration Tip
To configure the connector, you must specify the database address in the format
`host:port`. (You can also supply `host` only; the connector will use the port `3306` by default, which is correct in many cases.)
You can find the host and port in the following locations in each platform's console:
* Amazon RDS and Amazon Aurora: host as Endpoint; port as Port.
* Google Cloud SQL: host as Private IP Address; port is always `3306`. You may need to [configure private IP](https://cloud.google.com/sql/docs/mysql/configure-private-ip) on your database.
* Azure Database: host as Server Name; port under Connection Strings (usually `3306`).
:::

## Delta updates

This connector supports both standard (merge) and [delta updates](../../../concepts/materialization.md#delta-updates).
The default is to use standard updates.

## Reserved words

MySQL has a list of reserved words that must be quoted in order to be used as an identifier.
Flow considers all the reserved words in the official [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/keywords.html).

These reserved words are listed in the table below. Flow automatically quotes fields that are in this list.

|Reserved words| | | | |
|---|---|---|---|---|
|accessible|clone|describe|float|int|
|account|close|description|float4|int1|
|action|coalesce|des_key_file|float8|int2|
|active|code|deterministic|flush|int3|
|add|collate|diagnostics|following|int4|
|admin|collation|directory|follows|int8|
|after|column|disable|for|integer|
|against|columns|discard|force|intersect|
|aggregate|column_format|disk|foreign|interval|
|algorithm|column_name|distinct|format|into|
|all|comment|distinctrow|found|invisible|
|alter|commit|div|from|invoker|
|always|committed|do|full|io|
|analyse|compact|double|fulltext|io_after_gtid|
|analyze|completion|drop|function|io_before_gti|
|and|component|dual|general|io_thread|
|any|compressed|dumpfile|generate|ipc|
|array|compression|duplicate|generated|is|
|as|concurrent|dynamic|geomcollectio|isolation|
|asc|condition|each|geometry|issuer|
|ascii|connection|else|geometrycolle|iterate|
|asensitive|consistent|elseif|get|join|
|at|constraint|empty|get_format|json|
|attribute|constraint_ca|enable|get_master_pu|json_table|
|authenticatio|constraint_na|enclosed|get_source_pu|json_value|
|autoextend_si|constraint_sc|encryption|global|key|
|auto_incremen|contains|end|grant|keyring|
|avg|context|ends|grants|keys|
|avg_row_lengt|continue|enforced|group|key_block_siz|
|backup|convert|engine|grouping|kill|
|before|cpu|engines|groups|lag|
|begin|create|engine_attrib|group_replica|language|
|between|cross|enum|gtid_only|last|
|bigint|cube|error|handler|last_value|
|binary|cume_dist|errors|hash|lateral|
|binlog|current|escape|having|lead|
|bit|current_date|escaped|help|leading|
|blob|current_time|event|high_priority|leave|
|block|current_times|events|histogram|leaves|
|bool|current_user|every|history|left|
|boolean|cursor|except|host|less|
|both|cursor_name|exchange|hosts|level|
|btree|data|exclude|hour|like|
|buckets|database|execute|hour_microsec|limit|
|bulk|databases|exists|hour_minute|linear|
|by|datafile|exit|hour_second|lines|
|byte|date|expansion|identified|linestring|
|cache|datetime|expire|if|list|
|call|day|explain|ignore|load|
|cascade|day_hour|export|ignore_server|local|
|cascaded|day_microseco|extended|import|localtime|
|case|day_minute|extent_size|in|localtimestam|
|catalog_name|day_second|factor|inactive|lock|
|chain|deallocate|failed_login_|index|locked|
|challenge_res|dec|false|indexes|locks|
|change|decimal|fast|infile|logfile|
|changed|declare|faults|initial|logs|
|channel|default|fetch|initial_size|long|
|char|default_auth|fields|initiate|longblob|
|character|definer|file|inner|longtext|
|charset|definition|file_block_si|inout|loop|
|check|delayed|filter|insensitive|low_priority|
|checksum|delay_key_wri|finish|insert|master|
|cipher|delete|first|insert_method|master_auto_p|
|class_origin|dense_rank|first_value|install|master_bind|
|client|desc|fixed|instance|master_compre|
|master_connec|never|preserve|restrict|source_host|
|master_delay|new|prev|resume|source_log_fi|
|master_heartb|next|primary|retain|source_log_po|
|master_host|no|privileges|return|source_passwo|
|master_log_fi|nodegroup|privilege_che|returned_sqls|source_port|
|master_log_po|none|procedure|returning|source_public|
|master_passwo|not|process|returns|source_retry_|
|master_port|nowait|processlist|reuse|source_ssl|
|master_public|no_wait|profile|reverse|source_ssl_ca|
|master_retry_|no_write_to_b|profiles|revoke|source_ssl_ca|
|master_server|nth_value|proxy|right|source_ssl_ce|
|master_ssl|ntile|purge|rlike|source_ssl_ci|
|master_ssl_ca|null|quarter|role|source_ssl_cr|
|master_ssl_ca|nulls|query|rollback|source_ssl_cr|
|master_ssl_ce|number|quick|rollup|source_ssl_ke|
|master_ssl_ci|numeric|random|rotate|source_ssl_ve|
|master_ssl_cr|nvarchar|range|routine|source_tls_ci|
|master_ssl_cr|of|rank|row|source_tls_ve|
|master_ssl_ke|off|read|rows|source_user|
|master_ssl_ve|offset|reads|row_count|source_zstd_c|
|master_tls_ci|oj|read_only|row_format|spatial|
|master_tls_ve|old|read_write|row_number|specific|
|master_user|on|real|rtree|sql|
|master_zstd_c|one|rebuild|savepoint|sqlexception|
|match|only|recover|schedule|sqlstate|
|maxvalue|open|recursive|schema|sqlwarning|
|max_connectio|optimize|redofile|schemas|sql_after_gti|
|max_queries_p|optimizer_cos|redo_buffer_s|schema_name|sql_after_mts|
|max_rows|option|redundant|second|sql_before_gt|
|max_size|optional|reference|secondary|sql_big_resul|
|max_updates_p|optionally|references|secondary_eng|sql_buffer_re|
|max_user_conn|options|regexp|secondary_eng|sql_cache|
|medium|or|registration|secondary_loa|sql_calc_foun|
|mediumblob|order|relay|secondary_unl|sql_no_cache|
|mediumint|ordinality|relaylog|second_micros|sql_small_res|
|mediumtext|organization|relay_log_fil|security|sql_thread|
|member|others|relay_log_pos|select|sql_tsi_day|
|memory|out|relay_thread|sensitive|sql_tsi_hour|
|merge|outer|release|separator|sql_tsi_minut|
|message_text|outfile|reload|serial|sql_tsi_month|
|microsecond|over|remote|serializable|sql_tsi_quart|
|middleint|owner|remove|server|sql_tsi_secon|
|migrate|pack_keys|rename|session|sql_tsi_week|
|minute|page|reorganize|set|sql_tsi_year|
|minute_micros|parser|repair|share|srid|
|minute_second|partial|repeat|show|ssl|
|min_rows|partition|repeatable|shutdown|stacked|
|mod|partitioning|replace|signal|start|
|mode|partitions|replica|signed|starting|
|modifies|password|replicas|simple|starts|
|modify|password_lock|replicate_do_|skip|stats_auto_re|
|month|path|replicate_do_|slave|stats_persist|
|multilinestri|percent_rank|replicate_ign|slow|stats_sample_|
|multipoint|persist|replicate_ign|smallint|status|
|multipolygon|persist_only|replicate_rew|snapshot|stop|
|mutex|phase|replicate_wil|socket|storage|
|mysql_errno|plugin|replicate_wil|some|stored|
|name|plugins|replication|soname|straight_join|
|names|plugin_dir|require|sounds|stream|
|national|point|require_row_f|source|string|
|natural|polygon|reset|source_auto_p|subclass_orig|
|nchar|port|resignal|source_bind|subject|
|ndb|precedes|resource|source_compre|subpartition|
|ndbcluster|preceding|respect|source_connec|subpartitions|
|nested|precision|restart|source_delay|super|
|network_names|prepare|restore|source_heartb|suspend|
|swaps|timestampdiff|undo_buffer_s|utc_date|when|
|switches|tinyblob|unicode|utc_time|where|
|system|tinyint|uninstall|utc_timestamp|while|
|table|tinytext|union|validation|window|
|tables|tls|unique|value|with|
|tablespace|to|unknown|values|without|
|table_checksu|trailing|unlock|varbinary|work|
|table_name|transaction|unregister|varchar|wrapper|
|temporary|trigger|unsigned|varcharacter|write|
|temptable|triggers|until|variables|x509|
|terminated|true|update|varying|xa|
|text|truncate|upgrade|vcpu|xid|
|than|type|url|view|xml|
|then|types|usage|virtual|xor|
|thread_priori|unbounded|use|visible|year|
|ties|uncommitted|user|wait|year_month|
|time|undefined|user_resource|warnings|zerofill|
|timestamp|undo|use_frm|week|zone|
|timestampadd|undofile|using|weight_string|

## Changelog

The changelog includes a list of breaking changes made to this connector. Backwards-compatible changes are not listed.

**Proceed with caution when editing materializations created with previous versions of this connector;
editing always upgrades your materialization to the latest connector version.**

#### V1: 2023-08-21

- First version
