---
description: This connector materializes Flow collections into tables in a Google Cloud SQL for PostgreSQL database.
---

# Google Cloud SQL for PostgreSQL

This connector materializes Flow collections into tables in a Google Cloud SQL for PostgreSQL database.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/materialize-postgres:dev`](https://ghcr.io/estuary/materialize-postgres:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

- A Postgres database to which to materialize, and user credentials.
  The connector will create new tables in the database per your specification. Tables created manually in advance are not supported.
- At least one Flow collection

## Setup

You must configure your database to allow connections from Estuary.
There are two ways to do this: by granting direct access to Flow's IP or by creating an SSH tunnel.

### Conenecting Directly to Google Cloud SQL

1. [Enable public IP on your database](https://cloud.google.com/sql/docs/mysql/configure-ip#add) and add `34.121.207.128, 35.226.75.135, 34.68.62.148` as authorized IP addresses.

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
To configure the connector, you must specify the database address in the format `host:port`. (You can also supply `host` only; the connector will use the port `5432` by default, which is correct in many cases.)
You can find the host and port in the following location:

- Host as Private IP Address; port is always `5432`. You may need to [configure private IP](https://cloud.google.com/sql/docs/postgres/configure-private-ip) on your database.
  :::

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Postgres materialization, which will direct one or more of your Flow collections to your desired tables, or views, in the database.

### Properties

#### Endpoint

| Property            | Title            | Description                                                                                                                                                                                                                    | Type   | Required/Default |
| ------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------ | ---------------- |
| `/database`         | Database         | Name of the logical database to materialize to.                                                                                                                                                                                | string |                  |
| **`/address`**      | Address          | Host and port of the database. If only the host is specified, port will default to `5432`.                                                                                                                                     | string | Required         |
| **`/password`**     | Password         | Password for the specified database user.                                                                                                                                                                                      | string | Required         |
| `/schema`           | Database Schema  | Database [schema](https://www.postgresql.org/docs/current/ddl-schemas.html) to use for materialized tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables | string | `"public"`       |
| **`/user`**         | User             | Database user to connect as.                                                                                                                                                                                                   | string | Required         |
| `/advanced`         | Advanced Options | Options for advanced users. You should not typically need to modify these.                                                                                                                                                     | object |                  |
| `/advanced/sslmode` | SSL Mode         | Overrides SSL connection behavior by setting the 'sslmode' parameter.                                                                                                                                                          | string |                  |

#### Bindings

| Property                       | Title                       | Description                                                                                                        | Type    | Required/Default |
| ------------------------------ | --------------------------- | ------------------------------------------------------------------------------------------------------------------ | ------- | ---------------- |
| `/additional_table_create_sql` | Additional Table Create SQL | Additional SQL statement(s) to be run in the same transaction that creates the table.                              | string  |                  |
| `/delta_updates`               | Delta Update                | Should updates to this table be done via delta updates.                                                            | boolean | `false`          |
| `/schema`                      | Alternative Schema          | Alternative schema for this table (optional). Overrides schema set in endpoint configuration.                      | string  |                  |
| **`/table`**                   | Table                       | Table name to materialize to. It will be created by the connector, unless the connector has previously created it. | string  | Required         |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-postgres:dev
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

This connector supports both standard (merge) and [delta updates](/concepts/materialization.md#delta-updates).
The default is to use standard updates.

## Reserved words

PostgreSQL has a list of reserved words that must be quoted in order to be used as an identifier.
Flow considers all the reserved words that are marked as "reserved" in any of the columns in the official [PostgreSQL documentation](https://www.postgresql.org/docs/current/sql-keywords-appendix.html).

These reserve words are listed in the table below. Flow automatically quotes fields that are in this list.

| Reserved words                  |                                  |                      |                 |                 |
| ------------------------------- | -------------------------------- | -------------------- | --------------- | --------------- |
| abs                             | current_transform_group_for_type | indicator            | order           | sqlexception    |
| absolute                        | current_user                     | initial              | out             | sqlstate        |
| acos                            | cursor                           | initially            | outer           | sqlwarning      |
| action                          | cycle                            | inner                | output          | sqrt            |
| add                             | datalink                         | inout                | over            | start           |
| all                             | date                             | input                | overlaps        | static          |
| allocate                        | day                              | insensitive          | overlay         | stddev_pop      |
| alter                           | deallocate                       | insert               | pad             | stddev_samp     |
| analyse                         | dec                              | int                  | parameter       | submultiset     |
| analyze                         | decfloat                         | integer              | partial         | subset          |
| and                             | decimal                          | intersect            | partition       | substring       |
| any                             | declare                          | intersection         | pattern         | substring_regex |
| are                             | default                          | interval             | per             | succeeds        |
| array                           | deferrable                       | into                 | percent         | sum             |
| array_agg                       | deferred                         | is                   | percentile_cont | symmetric       |
| array_max_cardinality           | define                           | isnull               | percentile_disc | system          |
| as                              | delete                           | isolation            | percent_rank    | system_time     |
| asc                             | dense_rank                       | join                 | period          | system_user     |
| asensitive                      | deref                            | json_array           | permute         | table           |
| asin                            | desc                             | json_arrayagg        | placing         | tablesample     |
| assertion                       | describe                         | json_exists          | portion         | tan             |
| asymmetric                      | descriptor                       | json_object          | position        | tanh            |
| at                              | deterministic                    | json_objectagg       | position_regex  | temporary       |
| atan                            | diagnostics                      | json_query           | power           | then            |
| atomic                          | disconnect                       | json_table           | precedes        | time            |
| authorization                   | distinct                         | json_table_primitive | precision       | timestamp       |
| avg                             | dlnewcopy                        | json_value           | prepare         | timezone_hour   |
| begin                           | dlpreviouscopy                   | key                  | preserve        | timezone_minute |
| begin_frame                     | dlurlcomplete                    | lag                  | primary         | to              |
| begin_partition                 | dlurlcompleteonly                | language             | prior           | trailing        |
| between                         | dlurlcompletewrite               | large                | privileges      | transaction     |
| bigint                          | dlurlpath                        | last                 | procedure       | translate       |
| binary                          | dlurlpathonly                    | last_value           | ptf             | translate_regex |
| bit                             | dlurlpathwrite                   | lateral              | public          | translation     |
| bit_length                      | dlurlscheme                      | lead                 | range           | treat           |
| blob                            | dlurlserver                      | leading              | rank            | trigger         |
| boolean                         | dlvalue                          | left                 | read            | trim            |
| both                            | do                               | level                | reads           | trim_array      |
| by                              | domain                           | like                 | real            | true            |
| call                            | double                           | like_regex           | recursive       | truncate        |
| called                          | drop                             | limit                | ref             | uescape         |
| cardinality                     | dynamic                          | listagg              | references      | union           |
| cascade                         | each                             | ln                   | referencing     | unique          |
| cascaded                        | element                          | local                | regr_avgx       | unknown         |
| case                            | else                             | localtime            | regr_avgy       | unmatched       |
| cast                            | empty                            | localtimestamp       | regr_count      | unnest          |
| catalog                         | end                              | log                  | regr_intercept  | update          |
| ceil                            | end-exec                         | log10                | regr_r2         | upper           |
| ceiling                         | end_frame                        | lower                | regr_slope      | usage           |
| char                            | end_partition                    | match                | regr_sxx        | user            |
| character                       | equals                           | matches              | regr_sxy        | using           |
| character_length                | escape                           | match_number         | regr_syy        | value           |
| char_length                     | every                            | match_recognize      | relative        | values          |
| check                           | except                           | max                  | release         | value_of        |
| classifier                      | exception                        | measures             | restrict        | varbinary       |
| clob                            | exec                             | member               | result          | varchar         |
| close                           | execute                          | merge                | return          | variadic        |
| coalesce                        | exists                           | method               | returning       | varying         |
| collate                         | exp                              | min                  | returns         | var_pop         |
| collation                       | external                         | minute               | revoke          | var_samp        |
| collect                         | extract                          | mod                  | right           | verbose         |
| column                          | false                            | modifies             | rollback        | versioning      |
| commit                          | fetch                            | module               | rollup          | view            |
| concurrently                    | filter                           | month                | row             | when            |
| condition                       | first                            | multiset             | rows            | whenever        |
| connect                         | first_value                      | names                | row_number      | where           |
| connection                      | float                            | national             | running         | width_bucket    |
| constraint                      | floor                            | natural              | savepoint       | window          |
| constraints                     | for                              | nchar                | schema          | with            |
| contains                        | foreign                          | nclob                | scope           | within          |
| continue                        | found                            | new                  | scroll          | without         |
| convert                         | frame_row                        | next                 | search          | work            |
| copy                            | free                             | no                   | second          | write           |
| corr                            | freeze                           | none                 | section         | xml             |
| corresponding                   | from                             | normalize            | seek            | xmlagg          |
| cos                             | full                             | not                  | select          | xmlattributes   |
| cosh                            | function                         | notnull              | sensitive       | xmlbinary       |
| count                           | fusion                           | nth_value            | session         | xmlcast         |
| covar_pop                       | get                              | ntile                | session_user    | xmlcomment      |
| covar_samp                      | global                           | null                 | set             | xmlconcat       |
| create                          | go                               | nullif               | show            | xmldocument     |
| cross                           | goto                             | numeric              | similar         | xmlelement      |
| cube                            | grant                            | occurrences_regex    | sin             | xmlexists       |
| cume_dist                       | group                            | octet_length         | sinh            | xmlforest       |
| current                         | grouping                         | of                   | size            | xmliterate      |
| current_catalog                 | groups                           | offset               | skip            | xmlnamespaces   |
| current_date                    | having                           | old                  | smallint        | xmlparse        |
| current_default_transform_group | hold                             | omit                 | some            | xmlpi           |
| current_path                    | hour                             | on                   | space           | xmlquery        |
| current_role                    | identity                         | one                  | specific        | xmlserialize    |
| current_row                     | ilike                            | only                 | specifictype    | xmltable        |
| current_schema                  | immediate                        | open                 | sql             | xmltext         |
| current_time                    | import                           | option               | sqlcode         | xmlvalidate     |
| current_timestamp               | in                               | or                   | sqlerror        | year            |

## Changelog

The changelog includes a list of breaking changes made to this connector. Backwards-compatible changes are not listed.

**Proceed with caution when editing materializations created with previous versions of this connector;
editing always upgrades your materialization to the latest connector version.**

#### V4: 2022-11-30

This version includes breaking changes to materialized table columns.
These provide more consistent column names and types, but tables created from previous versions of the connector may
not be compatible with this version.

- Capitalization is now preserved when fields in Flow are converted to Postgres column names.
  Previously, fields containing uppercase letters were converted to lowercase.

- Field names and values of types `date`, `duration`, `ipv4`, `ipv6`, `macaddr`, `macaddr8`, and `time` are now converted into
  their corresponding Postgres types.
  Previously, only `date-time` was converted, and all others were materialized as strings.
