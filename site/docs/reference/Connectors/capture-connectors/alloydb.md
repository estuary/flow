
# AlloyDB

This connector uses change data capture (CDC) to continuously capture table updates in an AlloyDB database into one or more Flow collections.

AlloyDB is a fully managed, PostgreSQL-compatible database available in the Google Cloud platform.
This connector is derived from the [PostgreSQL capture connector](/reference/Connectors/capture-connectors/PostgreSQL/),
so the same configuration applies, but the setup steps look somewhat different.

It's available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-alloydb:dev`](https://github.com/estuary/connectors/pkgs/container/source-alloydb) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

You'll need a AlloyDB database setup with the following:

* Logical decoding enabled
* User role with `REPLICATION` attribute
* A replication slot. This represents a “cursor” into the PostgreSQL write-ahead log from which change events can be read.
    * Optional; if none exist, one will be created by the connector.
    * If you wish to run multiple captures from the same database, each must have its own slot.
    You can create these slots yourself, or by specifying a name other than the default in the advanced [configuration](#configuration).
* A publication. This represents the set of tables for which change events will be reported.
    * In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.
* A watermarks table. The watermarks table is a small “scratch space” to which the connector occasionally writes a small amount of data to ensure accuracy when backfilling preexisting table contents.
    * In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.

You'll also need a virtual machine to connect securely to the instance via SSH tunnelling (AlloyDB doesn't support IP allowlisting).

### Setup

To meet the prerequisites, complete these steps.

1. Set [the `alloydb.logical_decoding` flag to `on`](https://cloud.google.com/alloydb/docs/reference/alloydb-flags) to enable logical replication on your AlloyDB instance.

2. In your [psql client](https://cloud.google.com/alloydb/docs/connect-psql), connect to your instance and issue the following commands to create a new user for the capture with appropriate permissions,
and set up the watermarks table and publication.

  ```sql
  CREATE USER flow_capture WITH REPLICATION
  IN ROLE alloydbsuperuser LOGIN PASSWORD 'secret';
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO flow_capture;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO flow_capture;
  CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
  GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
  CREATE PUBLICATION flow_publication FOR ALL TABLES;
  ```

3. Follow the instructions to create a [virtual machine for SSH tunneling](../../../guides/connect-network.md#setup-for-google-cloud)
in the same Google Cloud project as your instance.

## Backfills and performance considerations

When the a AlloyDB capture is initiated, by default, the connector first *backfills*, or captures the targeted tables in their current state. It then transitions to capturing change events on an ongoing basis.

This is desirable in most cases, as in ensures that a complete view of your tables is captured into Flow.
However, you may find it appropriate to skip the backfill, especially for extremely large tables.

In this case, you may turn of backfilling on a per-table basis. See [properties](#properties) for details.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the PostgreSQL source connector.

### Properties

#### Endpoint

The SSH config section is required for this connector.
You'll fill in the database address with a localhost IP address,
and specify your VM's IP address as the SSH address.
See the table below and the [sample config](#sample).

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/address`** | Address | The host or host:port at which the database can be reached. | string | Required |
| **`/database`** | Database | Logical database name to capture from. | string | Required, `"postgres"` |
| **`/user`** | User | The database user to authenticate as. | string | Required, `"flow_capture"` |
| **`/password`** | Password | Password for the specified database user. | string | Required |
| `/advanced` | Advanced Options | Options for advanced users. You should not typically need to modify these. | object |  |
| `/advanced/backfill_chunk_size` | Backfill Chunk Size | The number of rows which should be fetched from the database in a single backfill query. | integer | `4096` |
| `/advanced/publicationName` | Publication Name | The name of the PostgreSQL publication to replicate from. | string | `"flow_publication"` |
| `/advanced/skip_backfills` | Skip Backfills | A comma-separated list of fully-qualified table names which should not be backfilled. | string |  |
| `/advanced/slotName` | Slot Name | The name of the PostgreSQL replication slot to replicate from. | string | `"flow_slot"` |
| `/advanced/watermarksTable` | Watermarks Table | The name of the table used for watermark writes during backfills. Must be fully-qualified in &#x27;&lt;schema&gt;.&lt;table&gt;&#x27; form. | string | `"public.flow_watermarks"` |
| `networkTunnel` | Network Tunnel | Connect to your system through an SSH server that acts as a bastion host for your network. | Object | |
| `networkTunnel/sshForwarding` | SSH Forwarding | | Object | |
| `networkTunnel/sshForwarding/sshEndpoint` | SSH Endpoint | Endpoint of the remote SSH server (in this case, your Google Cloud VM) that supports tunneling (in the form of ssh://user@address). | String | |
| `networkTunnel/sshForwarding/privateKey` | SSH Private Key | Private key to connect to the remote SSH server. | String | |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|-------|------|------|---------| --------|
| **`/namespace`** | Namespace | The [namespace/instance](https://cloud.google.com/alloydb/docs/overview#hierarchical_resource_structure) of the table. | string | Required |
| **`/stream`** | Stream | Table name. | string | Required |
| **`/syncMode`** | Sync mode | Connection method. Always set to `incremental`. | string | Required |

### Sample

A minimal capture definition will look like the following:

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/source-alloydb:dev"
        config:
          address: "127.0.0.1:5432"
          database: "postgres"
          user: "flow_capture"
          password: "secret"
          networkTunnel:
            sshForwarding:
              sshEndpoint: ssh://sshUser@vm-ip-address
              privateKey: |2
              -----BEGIN RSA PRIVATE KEY-----
              MIICXAIBAAKBgQCJO7G6R+kv2MMS8Suw21sk2twHg8Vog0fjimEWJEwyAfFM/Toi
              EJ6r5RTaSvN++/+MPWUll7sUdOOBZr6ErLKLHEt7uXxusAzOjMxFKZpEARMcjwHY
              v/tN1A2OYU0qay1DOwknEE0i+/Bvf8lMS7VDjHmwRaBtRed/+iAQHf128QIDAQAB
              AoGAGoOUBP+byAjDN8esv1DCPU6jsDf/Tf//RbEYrOR6bDb/3fYW4zn+zgtGih5t
              CR268+dwwWCdXohu5DNrn8qV/Awk7hWp18mlcNyO0skT84zvippe+juQMK4hDQNi
              ywp8mDvKQwpOuzw6wNEitcGDuACx5U/1JEGGmuIRGx2ST5kCQQDsstfWDcYqbdhr
              5KemOPpu80OtBYzlgpN0iVP/6XW1e5FCRp2ofQKZYXVwu5txKIakjYRruUiiZTza
              QeXRPbp3AkEAlGx6wMe1l9UtAAlkgCFYbuxM+eRD4Gg5qLYFpKNsoINXTnlfDry5
              +1NkuyiQDjzOSPiLZ4Abpf+a+myjOuNL1wJBAOwkdM6aCVT1J9BkW5mrCLY+PgtV
              GT80KTY/d6091fBMKhxL5SheJ4SsRYVFtguL2eA7S5xJSpyxkadRzR0Wj3sCQAvA
              bxO2fE1SRqbbF4cBnOPjd9DNXwZ0miQejWHUwrQO0inXeExNaxhYKQCcnJNUAy1J
              6JfAT/AbxeSQF3iBKK8CQAt5r/LLEM1/8ekGOvBh8MAQpWBW771QzHUN84SiUd/q
              xR9mfItngPwYJ9d/pTO7u9ZUPHEoat8Ave4waB08DsI=
              -----END RSA PRIVATE KEY-----
    bindings:
      - resource:
          stream: ${TABLE_NAME}
          namespace: ${TABLE_NAMESPACE}
          syncMode: incremental
        target: ${PREFIX}/${COLLECTION_NAME}
```
Your capture definition will likely be more complex, with additional bindings for each table in the source database.

[Learn more about capture definitions.](../../../concepts/captures.md)
