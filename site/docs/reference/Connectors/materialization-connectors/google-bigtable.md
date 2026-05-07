# Google Cloud Bigtable

This connector materializes Estuary collections into tables in a Google Cloud Bigtable instance.

## Prerequisites

To use this connector, you'll need:

* A Google Cloud project with the [Bigtable API](https://cloud.google.com/bigtable/docs/reference/admin/rest) enabled.
* A Bigtable [instance](https://cloud.google.com/bigtable/docs/instances-clusters-nodes) within that project, with **at least one table already created** (see [the note on the first table](#the-instance-must-contain-at-least-one-table) below).
* A Google Cloud [service account](https://cloud.google.com/docs/authentication/getting-started) authorized for the Bigtable instance with both of the following [roles](https://cloud.google.com/bigtable/docs/access-control#roles):
    * [`roles/bigtable.user`](https://cloud.google.com/bigtable/docs/access-control#roles) — for reading and writing rows.
    * [`roles/bigtable.admin`](https://cloud.google.com/bigtable/docs/access-control#roles) — for creating tables and column families during the connector's Apply step.

  Both roles are required: the connector both administers tables and reads/writes their data. See [Setup](#setup) for detailed steps.

### Setup

To prepare your Bigtable instance and service account, complete the following steps.

1. Create a Bigtable [instance](https://cloud.google.com/bigtable/docs/creating-instance) in the project of your choice, if one doesn't already exist. For example, using the `gcloud` CLI:

   ```bash
   gcloud bigtable instances create my-instance \
     --display-name=my-instance \
     --cluster-config=id=my-instance-c1,zone=us-east1-d,nodes=1 \
     --project=my-gcp-project
   ```

2. Create a placeholder table in the instance if it has no tables yet (see [The instance must contain at least one table](#the-instance-must-contain-at-least-one-table)):

   ```bash
   cbt -project=my-gcp-project -instance=my-instance createtable __keepalive
   ```

   `cbt` is part of the gcloud SDK and can be installed with `gcloud components install cbt`.

3. [Create a service account](https://cloud.google.com/iam/docs/service-accounts-create) for the connector to use:

   ```bash
   gcloud iam service-accounts create bigtable-materialization \
     --display-name="Bigtable materialization" \
     --project=my-gcp-project
   ```

4. Grant the service account both `roles/bigtable.user` and `roles/bigtable.admin` on the Bigtable instance:

   ```bash
   SA="<service-account-email>"

   gcloud bigtable instances add-iam-policy-binding my-instance \
     --member="serviceAccount:${SA}" \
     --role='roles/bigtable.user' \
     --project=my-gcp-project

   gcloud bigtable instances add-iam-policy-binding my-instance \
     --member="serviceAccount:${SA}" \
     --role='roles/bigtable.admin' \
     --project=my-gcp-project
   ```

   You can also grant these roles at the project level if you prefer broader scoping. IAM bindings can take several minutes to propagate.

5. Authenticate the connector with the service account using one of:

   - **Service account key**: select the new service account in the Cloud console. On the Keys tab, click **Add key** and create a new JSON key. The key is automatically downloaded. You'll paste its contents into the connector's `credentials_json` field.

   - **Google Cloud IAM (workload identity federation)**: follow the steps in the [GCP IAM guide](/guides/iam-auth/gcp/). This avoids managing a long-lived service account key.

### The instance must contain at least one table

The Bigtable client library primes its connection pool with a `PingAndWarm` request when it starts. If the target instance has no tables, the server returns `NotFound: No tables found for instance` and the client treats this as a fatal startup error — so the connector cannot Validate or Apply against a brand-new empty instance.

## Data model

Bigtable is a wide-column NoSQL store: each row has a single byte-string row key, and cell values are stored as bytes within column families. The connector maps Estuary data collections onto this model as follows:

- **Tables** correspond to bindings. Each binding writes to one Bigtable table.
- **Row keys** are derived from the source collection's primary key. Composite keys are encoded as [FoundationDB-packed tuples](https://github.com/apple/foundationdb/blob/main/design/tuple.md), which preserves lexicographic ordering of the components — so range scans by a key prefix work efficiently.
- **Column family**: the connector uses a single column family named `f` for all cells. The column family is created automatically with the table.
- **Columns**: each selected field is stored under a column qualifier matching the field name. The materialized root document is stored under the column qualifier `flow_document` (or an alternate name if a [projection](../../../concepts/collections.md#projections) is configured for the source collection's root document).

### Value encoding

Bigtable stores all cell values as raw bytes. The connector encodes field values as follows:

| Data type                       | Encoding                                                                                                                                                                                                       |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Boolean                         | A single byte: `0x00` for `false`, `0x01` for `true`.                                                                                                                                                          |
| Integer (fits in `int64`)       | 8 bytes, big-endian. This matches the format Bigtable uses for atomic increment operations.                                                                                                                    |
| Integer (wider than `int64`)    | Decimal text (for example `"99999999999999999999"`). Used when schema inference indicates the value range or string length exceeds `int64`.                                                                    |
| Number (floating point)         | 8 bytes, big-endian IEEE 754. Special values `NaN`, `Infinity`, and `-Infinity` are accepted. For values whose schema indicates a precision greater than 17 significant digits, the textual form is used instead. |
| String                          | UTF-8 bytes.                                                                                                                                                                                                   |
| Binary                          | Raw bytes (base64-decoded from the source JSON).                                                                                                                                                               |
| Array, object, or multi-type    | The original JSON encoding, stored as bytes. The root document is also stored in this form.                                                                                                                    |
| Null                            | An empty byte slice.                                                                                                                                                                                           |

A null value and a zero-length string or binary value are both stored as empty bytes and cannot be distinguished after the fact.

### Table names

Bigtable table IDs must match the pattern `[_a-zA-Z0-9][-_.a-zA-Z0-9]*` and are capped at 50 characters ([reference](https://cloud.google.com/bigtable/docs/reference/admin/rest/v2/projects.instances.tables/create)). The connector sanitizes binding table names to fit these rules: characters outside the allowed set are replaced with `_`, leading `-` and `.` characters are stripped, and the name is truncated to 50 characters if needed.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Bigtable materialization connector.

### Properties

#### Endpoint

| Property             | Title           | Description                                                                                                                                                                              | Type                          | Required/Default |
| -------------------- | --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------- | ---------------- |
| **`/project_id`**    | Project ID      | Google Cloud Project ID that owns the Bigtable instance.                                                                                                                                 | string                        | Required         |
| **`/instance_id`**   | Instance ID     | Bigtable instance ID for the materialized tables.                                                                                                                                        | string                        | Required         |
| **`/credentials`**   | Authentication  | Credentials for authentication.                                                                                                                                                          | [Credentials](#credentials)   | Required         |
| `/hardDelete`        | Hard Delete     | If enabled, items deleted in the source will also be deleted from the destination. Otherwise, `_meta/op` in the destination will signify whether rows have been deleted (soft-delete).   | boolean                       | `false`          |
| `/advanced/endpoint` | Bigtable Endpoint | The Bigtable endpoint URI to connect to. Use if you're materializing to a Bigtable-compatible API that isn't provided by Google.                                                       | string                        |                  |

#### Credentials

Credentials for authenticating with GCP. Use one of the following sets of options:

| Property                    | Title                | Description                                                            | Type   | Required/Default            |
| --------------------------- | -------------------- | ---------------------------------------------------------------------- | ------ | --------------------------- |
| **`/auth_type`**            | Auth Type            | Method to use for authentication.                                      | string | Required: `CredentialsJSON` |
| **`/credentials_json`**     | Service Account JSON | The JSON credentials of the service account to use for authorization.  | string | Required                    |

| Property                                   | Title                           | Description                                                                                                                                                          | Type   | Required/Default   |
| ------------------------------------------ | ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ------------------ |
| **`/auth_type`**                           | Auth Type                       | Method to use for authentication.                                                                                                                                    | string | Required: `GCPIAM` |
| **`/gcp_service_account_to_impersonate`**  | Service Account                 | GCP service account email to impersonate.                                                                                                                            | string | Required           |
| **`/gcp_workload_identity_pool_audience`** | Workload Identity Pool Audience | GCP Workload Identity Pool Audience in the format `https://iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test-pool/providers/test-provider`. | string | Required           |

#### Bindings

| Property      | Title      | Description                                          | Type   | Required/Default |
| ------------- | ---------- | ---------------------------------------------------- | ------ | ---------------- |
| **`/table`**  | Table Name | The name of the Bigtable table to materialize to.    | string | Required         |

### Sample

```yaml
materializations:
  ${PREFIX}/${MATERIALIZATION_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-bigtable:v1
        config:
          project_id: my-gcp-project
          instance_id: my-bigtable-instance
          credentials:
            auth_type: CredentialsJSON
            credentials_json: <secret>
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Hard delete

By default, deletions in the source surface as soft-deletes in Bigtable: the row is rewritten with the deletion document and the `_meta/op` field set to `d`, and downstream consumers can filter on that field. To instead remove the row from Bigtable when its source is deleted, set `hardDelete: true` in the endpoint configuration.
