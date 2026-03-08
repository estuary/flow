---
sidebar_position: 4
---

# Capture Multiple Paths with File Source Connectors

File source connectors like Amazon S3, Google Cloud Storage, SFTP, Google Drive, Azure Blob Storage, Dropbox, and HTTP File all support capturing from multiple paths within a single capture task. However, the Estuary web app only creates a single binding during initial setup. To add additional paths, you can use flowctl to manually configure extra bindings.

This is useful when you need to:

- Capture files from multiple directories or prefixes into **separate collections** (e.g., `/invoices/` and `/receipts/` into their own collections).
- Capture files from multiple directories or prefixes into the **same collection** (e.g., `/region-us/` and `/region-eu/` merged together).

## Prerequisites

- An existing file source capture created through the Estuary web app.
- [flowctl installed and authenticated](/guides/get-started-with-flowctl).

## Option A: Multiple paths into the same collection

This is the simpler approach. You add extra bindings that capture from different paths but write to the same target collection.

### 1. Pull the capture specification

```bash
flowctl catalog pull-specs --name your-org/your-capture
```

This creates a local `flow.yaml` file and subdirectories with your capture's specification.

### 2. Prevent auto-discover from adding bindings

Open the capture YAML file. Set `addNewBindings` to `false` so auto-discover won't overwrite your manual bindings. Keep `evolveIncompatibleCollections` enabled to preserve schema inference:

```yaml
autoDiscover:
  addNewBindings: false
  evolveIncompatibleCollections: true
```

:::tip
In the web app, this corresponds to unchecking **Automatically add new collections** while keeping **Automatically keep schemas up to date** checked.
:::

### 3. Add a new binding

In the `bindings` array, add a new entry with a different `stream` value but the same `target` collection:

```yaml
bindings:
  # Existing binding
  - resource:
      stream: "invoices/2024"
    target: your-org/your-collection
  # New binding — different path, same target
  - resource:
      stream: "invoices/2025"
    target: your-org/your-collection
```

### 4. Publish

```bash
flowctl catalog publish --source flow.yaml
```

Both paths now feed into the same collection.

## Option B: Multiple paths into separate collections

Use this when you want each path captured into its own distinct collection.

### 1–2. Pull and configure auto-discover

Same as Option A above.

### 3. Add a new binding with a new target

Add a binding that points to a **new** collection name:

```yaml
bindings:
  - resource:
      stream: "invoices/"
    target: your-org/invoices
  - resource:
      stream: "receipts/"
    target: your-org/receipts
```

### 4. Define the new collection

The new target collection must exist before you publish. Add its definition to your `flow.yaml` (or a file imported by it). The easiest approach is to copy the schema and key from your existing collection:

Copy the schema and key from your existing collection (found in the YAML files that `pull-specs` created):

```yaml
collections:
  your-org/receipts:
    schema:
      # Copy from your existing collection's schema
      type: object
      properties:
        _meta:
          type: object
          properties:
            file:
              type: string
            offset:
              type: integer
          required: [file, offset]
      required: [_meta]
    key: [/_meta/file, /_meta/offset]
```

:::tip
If you're unsure what schema to use, pull the existing collection's spec and copy it:
```bash
flowctl catalog pull-specs --name your-org/your-existing-collection
```
:::

### 5. Publish

```bash
flowctl catalog publish --source flow.yaml
```

## Worked example: SFTP with two directories

This example captures CSV files from two directories on an SFTP server into separate collections.

```yaml
captures:
  acmeCo/sftp-capture:
    autoDiscover:
      addNewBindings: false
      evolveIncompatibleCollections: true
    endpoint:
      connector:
        image: "ghcr.io/estuary/source-sftp:dev"
        config:
          address: sftp.example.com:22
          username: estuary
          password: <SECRET>
          directory: /data
          parser:
            format:
              type: csv
              config:
                delimiter: ","
                encoding: UTF-8
    bindings:
      - resource:
          stream: /data/invoices
        target: acmeCo/invoices
      - resource:
          stream: /data/receipts
        target: acmeCo/receipts

collections:
  acmeCo/invoices:
    schema:
      type: object
      properties:
        _meta:
          type: object
          properties:
            file:
              type: string
            offset:
              type: integer
          required: [file, offset]
      required: [_meta]
    key: [/_meta/file, /_meta/offset]

  acmeCo/receipts:
    schema:
      type: object
      properties:
        _meta:
          type: object
          properties:
            file:
              type: string
            offset:
              type: integer
          required: [file, offset]
      required: [_meta]
    key: [/_meta/file, /_meta/offset]
```

## Worked example: S3 with two prefixes

This example captures JSON files from two S3 prefixes into the same collection.

```yaml
captures:
  acmeCo/s3-capture:
    autoDiscover:
      addNewBindings: false
      evolveIncompatibleCollections: true
    endpoint:
      connector:
        image: "ghcr.io/estuary/source-s3:dev"
        config:
          bucket: acme-data-lake
          region: us-east-1
          credentials:
            auth_type: AWSAccessKey
            aws_access_key_id: <SECRET>
            aws_secret_access_key: <SECRET>
          parser:
            format:
              type: json
    bindings:
      - resource:
          stream: acme-data-lake/events/region-us
        target: acmeCo/all-events
      - resource:
          stream: acme-data-lake/events/region-eu
        target: acmeCo/all-events
```

## Connector-specific notes

### Google Drive

For Google Drive, the `stream` value in each binding is the **Google Drive folder ID** — the long string at the end of the folder's URL (e.g., `1aBcDeFgHiJkLmNoPqRsTuV` from `https://drive.google.com/drive/folders/1aBcDeFgHiJkLmNoPqRsTuV`).

Each binding can point to a different folder. The `folderUrl` in the endpoint config is the folder used during initial discovery, but bindings are not limited to that folder.

:::caution
The `folderUrl` must use the format `https://drive.google.com/drive/folders/FOLDER_ID`. URLs with `/u/0/` or `/u/1/` (from Google's multi-account switcher) will be rejected. Remove the `/u/N` segment if present.
:::

### SFTP

The `stream` value is the full path to the directory on the SFTP server (relative to the SFTP chroot). For example, if your SFTP `directory` config is `/data`, a binding might use `stream: /data/invoices`.

### Amazon S3 and Google Cloud Storage

The `stream` value is formatted as `bucket-name/prefix`. For example: `my-bucket/events/region-us`.

## Important caveats

- **Parser config is shared.** All bindings in a capture share the same endpoint-level parser configuration (compression, format, CSV options, etc.). You cannot mix file formats within a single capture — for example, capturing CSV from one path and JSON from another requires two separate captures.

- **Auto-discover must not add bindings.** Set `addNewBindings: false` in your `autoDiscover` config. If left as `true`, auto-discover may overwrite your manually-added bindings on the next discovery cycle. You can keep `evolveIncompatibleCollections: true` to preserve schema inference.

- **Target collections must exist.** When using Option B (separate collections), the target collection must be defined and published. If you reference a collection that doesn't exist, the publish will fail.
