# Pinecone

This connector materializes Flow collections into namespaces in a Pinecone index.

The connector uses the [OpenAI Embedding API](https://platform.openai.com/docs/guides/embeddings) to
create vector embeddings based on the documents in your collections and inserts these vector
embeddings and associated metadata into Pinecone for storage and retrieval.

[`ghcr.io/estuary/materialize-pinecone:dev`](https://ghcr.io/estuary/materialize-pinecone:dev)
provides the latest connector image. You can also follow the link in your browser to see past image
versions.

## Prerequisites

To use this connector, you'll need:

* A [Pinecone](https://www.pinecone.io/) account with an [API
  Key](https://docs.pinecone.io/docs/quickstart#2-get-and-verify-your-pinecone-api-key) for
  authentication.
* An [OpenAI](https://openai.com/) account with an [API
  Key](https://platform.openai.com/docs/api-reference/authentication) for authentication.
* A [Pinecone Index](https://docs.pinecone.io/docs/indexes) created to store materialized vector
  embeddings. When using the embedding model `text-embedding-ada-002` (recommended), the index must
  have `Dimensions` set to 1536.

## Embedding Input

The materialization creates a vector embedding for each collection document. Its structure is based
on the collection fields.

By default, fields of a single scalar type are including in the embedding: strings, integers,
numbers, and booleans. You can include additional array or object type fields using [projected
fields](/concepts/materialization/#projected-fields).

The text generated for the embedding has this structure, with field names and their values separated
by newlines:
```
stringField: stringValue
intField: 3
numberField: 1.2
boolField: false
```

## Pinecone Record Metadata

Pinecone supports metadata fields associated with stored vectors that can be used when performing
[vector queries](https://www.pinecone.io/learn/vector-search-filtering/). This materialization will
include the materialized document as a JSON string in the metadata field `flow_document` to enable
retrieval of the document from vectors returned by Pinecone queries.

Pinecone indexes all metadata fields by default. To manage memory usage of the index, use [selective
metadata indexing](https://docs.pinecone.io/docs/manage-indexes#selective-metadata-indexing) to
exclude the `flow_document` metadata field.

### Properties

#### Endpoint

| Property              | Title                | Description                                                                                                                                              | Type   | Required/Default           |
| --------------------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | -------------------------- |
| **`/index`**          | Pinecone Index       | Pinecone index for this materialization. Must already exist and have appropriate dimensions for the embedding model used.                                | string | Required                   |
| **`/environment`**    | Pinecone Environment | Cloud region for your Pinecone project. Example: us-central1-gcp                                                                                         | string | Required                   |
| **`/pineconeApiKey`** | Pinecone API Key     | Pinecone API key used for authentication.                                                                                                                | string | Required                   |
| **`/openAiApiKey`**   | OpenAI API Key       | OpenAI API key used for authentication.                                                                                                                  | string | Required                   |
| `/embeddingModel`     | Embedding Model ID   | Embedding model ID for generating OpenAI bindings. The default text-embedding-ada-002 is recommended.                                                    | string | `"text-embedding-ada-002"` |
| `/advanced`           |                      | Options for advanced users. You should not typically need to modify these.                                                                               | object |                            |
| `/advanced/openAiOrg`  | OpenAI Organization  | Optional organization name for OpenAI requests. Use this if you belong to multiple organizations to specify which organization is used for API requests. | string |                            |

#### Bindings

| Property           | Title                 | Description                                                                                                         | Type   | Required/Default |
| ------------------ | --------------------- | ------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/namespace`**   | Pinecone Namespace    | Name of the Pinecone namespace that this collection will materialize vectors into.                                  | string | Required         |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/materialize-pinecone:dev"
        config:
          index: your-index
          environment: us-central1-gcp
          pineconeApiKey: <YOUR_PINECONE_API_KEY>
          openAiApiKey: <YOUR_OPENAI_API_KEY>
    bindings:
      - resource:
          namespace: your-namespace
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Delta Updates

This connector operates only in [delta updates](/concepts/materialization/#delta-updates) mode.

Pinecone [upserts](https://docs.pinecone.io/reference/upsert) vectors based on their `id`. The `id`
for materialized vectors is based on the Flow Collection key.

For collections with a a top-level reduction strategy of
[merge](/reference/reduction-strategies/merge) and a strategy of
[lastWriteWins](/reference/reduction-strategies/firstwritewins-and-lastwritewins) for all nested
values (this is also the default), collections will be materialized "effectively once", with any
updated Flow documents replacing vectors in the Pinecone index if they have the same key.