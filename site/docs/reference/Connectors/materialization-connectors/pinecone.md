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

The materialization creates vector embeddings based on a text input from your collection. A field
with type `string` must exist in your collection and it must be `required` in the collection schema.

By default the materialization connector will look for a field named `"input"` in your collection
and use that value to create the embeddings. If you are using a
[derivation](../../../concepts/derivations.md) to transform your source data before materializing it
into Pinecone it may be convenient to create the derived collection with this field. Alternatively a
[projection](../../../concepts/advanced/projections.md) can be configured for the source collection
with the name of `"input"`. There is also an optional configuration for each binding to set an
alternate name of the collection projection to use as the embedding input (see Bindings below).

## Pinecone Record Metadata

Pinecone supports metadata fields associated with stored vectors that can be used when performing
[vector queries](https://www.pinecone.io/learn/vector-search-filtering/).

This materialization will automatically include all compatible fields in the source collection as
metadata, including the field used as `"input"` for creating the embedding.

Compatible fields have the following types, and do not have to be `required`:
- `integer`
- `number`
- `string`
- `boolean`

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
| `/advaned/openAiOrg`  | OpenAI Organization  | Optional organization name for OpenAI requests. Use this if you belong to multiple organizations to specify which organization is used for API requests. | string |                            |

#### Bindings

| Property           | Title                 | Description                                                                                                         | Type   | Required/Default |
| ------------------ | --------------------- | ------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/namespace`**   | Pinecone Namespace    | Name of the Pinecone namespace that this collection will materialize vectors into.                                  | string | Required         |
| `/inputProjection` | Input Projection Name | Alternate name of the collection projection to use as input for creating the vector embedding. Defaults to 'input'. | string | `"input"`        |

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

This connector operates only in [delta updates](../../../concepts/materialization.md#delta-updates) mode.

Pinecone [upserts](https://docs.pinecone.io/reference/upsert) vectors based on their `id`. The `id`
for materialized vectors is based on the Flow Collection key.

For collections with a a top-level reduction strategy of
[merge](../../reduction-strategies/merge.md) and a strategy of
[lastWriteWins](../../reduction-strategies/firstwritewins-and-lastwritewins.md) for all nested
values (this is also the default), collections will be materialized "effectively once", with any
updated Flow documents replacing vectors in the Pinecone index if they have the same key.