
# Google Firestore

This connector captures data from your Google Firestore collections into Flow collections.

[`ghcr.io/estuary/source-firestore:dev`](https://ghcr.io/estuary/source-firestore:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Data model

Firestore is a NoSQL database. Its [data model](https://firebase.google.com/docs/firestore/data-model) consists of **documents** (lightweight records that contain mappings of fields and values) organized in **collections**.

Collections are organized hierarchically. A given document in a collection can, in turn, be associated with a **[subcollection](https://firebase.google.com/docs/firestore/data-model#subcollections)**.

For example, you might have a collection called `users`, which contains two documents, `alice` and `bob`.
Each document has a subcollection called `messages` (for example, `users/alice/messages`), which contain more documents (for example, `users/alice/messages/1`).

```console
users
├── alice
│   └── messages
│       ├── 1
│       └── 2
└── bob
    └── messages
        └── 1
```

The connector works by identifying documents associated with a particular sequence of Firestore collection names,
regardless of documents that split the hierarchy.
These document groupings are mapped to Flow collections using a [path](#bindings) in the pattern `collection/*/subcollection`.

In this example, we'd end up with `users` and `users/*/messages` Flow collections, with the latter contain messages from both users.
The `/_meta/path` property for each document contains its full, original path, so we'd still know which messages were Alice's and which were Bob's.

## Prerequisites

You'll need:

* A Google service account with:

    * Read access to your Firestore database, via [roles/datastore.viewer](https://cloud.google.com/datastore/docs/access/iam).
    You can assign this role when you [create the service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts#creating), or [add it to an existing service account](https://cloud.google.com/iam/docs/granting-changing-revoking-access#single-role).

    * A generated [JSON service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating) for the account.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the Flow specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Firestore source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/googleCredentials`** | Credentials | Google Cloud Service Account JSON credentials. | string | Required |
| `/database` | Database | Optional name of the database to capture from. Leave blank to autodetect. Typically &quot;projects&#x2F;&#x24;PROJECTID&#x2F;databases&#x2F;(default)&quot;. | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/backfillMode`** | Backfill Mode | Configures the handling of data already in the collection. See [below](#backfill-mode) for details or just stick with &#x27;async&#x27; | string | Required |
| **`/path`** | Path to Collection | Supports parent&#x2F;&#x2A;&#x2F;nested to capture all nested collections of parent&#x27;s children | string | Required |
| **`/restartCursorPath`** | Restart Cursor Path | a specified cursor (ideally timestamp) that we will use (+5 minutes overlap) to start our backfills from, rather than the whole collection | string | Optional |
| **`/minBackfillInterval`** | Minimum Backfill Interval | A minimum amount of time between backfills if consistency is lost. Defaults to 24 hours if no restart cursor is set, 5 minutes if there is | string | Optional |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-firestore:dev
        config:
          googleCredentials:
            "type": "service_account",
            "project_id": "project-id",
            "private_key_id": "key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n",
            "client_email": "service-account-email",
            "client_id": "client-id",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://accounts.google.com/o/oauth2/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/service-account-email"
    bindings:
      - resource:
          #The below `path` will capture all Firestore documents that match the pattern
          #`orgs/<orgID>/runs/<runID>/runResults/<runResultID>/queryResults`.
          #See the Data Model section above for details.
          path: orgs/*/runs/*/runResults/*/queryResults
          backfillMode: async
        target: ${PREFIX}/orgs_runs_runResults_queryResults
      - resource:
          path: orgs/*/runs/*/runResults
          backfillMode: async
        target: ${PREFIX}/orgs_runs_runResults
      - resource:
          path: orgs/*/runs
          backfillMode: async
        target: ${PREFIX}/orgs_runs
      - resource:
          path: orgs
          backfillMode: async
        target: ${PREFIX}/orgs
```

## Backfill mode

In each captured collection's [binding configuration](#bindings), you can choose whether and how to backfill historical data.
There are three options:

* `none`: Skip preexisting data in the Firestore collection. Capture only new documents and changes to existing documents that occur after the capture is published.

* `async`: Use two threads to capture data. The first captures new documents, as with `none`.
The second progressively ingests historical data in chunks. This mode is most reliable for Firestore collections of all sizes but provides slightly weaker guarantees against data duplication.

   The connector uses a [reduction](../../../concepts/schemas.md#reductions) to reconcile changes to the same document found on the parallel threads.
   The version with the most recent timestamp the document metadata will be preserved (`{"strategy": "maximize", "key": "/_meta/mtime"}`). For most collections, this produces an accurate copy of your Firestore collections in Flow.

* `sync`: Request that Firestore stream all changes to the collection since its creation, in order.

   This mode provides the strongest guarantee against duplicated data, but can cause errors for large datasets.
   Firestore may terminate the process if the backfill of historical data has not completed within about ten minutes, forcing the capture to restart from the beginning.
   If this happens once it is likely to recur continuously. If left unattended for an extended time this can result in a massive number of read operations and a correspondingly large bill from Firestore.

   This mode should only be used when somebody can keep an eye on the backfill and shut it down if it has not completed within half an hour at most, and on relatively small collections.
   100,000 documents or fewer should generally be safe, although this can vary depending on the average document size in the collection.

If you're unsure which backfill mode to use, choose `async`.
