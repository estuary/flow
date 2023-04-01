
// Generated from collection schema aliceCo/ingest/webhook-data.schema.yaml.
// Referenced from aliceCo/ingest/flow.yaml#/collections/aliceCo~1ingest~1webhook-data.
export type Document = {
    "_meta": /* These fields are automatically added by the connector, and do not need to be specified in the request body */ {
        headers?: /* HTTP headers that were sent with the request will get added here. Headers that are known to be sensitive or not useful will not be included */ {
            [k: string]: string;
        };
        receivedAt: /* Timestamp of when the request was received by the connector */ string;
        webhookId: /* The id of the webhook request, which is automatically added by the connector */ string;
    };
};


// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
