
// Generated from collection schema aliceCo/derive/withHeaderCount.flow.yaml?ptr=/collections/aliceCo~1derive~1with-header-count/schema.
// Referenced from aliceCo/derive/withHeaderCount.flow.yaml#/collections/aliceCo~1derive~1with-header-count.
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

// Generated from derivation register schema aliceCo/derive/withHeaderCount.flow.yaml?ptr=/collections/aliceCo~1derive~1with-header-count/derivation/register/schema.
// Referenced from aliceCo/derive/withHeaderCount.flow.yaml#/collections/aliceCo~1derive~1with-header-count/derivation.
export type Register = unknown;


// Generated from transform fromRides as a re-export of collection aliceCo/ingest/webhook-data.
// Referenced from aliceCo/derive/withHeaderCount.flow.yaml#/collections/aliceCo~1derive~1with-header-count/derivation/transform/fromRides."
import { SourceDocument as FromRidesSource } from "./../ingest/webhook-data";
export { SourceDocument as FromRidesSource } from "./../ingest/webhook-data";


// Generated from derivation aliceCo/derive/withHeaderCount.flow.yaml#/collections/aliceCo~1derive~1with-header-count/derivation.
// Required to be implemented by aliceCo/derive/withHeaderCount.flow.ts.
export interface IDerivation {
    fromRidesPublish(
        source: FromRidesSource,
        register: Register,
        previous: Register,
    ): OutputDocument[];
}
