// Generated from collection schema examples/acmeBank.flow.yaml?ptr=/collections/acmeBank~1transfers/schema.
// Referenced from examples/acmeBank.flow.yaml#/collections/acmeBank~1transfers.
export type Document = {
    amount: number;
    id: number;
    recipient: string;
    sender: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
