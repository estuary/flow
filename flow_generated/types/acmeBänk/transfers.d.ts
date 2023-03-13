// Generated from collection schema examples/acmeBank.flow.yaml?ptr=/collections/acmeB%C3%A4nk~1transfers/schema.
// Referenced from examples/acmeBank.flow.yaml#/collections/acmeB%C3%A4nk~1transfers.
export type Document = {
    amount: number;
    id: number;
    recipient: string;
    sender: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
