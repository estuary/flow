// Generated from collection schema examples/acmeBank.flow.yaml?ptr=/collections/acmeB%C3%A4nk~1balances/schema.
// Referenced from examples/acmeBank.flow.yaml#/collections/acmeB%C3%A4nk~1balances.
export type Document = {
    account: string;
    amount: number;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
