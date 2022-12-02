// Generated from collection schema examples/hello-world/flow.yaml?ptr=/collections/examples~1greetings/schema.
// Referenced from examples/hello-world/flow.yaml#/collections/examples~1greetings.
export type Document = {
    count: number;
    message: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
