// Generated from collection schema examples/marketing/schema.yaml#/$defs/purchase.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1purchases.
export type Document = /* Event which captures a user's purchase of a product. */ {
    purchase_id: number;
    user_id: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
