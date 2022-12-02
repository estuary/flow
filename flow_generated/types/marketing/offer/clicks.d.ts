// Generated from collection schema examples/marketing/schema.yaml#/$defs/click.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1offer~1clicks.
export type Document = /* Event which captures a user's click of a marketing offer. */ {
    click_id: string;
    timestamp: string;
    user_id: string;
    view_id: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
