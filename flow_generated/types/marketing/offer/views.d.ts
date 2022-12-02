// Generated from collection schema examples/marketing/schema.yaml#/$defs/view.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1offer~1views.
export type Document = /* Event which captures a user's view of a marketing offer. */ {
    campaign_id: number;
    timestamp: string;
    user_id: string;
    view_id: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
