// Generated from collection schema examples/marketing/schema.yaml#/$defs/campaign.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1campaigns.
export type Document = /* Configuration of a marketing campaign. */ {
    campaign_id: number;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
