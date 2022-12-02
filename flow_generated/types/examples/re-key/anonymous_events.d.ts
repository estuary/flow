// Generated from collection schema examples/re-key/schema.yaml#/$defs/anonymous_event.
// Referenced from examples/re-key/flow.yaml#/collections/examples~1re-key~1anonymous_events.
export type Document = /* An interesting event, keyed on an anonymous ID */ {
    anonymous_id: string;
    event_id: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
