// Generated from collection schema examples/re-key/schema.yaml#/$defs/id_mapping.
// Referenced from examples/re-key/flow.yaml#/collections/examples~1re-key~1mappings.
export type Document = /* A learned association of an anonymous ID <=> stable ID */ {
    anonymous_id: string;
    stable_id: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
