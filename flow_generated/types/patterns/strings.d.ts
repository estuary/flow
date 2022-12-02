// Generated from $anchor schema examples/derive-patterns/schema.yaml#Int."
export type Int = /* A document that holds an integer */ {
    Int: number;
    Key: string;
};

// Generated from $anchor schema examples/derive-patterns/schema.yaml#Join."
export type Join = /* Document for join examples */ {
    Key?: string;
    LHS?: number;
    RHS?: string[];
};

// Generated from $anchor schema examples/derive-patterns/schema.yaml#String."
export type String = /* A document that holds a string */ {
    Key: string;
    String: string;
};

// Generated from collection schema examples/derive-patterns/schema.yaml#String.
// Referenced from examples/derive-patterns/inputs.flow.yaml#/collections/patterns~1strings.
export type Document = /* A document that holds a string */ {
    Key: string;
    String: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
