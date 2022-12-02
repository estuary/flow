// Generated from collection read schema examples/source-schema/flow.yaml?ptr=/collections/examples~1source-schema~1permissive/readSchema.
// Referenced from examples/source-schema/flow.yaml#/collections/examples~1source-schema~1permissive.
export type SourceDocument = /* Require that documents all have these fields */ {
    a: number;
    b: boolean;
    c: number;
    id: string;
};

// Generated from collection write schema examples/source-schema/flow.yaml?ptr=/collections/examples~1source-schema~1permissive/writeSchema.
// Referenced from examples/source-schema/flow.yaml#/collections/examples~1source-schema~1permissive.
export type OutputDocument = /* Allows any JSON object, as long as it has a string id field */ {
    id: string;
};
