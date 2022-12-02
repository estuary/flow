// Generated from collection schema examples/shopping/user.schema.yaml.
// Referenced from examples/shopping/users.flow.yaml#/collections/examples~1shopping~1users.
export type Document = /* A user who may buy things from our site */ {
    email: string;
    id: number;
    name: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
