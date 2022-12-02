// Generated from collection schema examples/shopping/product.schema.yaml.
// Referenced from examples/shopping/products.flow.yaml#/collections/examples~1shopping~1products.
export type Document = /* A product that is available for purchase */ {
    id: number;
    name: string;
    price: number;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
