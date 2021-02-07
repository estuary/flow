/* eslint-disable @typescript-eslint/no-explicit-any */
export type Document = any;
/* eslint-enable @typescript-eslint/no-explicit-any */

// BootstrapLambda is the type shape of a catalog bootstrap lambda.
export type BootstrapLambda = () => Promise<void>;

// UpdateLambda takes a document and returns an array of register update
// Documents.
export type UpdateLambda = (source: Document) => Promise<Document[]>;

// PublishLambda takes a source document, a previous register, and a
// next register, and returns an array of derived documents to publish.
export type PublishLambda = (source: Document, previous: Document, register: Document) => Promise<Document[]>;

// TransformMap indexes "update" and "publish" lambdas on their catalog
// transform_id.
export interface TransformMap {
    [transformId: number]: {
        update?: UpdateLambda;
        publish?: PublishLambda;
    };
}

// BootstrapMap indexes BootstrapLambdas on their catalog derivation_id.
export interface BootstrapMap {
    [derivationId: number]: BootstrapLambda[];
}
