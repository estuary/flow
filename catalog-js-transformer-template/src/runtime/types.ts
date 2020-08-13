/* eslint-disable @typescript-eslint/no-explicit-any */
export type Document = any;
/* eslint-enable @typescript-eslint/no-explicit-any */

// BootstrapLambda is the type shape of a catalog bootstrap lambda.
export type BootstrapLambda = () => Promise<void>;

// TransformLambda is the generic shape of a lambda function, taking
// up to three Documents as input.
export type TransformLambda = (source: Document, ...rest: Document[]) =>
    Promise<Document[]>;

// TransformMap indexes "update" and "publish" lambdas on their catalog
// transform_id.
export interface TransformMap {
  [transformId: number]: {
    update?: TransformLambda,
    publish?: TransformLambda,
  };
}

// BootstrapMap indexes BootstrapLambdas on their catalog derivation_id.
export interface BootstrapMap {
  [derivationId: number]: BootstrapLambda[];
}
