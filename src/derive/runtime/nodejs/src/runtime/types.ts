import {Store} from './store';

/* eslint-disable @typescript-eslint/no-explicit-any */
export type Document = any;
/* eslint-enable @typescript-eslint/no-explicit-any */

// BootstrapLambda is the type shape of a catalog bootstrap lambda.
export type BootstrapLambda = (state: Store) => Promise<void>;

// TransformLambda is the type shape of a catalog transform.
export type TransformLambda = (
  doc: Document,
  store: Store
) => Promise<Document[] | void>;

// TransformMap indexes TransformLambdas on their catalog transform_id.
export interface TransformMap {
  [transform_id: number]: TransformLambda;
}

// BootstrapMap indexes BootstrapLambdas on their catalog derivation_id.
export interface BootstrapMap {
  [derivation_id: number]: BootstrapLambda[];
}
