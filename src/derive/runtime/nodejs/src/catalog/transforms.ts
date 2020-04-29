// This file contains placeholder examples of catalog bootstrap lambdas,
// which are over-written during catalog build.

import './collections';
import {Store} from '../runtime/store';
import {TransformMap} from '../runtime/types';

/*eslint @typescript-eslint/no-unused-vars: ["error", { "argsIgnorePattern": "^store$" }]*/

export const transforms: TransformMap = {
  1: async (
    doc: ExampleSourceCollection,
    store: Store
  ): Promise<ExampleDerivedCollection[] | void> => {
    return [{world: doc.hello}];
  },
};
