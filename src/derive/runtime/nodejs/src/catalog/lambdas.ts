// This file contains placeholder examples of catalog lambdas,
// which are over-written during catalog build.

/*eslint @typescript-eslint/no-unused-vars: ["error", { "argsIgnorePattern": "^store$" }]*/
/*eslint @typescript-eslint/require-await: "off"*/

import './collections';
import { Store } from '../runtime/store';
import { BootstrapMap, TransformMap } from '../runtime/types';

export const bootstraps: BootstrapMap = {
    1: [
        async (store: Store): Promise<void> => {
            console.log('example of a bootstrap!');
        },
    ],
};

export const transforms: TransformMap = {
    1: async (
        doc: ExampleSourceCollection,
        store: Store
    ): Promise<ExampleDerivedCollection[] | void> => {
        return [{world: doc.hello}];
    },
};
