// This file contains placeholder examples of catalog bootstrap lambdas,
// which are over-written during catalog build.

import {Store} from '../runtime/store';
import {BootstrapMap} from '../runtime/types';

/*eslint @typescript-eslint/no-unused-vars: ["error", { "argsIgnorePattern": "^store$" }]*/

export const bootstraps: BootstrapMap = {
  9: [
    async (store: Store): Promise<void> => {
      console.log('example of a bootstrap!');
    },
  ],
};
