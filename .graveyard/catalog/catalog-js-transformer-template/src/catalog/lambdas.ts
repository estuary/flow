// This file contains placeholder examples of catalog lambdas,
// which are over-written during catalog build.

/* eslint @typescript-eslint/no-unused-vars: ["error", { "argsIgnorePattern": "^register$|^previous$" }] */
/* eslint @typescript-eslint/require-await: "off" */

import { BootstrapMap, TransformMap } from '../runtime/types';

import * as collections from './collections';
import * as registers from './registers';

export const bootstraps: BootstrapMap = {
    1: [
        async (): Promise<void> => {
            console.error('example of a bootstrap!');
        },
    ],
};

export const transforms: TransformMap = {
    1: {
        update: async (source: collections.ExampleSourceCollection): Promise<registers.ExampleRegister[]> => {
            return [{ value: source.hello.length }];
        },
        publish: async (
            source: collections.ExampleSourceCollection,
            previous: registers.ExampleRegister,
            register: registers.ExampleRegister,
        ): Promise<collections.ExampleDerivedCollection[]> => {
            return [
                {
                    world: source.hello,
                    value: register.value,
                },
            ];
        },
    },
};
