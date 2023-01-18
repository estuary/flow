// Generated from collection schema examples/wiki/pages.flow.yaml?ptr=/collections/examples~1wiki~1pages/schema.
// Referenced from examples/wiki/pages.flow.yaml#/collections/examples~1wiki~1pages.
export type Document = {
    add?: number;
    byCountry?: {
        [k: string]: {
            add?: number;
            cnt?: number;
            del?: number;
        };
    };
    cnt?: number;
    del?: number;
    page: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/wiki/pages.flow.yaml?ptr=/collections/examples~1wiki~1pages/derivation/register/schema.
// Referenced from examples/wiki/pages.flow.yaml#/collections/examples~1wiki~1pages/derivation.
export type Register = unknown;

// Generated from transform rollUpEdits as a re-export of collection examples/wiki/edits.
// Referenced from examples/wiki/pages.flow.yaml#/collections/examples~1wiki~1pages/derivation/transform/rollUpEdits."
import { SourceDocument as RollUpEditsSource } from './edits';
export { SourceDocument as RollUpEditsSource } from './edits';

// Generated from derivation examples/wiki/pages.flow.yaml#/collections/examples~1wiki~1pages/derivation.
// Required to be implemented by examples/wiki/pages.flow.ts.
export interface IDerivation {
    rollUpEditsPublish(source: RollUpEditsSource, register: Register, previous: Register): OutputDocument[];
}
