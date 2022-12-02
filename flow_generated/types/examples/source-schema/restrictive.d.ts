// Generated from collection schema examples/source-schema/flow.yaml?ptr=/collections/examples~1source-schema~1restrictive/schema.
// Referenced from examples/source-schema/flow.yaml#/collections/examples~1source-schema~1restrictive.
export type Document = {
    a: number;
    b: boolean;
    c: number;
    id: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/source-schema/flow.yaml?ptr=/collections/examples~1source-schema~1restrictive/derivation/register/schema.
// Referenced from examples/source-schema/flow.yaml#/collections/examples~1source-schema~1restrictive/derivation.
export type Register = unknown;

// Generated from transform fromPermissive as a re-export of collection examples/source-schema/permissive.
// Referenced from examples/source-schema/flow.yaml#/collections/examples~1source-schema~1restrictive/derivation/transform/fromPermissive."
import { SourceDocument as FromPermissiveSource } from './permissive';
export { SourceDocument as FromPermissiveSource } from './permissive';

// Generated from derivation examples/source-schema/flow.yaml#/collections/examples~1source-schema~1restrictive/derivation.
// Required to be implemented by examples/source-schema/flow.ts.
export interface IDerivation {
    fromPermissivePublish(source: FromPermissiveSource, register: Register, previous: Register): OutputDocument[];
}
