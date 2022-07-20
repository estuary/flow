// Generated from collection schema examples/source-schema/flow.yaml?ptr=/collections/examples~1source-schema~1restrictive/schema.
// Referenced from examples/source-schema/flow.yaml#/collections/examples~1source-schema~1restrictive.
export type Document = {
    a: number;
    b: boolean;
    c: number;
    id: string;
};

// Generated from derivation register schema examples/source-schema/flow.yaml?ptr=/collections/examples~1source-schema~1restrictive/derivation/register/schema.
// Referenced from examples/source-schema/flow.yaml#/collections/examples~1source-schema~1restrictive/derivation.
export type Register = unknown;

// Generated from transform fromPermissive source schema examples/source-schema/flow.yaml?ptr=/collections/examples~1source-schema~1restrictive/derivation/transform/fromPermissive/source/schema.
// Referenced from examples/source-schema/flow.yaml#/collections/examples~1source-schema~1restrictive/derivation/transform/fromPermissive.
export type FromPermissiveSource = /* Require that the documents from permissive all have these fields */ {
    a: number;
    b: boolean;
    c: number;
    id: string;
};

// Generated from derivation examples/source-schema/flow.yaml#/collections/examples~1source-schema~1restrictive/derivation.
// Required to be implemented by examples/source-schema/flow.ts.
export interface IDerivation {
    fromPermissivePublish(source: FromPermissiveSource, register: Register, previous: Register): Document[];
}
