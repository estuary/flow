import { IDerivation, Document, Register, FromPermissiveSource } from 'flow/examples/source-schema/restrictive';

// Implementation for derivation examples/source-schema/flow.yaml#/collections/examples~1source-schema~1restrictive/derivation.
export class Derivation implements IDerivation {
    fromPermissivePublish(source: FromPermissiveSource, _register: Register, _previous: Register): Document[] {
        return [source];
    }
}
