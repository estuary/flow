import { IDerivation, Document, Register, FromTransfersSource } from 'flow/acmeBank/last-large-send';

// Implementation for derivation examples/bank/last-large-send.flow.yaml#/collections/acmeBank~1last-large-send/derivation.
export class Derivation implements IDerivation {
    fromTransfersPublish(source: FromTransfersSource, _register: Register, _previous: Register): Document[] {
        if (source.amount > 100) {
            return [source]; // This is a large send.
        }
        return []; // Filter this `source` document.
    }
}
