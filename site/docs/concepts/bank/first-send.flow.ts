import { IDerivation, Document, Register, FromTransfersSource } from 'flow/acmeBank/first-send';

// Implementation for derivation first-send.flow.yaml#/collections/acmeBank~1first-send/derivation.
export class Derivation implements IDerivation {
    fromTransfersUpdate(
        _source: FromTransfersSource,
    ): Register[] {
        return [true]; // Toggle the register from `false` => `true`.
    }
    fromTransfersPublish(
        source: FromTransfersSource,
        _register: Register,
        previous: Register,
    ): Document[] {
        // If the register was previously false, than this is the first
        // transfer for this account pair.
        if (!previous) {
            return [source];
        }
        return []; // Not the first transfer.
    }
}

