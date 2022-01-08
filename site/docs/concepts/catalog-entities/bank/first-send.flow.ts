import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation site/docs/concepts/catalog-entities/derivations/bank/first-send.flow.yaml#/collections/acmeBank~1first-send/derivation.
export class AcmeBankFirstSend implements interfaces.AcmeBankFirstSend {
    fromTransfersUpdate(_source: collections.AcmeBankTransfers): registers.AcmeBankFirstSend[] {
        return [true]; // Toggle the register from `false` => `true`.
    }
    fromTransfersPublish(
        source: collections.AcmeBankTransfers,
        _register: registers.AcmeBankFirstSend,
        previous: registers.AcmeBankFirstSend,
    ): collections.AcmeBankFirstSend[] {
        // If the register was previously false, than this is the first
        // transfer for this account pair.
        if (!previous) {
            return [source];
        }
        return []; // Not the first transfer.
    }
}
