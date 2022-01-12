import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/bank/last-large-send.flow.yaml#/collections/acmeBank~1last-large-send/derivation.
export class AcmeBankLastLargeSend implements interfaces.AcmeBankLastLargeSend {
    fromTransfersPublish(
        source: collections.AcmeBankTransfers,
        _register: registers.AcmeBankLastLargeSend,
        _previous: registers.AcmeBankLastLargeSend,
    ): collections.AcmeBankLastLargeSend[] {
        if (source.amount > 100) {
            return [source]; // This is a large send.
        }
        return []; // Filter this `source` document.
    }
}
