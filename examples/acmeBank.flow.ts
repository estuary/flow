import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation acmeBank.flow.yaml#/collections/acmeBank~1balances/derivation.
export class AcmeBankBalances implements interfaces.AcmeBankBalances {
    fromTransfersPublish(
        source: collections.AcmeBankTransfers,
        _register: registers.AcmeBankBalances, // Registers enable stateful derivations,
        _previous: registers.AcmeBankBalances, // but aren't needed here.
    ): collections.AcmeBankBalances[] {
        return [
            // A transfer removes from the sender and adds to the receiver.
            { account: source.from, amount: -source.amount },
            { account: source.to, amount: source.amount },
        ];
    }
}
