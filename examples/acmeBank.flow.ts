import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation acmeBank.flow.yaml#/collections/acmeBank~1balances/derivation.
export class AcmeBankBalances implements interfaces.AcmeBankBalances {
    fromTransfersPublish(
        source: collections.AcmeBankTransfers,
        // Registers enable stateful workflows, and are part of
        // the interface Flow expects, but aren't used here.
        _register: registers.AcmeBankBalances,
        _previous: registers.AcmeBankBalances,
    ): collections.AcmeBankBalances[] {
        return [
            // A transfer removes from the sender and adds to the recipient.
            { account: source.sender, amount: -source.amount },
            { account: source.recipient, amount: source.amount },
        ];
    }
}
