import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation site/docs/concepts/catalog-entities/derivations/bank/balances.flow.yaml#/collections/acmeBank~1balances/derivation.
export class AcmeBankBalances implements interfaces.AcmeBankBalances {
    fromTransfersPublish(
        source: collections.AcmeBankTransfers,
        _register: registers.AcmeBankBalances,
        _previous: registers.AcmeBankBalances,
    ): collections.AcmeBankBalances[] {
        return [
            // Debit the sender.
            { user: source.sender, balance: -source.amount },
            // Credit the recipient.
            { user: source.recipient, balance: source.amount },
        ];
    }
}
