import { IDerivation, Document, Register, FromTransfersSource } from 'flow/acmeBank/balances';

// Implementation for derivation balances.flow.yaml#/collections/acmeBank~1balances/derivation.
export class Derivation implements IDerivation {
    fromTransfersPublish(
        source: FromTransfersSource,
        _register: Register,
        _previous: Register,
    ): Document[] {
        return [
            // Debit the sender.
            { user: source.sender, balance: -source.amount },
            // Credit the recipient.
            { user: source.recipient, balance: source.amount },
        ];
    }
}
