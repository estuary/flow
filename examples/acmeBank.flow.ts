import { IDerivation, Document, Register, FromTransfersSource } from 'flow/acmeBank/balances';

// Implementation for derivation examples/acmeBank.flow.yaml#/collections/acmeBank~1balances/derivation.
export class Derivation implements IDerivation {
    fromTransfersPublish(source: FromTransfersSource, _register: Register, _previous: Register): Document[] {
        return [
            // A transfer removes from the sender and adds to the recipient.
            { account: source.sender, amount: -source.amount },
            { account: source.recipient, amount: source.amount },
        ];
    }
}
