import { IDerivation, Document, Register, FromTransfersSenderSource, FromTransfersRecipientSource } from 'flow/acmeBank/flagged-transfers';

// Implementation for derivation examples/bank/flagged-transfers.flow.yaml#/collections/acmeBank~1flagged-transfers/derivation.
export class Derivation implements IDerivation {
    fromTransferRecipientUpdate(source: FromTransfersRecipientSource): Register[] {
        return [source.amount]; // Credit recipient.
    }
    fromTransferSenderUpdate(source: FromTransfersSenderSource): Register[] {
        return [-source.amount]; // Debit sender.
    }
    fromTransferSenderPublish(source: FromTransfersSenderSource, balance: Register, _previous: Register): Document [] {
        if (balance > 0 || source.sender == 'CREDIT') {
            return [{ ...source, balance: balance, overdrawn: false }];
        } else {
            return [{ ...source, balance: balance, overdrawn: true }];
        }
    }
}