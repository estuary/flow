import { IDerivation, Document, Register, FromTransferRecipientSource, FromTransferSenderSource } from 'flow/examples/acmeBank/flagged-transfers';

// Implementation for derivation flagged-transfers.flow.yaml#/collections/examples~1acmeBank~1flagged-transfers/derivation.
export class Derivation implements IDerivation {
    fromTransferRecipientUpdate(
        source: FromTransferRecipientSource,
    ): Register[] {
        return [source.amount]; // Credit recipient.
    }
    fromTransferSenderUpdate(
        source: FromTransferSenderSource,
    ): Register[] {
        return [-source.amount]; // Debit sender.
    }
    fromTransferSenderPublish(
        source: FromTransferSenderSource,
        balance: Register,
        _previous: Register,
    ): Document[] {
        if (balance > 0 || source.sender == 'CREDIT') {
            return [{ ...source, balance: balance, overdrawn: false }];
        } else {
            return [{ ...source, balance: balance, overdrawn: true }];
        }
    }
}
