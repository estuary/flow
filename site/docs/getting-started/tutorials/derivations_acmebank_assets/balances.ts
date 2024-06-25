import { IDerivation, Document, SourceFromOutcomes } from 'flow/acmeBank/balances.ts';

// Implementation for derivation acmeBank/balances.
export class Derivation extends IDerivation {
    fromOutcomes(read: { doc: SourceFromOutcomes }): Document[] {
        const doc = read.doc;
        return [
            // Debit the sender.
            { user: doc.sender, balance: -doc.amount },
            // Credit the recipient.
            { user: doc.recipient, balance: doc.amount },
        ];
    }
}
