import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation site/docs/concepts/catalog-entities/derivations/bank/flagged-transfers.flow.yaml#/collections/acmeBank~1flagged-transfers/derivation.
export class AcmeBankFlaggedTransfers implements interfaces.AcmeBankFlaggedTransfers {
    fromTransferRecipientUpdate(source: collections.AcmeBankTransfers): registers.AcmeBankFlaggedTransfers[] {
        return [source.amount]; // Credit recipient.
    }
    fromTransferSenderUpdate(source: collections.AcmeBankTransfers): registers.AcmeBankFlaggedTransfers[] {
        return [-source.amount]; // Debit sender.
    }
    fromTransferSenderPublish(
        source: collections.AcmeBankTransfers,
        balance: registers.AcmeBankFlaggedTransfers,
        _previous: registers.AcmeBankFlaggedTransfers,
    ): collections.AcmeBankFlaggedTransfers[] {
        if (balance > 0 || source.sender == 'CREDIT') {
            return [{ ...source, balance: balance, overdrawn: false }];
        } else {
            return [{ ...source, balance: balance, overdrawn: true }];
        }
    }
}
