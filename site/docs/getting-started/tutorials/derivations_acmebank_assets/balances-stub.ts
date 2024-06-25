import { IDerivation, Document, SourceFromOutcomes } from 'flow/acmeBank/balances.ts';

// Implementation for derivation acmeBank/balances.
export class Derivation extends IDerivation {
    fromOutcomes(_read: { doc: SourceFromOutcomes }): Document[] {
        throw new Error("Not implemented"); // 👈 Your implementation goes here.
    }
}
