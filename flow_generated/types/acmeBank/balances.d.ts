// Generated from collection schema examples/acmeBank.flow.yaml?ptr=/collections/acmeBank~1balances/schema.
// Referenced from examples/acmeBank.flow.yaml#/collections/acmeBank~1balances.
export type Document = {
    account: string;
    amount: number;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/acmeBank.flow.yaml?ptr=/collections/acmeBank~1balances/derivation/register/schema.
// Referenced from examples/acmeBank.flow.yaml#/collections/acmeBank~1balances/derivation.
export type Register = unknown;

// Generated from transform fromTransfers as a re-export of collection acmeBank/transfers.
// Referenced from examples/acmeBank.flow.yaml#/collections/acmeBank~1balances/derivation/transform/fromTransfers."
import { SourceDocument as FromTransfersSource } from './transfers';
export { SourceDocument as FromTransfersSource } from './transfers';

// Generated from derivation examples/acmeBank.flow.yaml#/collections/acmeBank~1balances/derivation.
// Required to be implemented by examples/acmeBank.flow.ts.
export interface IDerivation {
    fromTransfersPublish(source: FromTransfersSource, register: Register, previous: Register): OutputDocument[];
}
