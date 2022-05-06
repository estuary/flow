
// Generated from collection schema balances.flow.yaml?ptr=/collections/examples~1acmeBank~1balances/schema.
// Referenced from balances.flow.yaml#/collections/examples~1acmeBank~1balances.
export type Document = {
    balance?: number;
    user: string;
};


// Generated from derivation register schema balances.flow.yaml?ptr=/collections/examples~1acmeBank~1balances/derivation/register/schema.
// Referenced from balances.flow.yaml#/collections/examples~1acmeBank~1balances/derivation.
export type Register = unknown;


// Generated from transform fromTransfers as a re-export of collection examples/acmeBank/transfers.
// Referenced from balances.flow.yaml#/collections/examples~1acmeBank~1balances/derivation/transform/fromTransfers."
import { Document as FromTransfersSource } from "./transfers";
export { Document as FromTransfersSource } from "./transfers";


// Generated from derivation balances.flow.yaml#/collections/examples~1acmeBank~1balances/derivation.
// Required to be implemented by balances.flow.ts.
export interface IDerivation {
    fromTransfersPublish(
        source: FromTransfersSource,
        register: Register,
        previous: Register,
    ): Document[];
}
