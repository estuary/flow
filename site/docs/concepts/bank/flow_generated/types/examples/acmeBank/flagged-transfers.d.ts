
// Generated from collection schema flagged-transfers.flow.yaml?ptr=/collections/examples~1acmeBank~1flagged-transfers/schema.
// Referenced from flagged-transfers.flow.yaml#/collections/examples~1acmeBank~1flagged-transfers.
export type Document = {
    amount: number;
    balance: number;
    id: number;
    overdrawn: boolean;
    recipient: string;
    sender: string;
};


// Generated from derivation register schema flagged-transfers.flow.yaml?ptr=/collections/examples~1acmeBank~1flagged-transfers/derivation/register/schema.
// Referenced from flagged-transfers.flow.yaml#/collections/examples~1acmeBank~1flagged-transfers/derivation.
export type Register = number;


// Generated from transform fromTransferRecipient as a re-export of collection examples/acmeBank/transfers.
// Referenced from flagged-transfers.flow.yaml#/collections/examples~1acmeBank~1flagged-transfers/derivation/transform/fromTransferRecipient."
import { Document as FromTransferRecipientSource } from "./transfers";
export { Document as FromTransferRecipientSource } from "./transfers";


// Generated from transform fromTransferSender as a re-export of collection examples/acmeBank/transfers.
// Referenced from flagged-transfers.flow.yaml#/collections/examples~1acmeBank~1flagged-transfers/derivation/transform/fromTransferSender."
import { Document as FromTransferSenderSource } from "./transfers";
export { Document as FromTransferSenderSource } from "./transfers";


// Generated from derivation flagged-transfers.flow.yaml#/collections/examples~1acmeBank~1flagged-transfers/derivation.
// Required to be implemented by flagged-transfers.flow.ts.
export interface IDerivation {
    fromTransferRecipientUpdate(
        source: FromTransferRecipientSource,
    ): Register[];
    fromTransferSenderUpdate(
        source: FromTransferSenderSource,
    ): Register[];
    fromTransferSenderPublish(
        source: FromTransferSenderSource,
        register: Register,
        previous: Register,
    ): Document[];
}
