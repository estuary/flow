
// Generated from collection schema transfers.schema.yaml.
// Referenced from last-large-send.flow.yaml#/collections/examples~1acmeBank~1last-large-send.
export type Document = {
    amount: number;
    id: number;
    recipient: string;
    sender: string;
};


// Generated from derivation register schema last-large-send.flow.yaml?ptr=/collections/examples~1acmeBank~1last-large-send/derivation/register/schema.
// Referenced from last-large-send.flow.yaml#/collections/examples~1acmeBank~1last-large-send/derivation.
export type Register = unknown;


// Generated from transform fromTransfers as a re-export of collection examples/acmeBank/transfers.
// Referenced from last-large-send.flow.yaml#/collections/examples~1acmeBank~1last-large-send/derivation/transform/fromTransfers."
import { Document as FromTransfersSource } from "./transfers";
export { Document as FromTransfersSource } from "./transfers";


// Generated from derivation last-large-send.flow.yaml#/collections/examples~1acmeBank~1last-large-send/derivation.
// Required to be implemented by last-large-send.flow.ts.
export interface IDerivation {
    fromTransfersPublish(
        source: FromTransfersSource,
        register: Register,
        previous: Register,
    ): Document[];
}
