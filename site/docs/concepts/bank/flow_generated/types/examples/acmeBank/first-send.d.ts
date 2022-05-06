
// Generated from collection schema transfers.schema.yaml.
// Referenced from first-send.flow.yaml#/collections/examples~1acmeBank~1first-send.
export type Document = {
    amount: number;
    id: number;
    recipient: string;
    sender: string;
};


// Generated from derivation register schema first-send.flow.yaml?ptr=/collections/examples~1acmeBank~1first-send/derivation/register/schema.
// Referenced from first-send.flow.yaml#/collections/examples~1acmeBank~1first-send/derivation.
export type Register = boolean;


// Generated from transform fromTransfers as a re-export of collection examples/acmeBank/transfers.
// Referenced from first-send.flow.yaml#/collections/examples~1acmeBank~1first-send/derivation/transform/fromTransfers."
import { Document as FromTransfersSource } from "./transfers";
export { Document as FromTransfersSource } from "./transfers";


// Generated from derivation first-send.flow.yaml#/collections/examples~1acmeBank~1first-send/derivation.
// Required to be implemented by first-send.flow.ts.
export interface IDerivation {
    fromTransfersUpdate(
        source: FromTransfersSource,
    ): Register[];
    fromTransfersPublish(
        source: FromTransfersSource,
        register: Register,
        previous: Register,
    ): Document[];
}
