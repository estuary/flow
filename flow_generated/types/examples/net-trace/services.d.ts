
// Generated from collection schema examples/net-trace/services.flow.yaml?ptr=/collections/examples~1net-trace~1services/schema.
// Referenced from examples/net-trace/services.flow.yaml#/collections/examples~1net-trace~1services.
export type Document = {
    date: string;
    service: {
        ip: string;
        port: number;
    };
    stats?: {
        bytes?: number;
        packets?: number;
    };
};


// Generated from derivation register schema examples/net-trace/services.flow.yaml?ptr=/collections/examples~1net-trace~1services/derivation/register/schema.
// Referenced from examples/net-trace/services.flow.yaml#/collections/examples~1net-trace~1services/derivation.
export type Register = unknown;


// Generated from transform fromPairs as a re-export of collection examples/net-trace/pairs.
// Referenced from examples/net-trace/services.flow.yaml#/collections/examples~1net-trace~1services/derivation/transform/fromPairs."
import { Document as FromPairsSource } from "./pairs";
export { Document as FromPairsSource } from "./pairs";


// Generated from derivation examples/net-trace/services.flow.yaml#/collections/examples~1net-trace~1services/derivation.
// Required to be implemented by examples/net-trace/services.flow.ts.
export interface IDerivation {
    fromPairsPublish(
        source: FromPairsSource,
        register: Register,
        previous: Register,
    ): Document[];
}
