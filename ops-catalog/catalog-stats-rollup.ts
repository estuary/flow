import { IDerivation, OutputDocument, Register, FromL1IDSource } from 'flow/ops/catalog-stats-L2/0';

// This allows for a general form of the specific types to be used for the many publish functions
// that will be templated in to this module. The word L1ID gets replaced by the most recent
// derivation used to generate the module, which is fine since the concrete types are the same for
// all derivations. The linter will try to convert an empty interface extension into a type, so it
// is disabled here.

// eslint-disable-next-line
interface AggregateSouce extends FromL1IDSource { }

// Do not change anything in this class definition block without also making the necessary changes
// in the ops_catalogs pgSQL functions. This exact structure is coupled with the templating logic to
// produce a complete set of publish functions for this derivation.
export class Derivation implements IDerivation {
    // transformsBegin
    fromL1IDPublish(source: AggregateSouce, _register: Register, _previous: Register): OutputDocument[] {
        return [source];
    }
    // transformsEnd
}
