import { IDerivation, Document, Register, OnOperationSource } from 'flow/soak/set-ops/sets';

// Implementation for derivation examples/soak-tests/set-ops/flow.yaml#/collections/soak~1set-ops~1sets/derivation.
export class Derivation implements IDerivation {
    onOperationPublish(source: OnOperationSource, _register: Register, _previous: Register): Document[] {
        return [
            {
                author: source.author,
                id: source.id,
                appliedOps: source.op,
                [source.type == 'add' ? 'appliedAdd' : 'appliedRemove']: 1,
                timestamp: source.timestamp,
                expectValues: source.expectValues,
                derived: {
                    [source.type]: source.values,
                },
            },
        ];
    }
}
