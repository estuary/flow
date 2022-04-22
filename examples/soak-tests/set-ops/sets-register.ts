import { IDerivation, Document, Register, OnOperationSource } from 'flow/soak/set-ops/sets-register';

// Implementation for derivation examples/soak-tests/set-ops/flow.yaml#/collections/soak~1set-ops~1sets-register/derivation.
export class Derivation implements IDerivation {
    onOperationUpdate(source: OnOperationSource): Register[] {
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
    onOperationPublish(_source: OnOperationSource, register: Register, _previous: Register): Document[] {
        return [register];
    }
}
