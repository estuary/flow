import { interfaces, collections, registers } from 'flow/modules';

export class SoakSetOpsSets implements interfaces.SoakSetOpsSets {
    onOperationPublish(
        source: collections.SoakSetOpsOperations,
        _register: unknown,
        _previous: unknown,
    ): [collections.SoakSetOpsSets] {
        return operationToSet(source);
    }
}

export class SoakSetOpsSetsRegister implements interfaces.SoakSetOpsSetsRegister {
    onOperationUpdate(source: collections.SoakSetOpsOperations): [registers.SoakSetOpsSetsRegister] {
        return operationToSet(source);
    }
    onOperationPublish(
        _source: collections.SoakSetOpsOperations,
        register: registers.SoakSetOpsSetsRegister,
        _previous: registers.SoakSetOpsSetsRegister,
    ): collections.SoakSetOpsSetsRegister[] {
        return [register];
    }
}

function operationToSet(source: collections.SoakSetOpsOperations): [collections.SoakSetOpsSets] {
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
