import { interfaces, collections, registers, anchors } from 'flow/modules';

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
        source: collections.SoakSetOpsOperations,
        register: registers.SoakSetOpsSetsRegister,
        _previous: registers.SoakSetOpsSetsRegister,
    ): collections.SoakSetOpsSetsRegister[] {
        if (source.Type == 'verify') {
            return [register];
        }
        return [];
    }
}

function operationToSet(source: collections.SoakSetOpsOperations): [collections.SoakSetOpsSets] {
    if (source.Type == 'add' || source.Type == 'remove') {
        const mutate = source as anchors.MutateOp;

        return [
            {
                Author: mutate.Author,
                ID: mutate.ID,
                AppliedOps: [mutate.Op],
                Derived: {
                    [mutate.Type]: mutate.Values,
                },
                [mutate.Type == 'add' ? 'AppliedAdd' : 'AppliedRemove']: 1,
            },
        ];
    }

    const verify = source as anchors.VerifyOp;

    return [
        {
            Author: verify.Author,
            ID: verify.ID,
            AppliedOps: [verify.Op],
            ExpectAdd: verify.TotalAdd,
            ExpectRemove: verify.TotalRemove,
            ExpectValues: verify.Values,
        },
    ];
}
