import { collections, interfaces, registers, transforms } from 'flow/modules';

// Implementation for derivation flow.yaml#/collections/examples~1source-schema~1restrictive/derivation.
export class ExamplesSourceSchemaRestrictive implements interfaces.ExamplesSourceSchemaRestrictive {
    fromPermissivePublish(
        source: transforms.ExamplesSourceSchemaRestrictivefromPermissiveSource,
        _register: registers.ExamplesSourceSchemaRestrictive,
        _previous: registers.ExamplesSourceSchemaRestrictive,
    ): collections.ExamplesSourceSchemaRestrictive[] {
        return [source];
    }
}
