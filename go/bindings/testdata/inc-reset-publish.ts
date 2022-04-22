import { IDerivation, Document, Register, IncrementSource, PublishSource, ResetSource } from 'flow/derivation';

// Implementation for derivation go/bindings/testdata/inc-reset-publish.flow.yaml#/collections/derivation/derivation.
export class Derivation implements IDerivation {
    incrementUpdate(
        _source: IncrementSource,
    ): Register[] {
        throw new Error("Not implemented");
    }
    publishPublish(
        _source: PublishSource,
        _register: Register,
        _previous: Register,
    ): Document[] {
        throw new Error("Not implemented");
    }
    resetUpdate(
        _source: ResetSource,
    ): Register[] {
        throw new Error("Not implemented");
    }
    resetPublish(
        _source: ResetSource,
        _register: Register,
        _previous: Register,
    ): Document[] {
        throw new Error("Not implemented");
    }
}
