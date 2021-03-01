import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/int-string.flow.yaml#/collections/testing~1int-strings/derivation.
export class TestingIntStrings implements interfaces.TestingIntStrings {
    appendStringsPublish(
        source: collections.TestingIntString,
        _register: registers.TestingIntStrings,
        _previous: registers.TestingIntStrings,
    ): collections.TestingIntStrings[] {
        return [{ i: source.i, s: [source.s] }];
    }
}
