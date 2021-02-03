import { interfaces, collections } from 'flow/modules';

export class TestingIntStrings implements interfaces.TestingIntStrings {
    appendStringsPublish(
        source: collections.TestingIntString,
        _register: unknown,
        _previous: unknown,
    ): collections.TestingIntStrings[] {
        return [{ i: source.i, s: [source.s] }];
    }
}
