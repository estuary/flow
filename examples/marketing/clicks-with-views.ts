import {
    IDerivation,
    Document,
    Register,
    IndexViewsSource,
    JoinClickWithIndexedViewsSource,
} from 'flow/marketing/clicks-with-views';

// Implementation for derivation examples/marketing/flow.yaml#/collections/marketing~1clicks-with-views/derivation.
export class Derivation implements IDerivation {
    indexViewsUpdate(source: IndexViewsSource): Register[] {
        return [source];
    }
    joinClickWithIndexedViewsPublish(
        source: JoinClickWithIndexedViewsSource,
        register: Register,
        _previous: Register,
    ): Document[] {
        return [{ ...source, view: register }];
    }
}
