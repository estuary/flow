import { IDerivation, Document, Register, RollUpEditsSource } from 'flow/examples/wiki/pages';

// Implementation for derivation examples/wiki/pages.flow.yaml#/collections/examples~1wiki~1pages/derivation.
export class Derivation implements IDerivation {
    rollUpEditsPublish(source: RollUpEditsSource, _register: Register, _previous: Register): Document[] {
        const stats = { cnt: 1, add: source.added, del: source.deleted };

        if (source.countryIsoCode) {
            return [
                {
                    page: source.page,
                    byCountry: { [source.countryIsoCode]: stats },
                    ...stats,
                },
            ];
        }
        // Unknown country.
        return [{ page: source.page, ...stats }];
    }
}
