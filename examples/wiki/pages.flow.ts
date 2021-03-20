import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/wiki/pages.flow.yaml#/collections/examples~1wiki~1pages/derivation.
export class ExamplesWikiPages implements interfaces.ExamplesWikiPages {
    rollUpEditsPublish(
        source: collections.ExamplesWikiEdits,
        _register: registers.ExamplesWikiPages,
        _previous: registers.ExamplesWikiPages,
    ): collections.ExamplesWikiPages[] {
        let stats = { cnt: 1, add: source.added, del: source.deleted };

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
