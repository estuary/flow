import {
    IDerivation,
    Document,
    Register,
    IndexClicksSource,
    IndexViewsSource,
    JoinPurchaseWithViewsAndClicksSource,
} from 'flow/marketing/purchase-with-offers';

import * as moment from 'moment';

// Implementation for derivation examples/marketing/flow.yaml#/collections/marketing~1purchase-with-offers/derivation.
export class Derivation implements IDerivation {
    indexClicksUpdate(source: IndexClicksSource): Register[] {
        const hour = moment.utc(source.timestamp).format('YYYY-MM-DD-HH');
        return [
            {
                lastSeen: source.timestamp,
                views: {},
                clicks: { [hour]: source },
            },
        ];
    }
    indexViewsUpdate(source: IndexViewsSource): Register[] {
        const day = moment.utc(source.timestamp).format('YYYY-MM-DD');
        return [
            {
                lastSeen: source.timestamp,
                views: { [day]: source },
                clicks: {},
            },
        ];
    }
    joinPurchaseWithViewsAndClicksPublish(
        source: JoinPurchaseWithViewsAndClicksSource,
        register: Register,
        _previous: Register,
    ): Document[] {
        return [
            {
                ...source,
                views: Object.values(register.views),
                clicks: Object.values(register.clicks),
            },
        ];
    }
}
