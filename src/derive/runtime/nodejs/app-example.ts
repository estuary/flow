
import moment from 'moment';

import * as estuary from './estuary_runtime';

const bootstrapLambdas: estuary.BootstrapMap = {
	8: () => { console.log("example of a bootstrap!"); },
};

const transformLambdas: estuary.TransformMap = {
	1: (doc, state) => state.set(doc.campaign_id, doc),
	2: async (doc, state) => [
  { ...doc, campaign: await state.get(doc.campaign_id) },
]
,
	3: async (doc, state) => {
  const ts = moment.utc(doc.timestamp);
  const expiry = ts.add({days: 2});
  await state.set(doc.view_id, doc, expiry.toDate());
}
,
	4: async (doc, state) => [{ ...doc, view: await state.get(doc.view_id) }]
,
	5: async (doc, state) => {
        const ts = moment.utc(doc.timestamp);
        const key = `${doc.user_id}/views/${ts.format('YYYY-MM-DD')}`;
        const expiry = ts.add({days: 30});
        await state.set(key, doc, expiry.toDate());
}
,
	6: async (doc, state) => {
  const ts = moment.utc(doc.timestamp);
  const key = `${doc.user_id}/clicks/${ts.format('YYYY-MM-DD-HH')}`;
  const expiry = ts.add({days: 30});
  await state.set(key, doc, expiry.toDate());
}
,
	7: async (doc, state) => [{ ...doc,
  views: await state.getPrefix(`{doc.user_id}/views/`),
  clicks: await state.getPrefix(`{rec.user_id}/clicks/`),
}]
,
	9: (doc, _) => {
    const spread = doc.ask.price - doc.bid.price;

    return [{
        exchange: doc.exchange,
        security: doc.security,
        date: moment.utc(doc.time).format("YYYY-MM-DD"),
        // Price stat uses a by-volume weighted average of trades.
        price: {
            low: doc.last.price,
            high: doc.last.price,
            avgN: doc.last.price * doc.last.size,
            avgD: doc.last.size,
        },
        // Bid, ask, and spread stats use equal weighting of observed prices across ticks.
        bid: {
            low: doc.bid.price,
            high: doc.bid.price,
            avgN: doc.bid.price,
            avgD: 1,
        },
        ask: {
            low: doc.ask.price,
            high: doc.ask.price,
            avgN: doc.ask.price,
            avgD: 1,
        },
        spread: {low: spread, high: spread, avgN: spread, avgD: 1},
        volume: doc.last.size,
        first: doc.last,
        last: doc.last,
    }];
}
,
};

estuary.main(bootstrapLambdas, transformLambdas);
