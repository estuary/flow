(doc, _) => {
    let spread = doc.ask.price - doc.bid.price;

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
