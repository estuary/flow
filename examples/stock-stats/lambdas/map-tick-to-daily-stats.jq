{
  exchange: .exchange,
  security: .security,
  # Date is produced by truncating from "2020-01-16T12:34:56Z" => "2020-01-16".
  date:   .time | fromdate | strftime("%Y-%m-%d"),
  # Price stat uses a by-volume weighted average of trades.
  price:  {low: .last.price, high: .last.price, avgN: (.last.price * .last.size), avgD: .last.size},
  # Bid, ask, and spread stats use equal weighting of observed prices across ticks.
  bid:    {low: .bid.price,  high: .bid.price,  avgN: .bid.price, avgD: 1},
  ask:    {low: .ask.price,  high: .ask.price,  avgN: .ask.price, avgD: 1},
  spread: ((.ask.price - .bid.price) as $s | {low: $s, high: $s, avgN: $s, avgD: 1}),
  volume: .last.size,
  first:  .last,
  last:   .last,
}
