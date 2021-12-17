---
description: Using the firstWriteWins and lastWriteWins reduction strategies
---

# firstWriteWins and lastWriteWins

`firstWriteWins` always takes the first value seen at the annotated location. Likewise, `lastWriteWins` always takes the last. Schemas that don’t have an explicit reduce annotation default to `lastWriteWins` behavior.

```yaml
collections:
  - name: example/reductions/fww-lww
    schema:
      type: object
      reduce: { strategy: merge }
      properties:
        key: { type: string }
        fww: { reduce: { strategy: firstWriteWins } }
        lww: { reduce: { strategy: lastWriteWins } }
      required: [key]
    key: [/key]

tests:
  "Expect we can track first- and list-written values":
    - ingest:
        collection: example/reductions/fww-lww
        documents:
          - { key: "key", fww: "one", lww: "one" }
          - { key: "key", fww: "two", lww: "two" }
    - verify:
        collection: example/reductions/fww-lww
        documents:
          - { key: "key", fww: "one", lww: "two" }
```
