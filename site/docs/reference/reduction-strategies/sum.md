---
description: Using the sum reduction strategy
---

# sum

`sum` reduces two numbers or integers by adding their values.

```yaml
collections:
  - name: example/reductions/sum
    schema:
      type: object
      reduce: { strategy: merge }
      properties:
        key: { type: string }
        value:
          # Sum only works with types "number" or "integer".
          # Others will throw an error at build time.
          type: number
          reduce: { strategy: sum }
      required: [key]
    key: [/key]

tests:
  "Expect we can sum two numbers":
    - ingest:
        collection: example/reductions/sum
        documents:
          - { key: "key", value: 5 }
          - { key: "key", value: -1.2 }
    - verify:
        collection: example/reductions/sum
        documents:
          - { key: "key", value: 3.8 }
```
