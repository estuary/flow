---
description: Using the bigSum reduction strategy
sidebar_position: 6
---

# bigSum

`big-sum` reduces two numbers or integers by adding their values.

```yaml
collections:
  - name: example/reductions/bigSum
    schema:
      type: object
      reduce: { strategy: merge }
      properties:
        key: { type: string }
        value:
          # BigSum accepts strings and integers,
          # but will always output strings.
          type: string
          reduce: { strategy: bigSum }
      required: [key]
    key: [/key]

tests:
  "Expect we can sum two numbers":
    - ingest:
        collection: example/reductions/sum
        documents:
          - { key: "key", value: 5 }
          - { key: "key", value: "-1.2" }
    - verify:
        collection: example/reductions/sum
        documents:
          - { key: "key", value: "3.8" }
```
