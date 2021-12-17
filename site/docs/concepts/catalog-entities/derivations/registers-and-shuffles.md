---
description: Registers allow stateful processing, and shuffles help map data to registers
---

# Registers and shuffles

### Registers

Registers enable the full gamut of stateful processing workflows, including all varieties of joins and custom windowing semantics over prior events. Each register is an arbitrary JSON document that is shared between the various transformations of a derivation. It allows those transformations to communicate with one another through updates of the register’s value.&#x20;

They are sub-entity of derivations and can be implemented as follows:

```yaml
    derivation:
      register:
        schema: schemas.yaml#/$defs/myRegister
        initial: { fieldValue: null }
```

Like collections, registers always have an associated JSON schema. That schema may have reduction annotations, which are applied to fold updates of a register into a fully reduced value.

Update lambdas add updates to the register, and publish lambdas publish the contents of the register to the derived collection

Each source document is mapped to a corresponding register using the transformation’s shuffle, and a derivation may have _lots_ of distinct registers. Flow manages the mapping, retrieval, and persistence of register values.  They're backed by an embedded RocksDB instance which dynamically splits when it becomes too large to automate scaling.

### Shuffles

Transformations may provide a shuffle key as one or more [JSON-Pointer](https://tools.ietf.org/html/rfc6901) locations, to be extracted from documents of the transformation’s sourced collection. If multiple pointers are given, they’re treated as an ordered composite key. If no key is provided, Flow uses the source’s collection key instead.

During processing, every source document is mapped through its shuffle key to identify an associated register. Multiple transformations can coordinate with one another by selecting shuffle keys that reflect the same identifiers – even if those identifiers are structured differently within their respective documents.

For example, suppose we’re joining two collections related to user accounts: one transformation might use a shuffle key of `[/id]` for “account” collection documents like `{"id": 123, ...}`, while another uses key `[/account_id]` for “action” documents like `` {"account_id": 123, ...}` ``. In both cases, the shuffled entity is an account ID, and we can implement a left-join of accounts and their actions by _updating_ the register with the latest “account” document and _publishing_ “action” documents enriched by the latest “account” stored in the register.

```yaml
collections: 
  - name: example/account
    schema: 
      $ref: schema.yaml#Account
      reduce: {strategy: merge}
      required: [id]
    key: [/id]
    
    derivation:
      transform:
        fromAccountID:
          source: {name: collection/accountId}
          shuffle: [/accountId]
          publish: {lambda: typescript}
```

####
