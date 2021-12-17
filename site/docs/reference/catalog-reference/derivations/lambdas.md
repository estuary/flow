---
description: How to implement lambdas for derivations
---

# Lambdas

{% hint style="info" %}
Currently, Flow only offers TypeScript lambdas, but more are coming in the future.&#x20;
{% endhint %}

A TypeScript file is required to create a [lambda](../../../concepts/catalog-entities/derivations/lambdas.md). If you haven't already, run the catalog with `flowctl develop` or `flowctl test`, even if the catalog isn't complete. This will [generate a TypeScript file ](../../flowctl-build-outputs.md#typescript-code-generation)and stub it out for you.

Lambdas take in a source, register, and previous value and use the language in which they were implemented to return a value in the correct schema. This value is used to either `update` a register or `publish` to a collection.

An example lambda can be seen below:

```javascript
import { collections, interfaces, registers } from 'flow/modules';

export class YourCollectionName implements interfaces.YourCollectionName {
    yourTransformName(
        source: collections.YourCollectionName,
        _register: registers.YourCollectionName,
        _previous: registers.YourCollectionName,
    ): collections.YourCollectionName[] {
            // Above here is created by Flow, the below line by the user.
            return [{ field: source.id}];
    }
}
```

Flow stubs this file out for you, so all you have to do is write the function body, which in this case is the `return`.

As the user, all you must do is ensure that `update` and `publish` return the correct schema using the specific lambda type that you've defined, such as TypeScript.
