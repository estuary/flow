---
description: Lambdas are pure functions that power transformations.
---

# Lambdas

Lambda functions power transformations in derivations. In MapReduce terms, lambdas are mappers. The Flow runtime performs combine and reduce operations using [reduction annotations](../reductions.md) provided with schemas. While reductions can be applied during captures or materializations, lambdas are only possible in derivations; thus, the full power of map-reduce, joins, and other transformations is only possible in the derivation context.

Lambdas exist as standalone files, which are referenced by derivations as in the following:

```yaml
 derivation:
      transform:
        transformOnesName:
          source:
            name: sourceOneName
            schema: "remote.schema.yaml#/$defs/withRequired"
          publish: { lambda: typescript }
```

Lambdas are anonymous [pure functions](https://en.wikipedia.org/wiki/Pure\_function) that take documents and return zero, one, or more output documents. The Flow runtime manages the execution and scale-out of lambdas. Under the hood, they're modeled as [**shards**](../../../architecture/scaling.md) and make use of hot standbys for fast fail-over and scaling.&#x20;

When you run a catalog containing a reference to a lambda such as the above, Flow automatically creates a stubbed-out file for you. You then simply add to the function body. When you next run the catalog, it will execute the function.

Note that this transform shows a **publish** lambda. Depending on the transform, you may also use an **update** lambda. You can learn more on the [Transformations](transforms.md) page.

### **TypeScript lambdas**

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

The above is a sample TypeScript lambda. Flow will stub this file out for you and all you have to do is write the function body.

[TypeScript](https://www.typescriptlang.org) is typed JavaScript that compiles to regular JavaScript during catalog builds, which Flow then executes on the [NodeJS](https://nodejs.dev) runtime. JSON Schemas are mapped to TypeScript types with high fidelity, enabling succinct and performant lambdas with rigorous type safety. Lambdas can also take advantage of the [NPM](http://npmjs.com) package ecosystem.

{% hint style="info" %}
Flow intends to support a variety of lambda languages in the future, such as Python, SQLIte, and [jq](https://stedolan.github.io/jq/).
{% endhint %}

### **Remote lambdas**

Remote endpoints are URLs that Flow invokes via JSON POST, sending batches of input documents and expecting to receive batches of output documents in return. You can invoke them as follows:

```yaml
publish: { lambda: {"remote": "http://example/api"} }
```

Remote lambdas are a means of integrating other languages and environments into a Flow derivation. Intended uses include APIs implemented in other languages and running as serverless functions (AWS lambdas, or Google Cloud Functions).
