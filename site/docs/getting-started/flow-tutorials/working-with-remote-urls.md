---
sidebar_position: 4
description: Use remote URLs to call other Flow entities remotely
---

# Working with remote URLs

Now that you understand the basic concepts of Flow, you can learn more by exploring the pre-created examples in our [GitHub repository](https://github.com/estuary/flow/tree/master/examples).  It's easy to do so using remote URLs. This tutorial will walk through a few examples, but when you're done, feel free to explore and run anything you'd like.

Flow catalogs can import each other, which grants you direct access to remote collections of data. You can also directly import entities from within your own, local catalog.

### Building off remote catalogs

You can easily import catalogs by adding an `import` section to your catalog spec file:

{% code title="flow.yaml" %}
```yaml
import:
  - ../common.flow.yaml
```
{% endcode %}

Common use cases for importing catalogs include:

* Gaining access to endpoints that are managed by another group&#x20;
* Directly building on collections of data that are managed by another group

Here's a brief example in which we import a collection to derive a new view from it:

```yaml
import:
  - stock-stats/flow.yaml

collections:
  stock/new-daily-stats:
    schema: schemas/myNewSchema.yaml
    key: [/security, /date]

    derivation:
      transform:
        newTransform:
          source:
            name: stock/daily-stats
          publish: {lambda: typescript}
```

We can then implement a lambda function and schema to change `stock/daily-stats`. However, we'd like to keep it as a new derived collection based on the original collection, which another group maintains.

Flow also allows you to import entities through remote URLs, which can be hosted by another company. The only difference between local and remote imports is that the latter requires you to reference a remote URL, as shown below:

```yaml
import:
    -http://www.acme.com/myRemoteURL
```

### Referencing remote schemas

Flow also supports remote and file-based schemas. You can explore syntax in our [reference documentation](../../reference/catalog-reference/schemas-and-data-reductions.md).

### Give it a try

Try running a few examples for yourself. Find an example on GitHub that you want to check out, open `flow.yaml`,  and click the **Raw** button. You can use the URL to directly access schema. For instance, you can run tests remotely for our Citi-bike collection like this:

```yaml
flowctl test --source https://raw.githubusercontent.com/estuary/flow/master/examples/citi-bike/flow.yaml
```

Next, try creating a new `flow.yaml` file, importing a Flow collection and building on it similar to our stock statistics example above. You can imagine how easy Flow makes it to work within teams and create new data products.
