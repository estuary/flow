---
slug: /reference/organizing-catalogs/
---

# Organizing a Flow Catalog

:::caution Beta
This page is outdated. It does not reflect the current state of the Flow web application and the
[authorization model](/reference/authentication) used to share
entities in Flow catalogs. Updates are coming soon.
:::

It's not necessary to store the entire catalog spec in one YAML file, and Flow provides the flexibility to reference other files, which can be managed independently.
You can leverage this capability when you [run Flow from the command line](/concepts/flowctl). You may want to do so if:

* You want to ensure shared collections remain easy to find
* You use group data that's managed by different teams
* You could benefit from DRY factoring things that are different per environment
* You need to manage sensitive credentials separately from materialization definitions

### `import`

Flow's [`import`](/concepts/import) directive can help you easily handle all of these scenarios while keeping your catalogs well organized. Each catalog spec file may import any number of other files, and each import may refer to either relative or an absolute URL.

When you use `import` in a catalog spec, you're conceptually bringing the entirety of another catalog — as well as the schemas and typescript files it uses — into your catalog. Imports are also transitive, so when you import another catalog, you're _also_ importing everything that other catalog has imported. This allows you to keep your catalogs organized, and is flexible enough to support collaboration between separate teams and organizations.

Perhaps the best way of explaining this is with some examples.

#### Example: Organizing collections

Let's look at a relatively simple case in which you want to organize your collections into multiple catalog files. Say you work for Acme Corp on the team that's introducing Flow. You might start with the collections and directory structure below:

```
acme/customers/customerInfo
acme/products/info/manufacturers
acme/products/info/skus
acme/products/inventory
acme/sales/pending
acme/sales/complete
```

```
acme
├── flow.yaml
├── customers
│   ├── flow.ts
│   ├── flow.yaml
│   └── schemas.yaml
├── products
│   ├── flow.yaml
│   ├── info
│   │   ├── flow.ts
│   │   ├── flow.yaml
│   │   └── schemas.yaml
│   └── inventory
│       ├── flow.ts
│       ├── flow.yaml
│       └── schemas.yaml
schemas.yaml
└── sales
    ├── flow.ts
    ├── flow.yaml
    └── schemas.yaml
```

It's immediately clear where each of the given collections is defined, since the directory names match the path segments in the collection names. This is not required by the`flowctl` CLI, but is strongly recommended, since it makes your catalogs more readable and maintainable. Each directory contains a catalog spec (`flow.yaml`), which will import all of the catalogs from child directories.

So, the top-level catalog spec, `acme/flow.yaml`, might look something like this:

```
import:
  - customers/flow.yaml
  - products/flow.yaml
  - sales/flow.yaml
```

This type of layout has a number of other advantages. During development, you can easily work with a subset of collections using, for example, `flowctl test --source acme/products/flow.yaml` to run only the tests for product-related collections. It also allows other imports to be more granular. For example, you might want a derivation under `sales` to read from `acme/products/info`. Since `info` has a separate catalog spec, `acme/sales/flow.yaml` can import `acme/products/info/flow.yaml` without creating a dependency on the `inventory` collection.

#### Example: Separate environments

It's common to use separate environments for tiers like development, staging, and production. Flow catalog specs often necessarily include endpoint configuration for external systems that will hold materialized views. Let's say you want your production environment to materialize views to Snowflake, but you want to develop locally on SQLite. We might modify the Acme example slightly to account for this.

```
acme
├── dev.flow.yaml
├── prod.flow.yaml
... the remainder is the same as above
```

Each of the top-level catalog specs might import all of the collections and define an endpoint called `ourMaterializationEndpoint` that points to the desired system. The `import` block might be the same for each system, but each file may use a different configuration for the endpoint, which is used by any materializations that reference it.

Our configuration for our development environment will look like:

```yaml title="dev.flow.yaml"
  import:
  - customers/flow.yaml
  - products/flow.yaml
  - sales/flow.yaml

  ourMaterializationEndpoint:
    # dev.flow.yaml
    sqlite:
      path: dev-materializations.db
```

While production will look like:

```yaml title="prod.flow.yaml"
import:
  - customers/flow.yaml
  - products/flow.yaml
  - sales/flow.yaml

endpoints:
    snowflake:
      account: acme_production
      role: admin
      schema: snowflake.com/acmeProd
      user: importantAdmin
      password: abc123
      warehouse: acme_production
```

When we test the draft locally, we'll work with dev.flow.yaml, but we'll publish prod.flow.yaml.

Everything will continue to work because in our development environment we'll be binding collections to our local SQLite DB and in production we'll use Snowflake.

#### Example: Cross-team collaboration

When working across teams, it's common for one team to provide a data product for another to reference and use. Flow is designed for cross-team collaboration, allowing teams and users to reference each other's full catalog or schema. &#x20;

Again using the Acme example, let's imagine we have two teams. Team Web is responsible for Acme's website, and Team User is responsible for providing a view of Acme customers that's always up to date. Since Acme wants a responsive site that provides a good customer experience, Team Web needs to pull the most up-to-date information from Team User at any point. Let's look at Team User's collections:

```yaml title="teamUser.flow.yaml"
import:
    - userProfile.flow.yaml
```

Which references:

```yaml title="userProfile.flow.yaml"
collection:
    userProfile:
        schema:
            -"/userProfile/schema"
        key:
            [/id]
```

Team User references files in their directory, which they actively manage in both their import and schema sections. If Team Web wants to access user data (and they have access), they can use a relative path or a URL-based path given that Team User publishes their data to a URL for access:

```yaml title="teamWeb.flow.yaml"
import:
    -http://www.acme.com/teamUser#userProfile.flow.yaml
    -webStuff.flow.yaml
```

Now Team Web has direct access to collections (referenced by their name) to build derived collections on top of. They can also directly import schemas:

```yaml title="webStuff.flow.yaml"
collection:
    webStuff:
        schema:
            -http://acme.com/teamUser#userProfile/#schema
        key:
            [/id]
```

### Global namespace

Every Flow collection has a name, and that name _must_ be unique within a running Flow system. Flow collections should be thought of as existing within a global namespace. Keeping names globally unique makes it easy to import catalogs from other teams, or even other organizations, without having naming conflicts or ambiguities.

For example, imagine your catalog for the inside sales team has a collection just named `customers`. If you later try to import a catalog from the outside sales team that also contains a `customers` collection, 💥 there's a collision. A better collection name would be `acme/inside-sales/customers`. This allows a catalog to include customer data from separate teams, and also separate organizations.

[Learn more about the Flow namespace.](/concepts/catalogs/#namespace)
