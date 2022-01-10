---
sidebar_position: 3
description: >-
  Learn how to use the command-line interface to build, test, deploy, and run
  catalogs
---

# flowctl

The `flowctl` command-line interface is used to test, develop, and deploy Flow catalogs.
It is the one and only Flow binary that you need to work with,
so distribution and upgrades are all simple.

`flowctl` includes a number of important sub-commands:

* `discover` auto-creates a catalog spec given a connector and a data source.
  It’s an assisted way to configure an endpoint data capture and scaffold a Flow project.

* `temp-data-plane` starts a small local Flow data plane.
  You are running a complete deployment of the Flow runtime, but shrunk down to run on your local machine instead of a data center.

* `deploy` builds from catalog sources and deploys into a Flow data plane.
  It's typically used to deploy to a temporary data plane for local development.

* `test` builds from catalog sources and runs all of your catalog tests.

:::tip
For help while using the `flowctl` CLI, use `--help` after any subcommand for more information.
:::

## Build Directory

When building Flow catalogs, `flowctl` uses a **build directory**
which is typically the root directory of your project, and is controlled by flag `--directory`.
Within this directory `flowctl` creates a number of files and sub-directories.
Except where noted, it's recommended that these outputs be committed within your GitOps project:

* `flow_generated/`: ♻
  Directory of generated files, including TypeScript classes and interfaces.
  See [Typescript code generation](#typescript-code-generation).

* `*.flow.ts`: ⚓
  TypeScript modules which accompany your catalog source files.
  A stub is generated for you if your catalog source uses a TypeScript lambda, and a module doesn't yet exist.
  See [Typescript code generation](#typescript-code-generation) and
  [learn how TypeScript modules are imported](catalog-entities/import.md#typescript-modules).

* `dist/`: ♻
  Holds JavaScript and source map files produced during TypeScript compilation.
  `dist/` should be added to your `.gitignore`.

* `node_modules/`: ♻
  Location where `npm` will download your TypeScript dependencies.
  `node_modules/` should be added to your `.gitignore`.

* `package.json` and `package-lock.json`: ♻
  Files used by `npm` to manage dependencies and your catalog's associated JavaScript project.
  You may customize portions of `package.json`,
  but its `dependencies` stanza will be overwritten by the
  [npmDependencies](catalog-entities/import.md#npm-dependencies)
  of your catalog source files.

* `.eslintrc.js`: ⚓
  Configures the TypeScript linter that's run as part of the catalog build process.
  Linting supplements TypeScript compilation to catch additional common mistakes and errors at build time.

* `.prettierrc.js`: ⚓
  Configures the formatter that's used to format your TypeScript files.

:::info
⚓: Generated only if it does not exist. Never modified or deleted by `flowctl`.

♻ : `flowctl` re-generates and overwrites contents.
:::}

### TypeScript Code Generation

As part of the catalog build process, Flow translates your
[schemas](catalog-entities/schemas-and-data-reductions.md)
into equivalent TypeScript types on your behalf.
These definitions live within `flow_generated/` of your catalog build directory,
and are frequently over-written by invocations of `flowctl`.
Files in this subdirectory are human-readable and stable.
You may want to commit them as part of a GitOps-managed project, but this isn't required.

Flow also generates TypeScript module stubs for Flow catalog sources which reference
a TypeScript lambda, if that particular Flow catalog source doesn't yet have an accompanying TypeScript module.
Generated stubs include implementations of the required TypeScript interfaces,
with all method signatures filled out for you:

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="acmeBank.flow.yaml" default>

```yaml
collections:
  acmeBank/balances:
    schema: balances.schema.yaml
    key: [/account]

    derivation:
      transform:
        fromTransfers:
          source: { name: acmeBank/transfers }
          publish: { lambda: typescript }
```

</TabItem>
<TabItem value="acmeBank.flow.ts (generated stub)" default>

```typescript
import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/acmeBank.flow.yaml#/collections/acmeBank~1balances/derivation.
export class AcmeBankBalances implements interfaces.AcmeBankBalances {
    fromTransfersPublish(
        _source: collections.AcmeBankTransfers,
        _register: registers.AcmeBankBalances,
        _previous: registers.AcmeBankBalances,
    ): collections.AcmeBankBalances[] {
        throw new Error("Not implemented");
    }
}
```

</TabItem>
</Tabs>

If a TypeScript module exists then `flowctl` will never over-write it,
even if you update or expand your catalog sources such that the required interfaces have changed.

:::tip

If you make changes to a catalog source file `my.flow.yaml` which substantially
change the required TypeScript interfaces, try re-naming an existing
`my.flow.ts` to another name like `old.flow.ts`.

Then run `flowctl check` to re-generate a new implementation stub,
which will have correct interfaces and can be updated from the definitions of `old.flow.ts`.

:::