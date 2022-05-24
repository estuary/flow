---
sidebar_position: 5
---
# flowctl

There are two ways to work with Flow: through the web app, and using the flowctl command-line interface.
With flowctl, you can work on drafts and active catalogs created in the webapp with a
higher degree of control.
You can also authenticate Flow users and roles and generate Typescript modules to write custom transformations for your [derivations](derivations.md).

flowctl is the only Flow binary that you need to work with,
so distribution and upgrades are all simple.

## Installation

:::info Beta
Simplified installation is coming soon.

For now, you can [contact Estuary support](mailto:support@estuary.dev) for access.
:::

## flowctl subcommands

flowctl includes several top-level subcommands representing different functional areas. Each of these include multiple nested subcommands.
Important top-level flowctl subcommands are described below.

* `auth` allows you to authenticate your development session in your local development environment.
It's also how you provision Flow roles and users. Learn more about [authentication](../reference/authentication.md).

* `catalog` allows you to work with your organization's current active catalog. You can investigate the current deployment,
 or add its specification to a **draft**, where you can develop it further.

* `draft` allows you to work with draft catalog specifications. You can create, test, develop locally, and then **publish**, or deploy, them.

You can access full documentation of all flowctl subcommands from the command line by passing the `--help` or `-h` flag, for example:

* `flowctl --help` lists top-level flowctl subcommands

* `flowctl catalog --help` lists subcommands of `catalog`

## Build directory

When building Flow catalogs, `flowctl` uses a **build directory**
which is typically the root directory of your project, and is controlled by flag `--directory`.
Within this directory, `flowctl` creates a number of files and sub-directories.
Except where noted, it's recommended that these outputs be committed within your GitOps project.

* `flow_generated/`: ♻
  Directory of generated files, including TypeScript classes and interfaces.
  See [Typescript code generation](#typescript-code-generation).

* `*.flow.ts`: ⚓
  TypeScript modules that accompany your catalog source files.
  A stub is generated for you if your catalog source uses a TypeScript lambda, and a module doesn't yet exist.
  See [Typescript code generation](#typescript-code-generation) and
  [learn how TypeScript modules are imported](import.md#typescript-modules).

* `dist/`: ♻
  Holds JavaScript and source map files produced during TypeScript compilation.
  `dist/` should be added to your `.gitignore`.

* `node_modules/`: ♻
  Location where `npm` will download your TypeScript dependencies.
  `node_modules/` should be added to your `.gitignore`.

* `package.json` and `package-lock.json`: ♻
  Files used by `npm` to manage dependencies and your catalog's associated JavaScript project.
  You may customize `package.json`,
  but its `dependencies` stanza will be overwritten by the
  [npmDependencies](import.md#npm-dependencies)
  of your catalog source files.

* `.eslintrc.js`: ⚓
  Configures the TypeScript linter that's run as part of the catalog build process.
  Linting supplements TypeScript compilation to catch additional common mistakes and errors at build time.

* `.prettierrc.js`: ⚓
  Configures the formatter that's used to format your TypeScript files.

:::info Legend
⚓: Generated only if it does not exist. Never modified or deleted by `flowctl`.

♻: `flowctl` re-generates and overwrites contents.
:::}

### TypeScript code generation

As part of the catalog build process, Flow translates your
[schemas](schemas.md)
into equivalent TypeScript types on your behalf.
These definitions live within `flow_generated/` in your catalog build directory,
and are frequently over-written by invocations of `flowctl`.
Files in this subdirectory are human-readable and stable.
You may want to commit them as part of a GitOps-managed project, but this isn't required.

Flow also generates TypeScript module stubs for Flow catalog sources, which reference
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

If a TypeScript module exists, `flowctl` will never over-write it,
even if you update or expand your catalog sources such that the required interfaces have changed.

:::tip

If you make changes to a catalog source file `my.flow.yaml` that substantially
change the required TypeScript interfaces, try re-naming an existing
`my.flow.ts` to another name like `old.flow.ts`.

Then run `flowctl check` to re-generate a new implementation stub,
which will have correct interfaces and can be updated from the definitions of `old.flow.ts`.

:::