---
sidebar_position: 5
---
# flowctl

The `flowctl` command-line interface is used to test, develop, and deploy Flow catalogs.
It is the only Flow binary that you need to work with,
so distribution and upgrades are all simple.

`flowctl` includes a number of important sub-commands, including:

* `discover` auto-creates a catalog specification given a connector and endpoint configuration.
  It’s an assisted way to configure an endpoint capture and scaffold a Flow project.

  [Learn more about `flowctl discover`](connectors.md#flowctl-discover)

* `temp-data-plane` starts a local, ephemeral Flow data plane.
  It runs a complete deployment of the Flow runtime,
  but shrunk down to your local machine instead of a data center.

* `deploy` builds from catalog sources and deploys into a Flow data plane.
  It's typically used to deploy to a temporary data plane for local development.

* `test` builds from catalog sources and runs all of your catalog tests.

:::tip
Additional `flowctl` commands are available for advanced users and development workflows.
To read a list of all current commands and details in the CLI, run  `flowctl --help`.
:::

## Build directory

When building Flow catalogs, `flowctl` uses a **build directory**
which is typically the root directory of your project, and is controlled by flag `--directory`.
Within this directory, `flowctl` creates a number of files and sub-directories.
Except where noted, it's recommended that these outputs be committed within your GitOps project.

* `flow_generated/`: ♻
  Directory of generated files, including TypeScript classes and interfaces.
  See [TypeScript code generation](#typescript-code-generation).

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

TypeScript files are used in the Flow catalog both as part of the automatic build process,
and to define lambdas functions for [derivations](./derivations.md), which requires your input.

As part of the catalog build process, Flow translates your
[schemas](schemas.md)
into equivalent TypeScript types on your behalf.
These definitions live within `flow_generated/` in your catalog build directory,
and are frequently over-written by invocations of `flowctl`.
Files in this subdirectory are human-readable and stable.
You may want to commit them as part of a GitOps-managed project, but this isn't required.

Whenever you define a derivation that uses a [lambda](./derivations.md#lambdas),
you must define the lambda in an accompanying TypeScript module, and reference that module
in the derivation's definition. To facilitate this,
you can generate a stub of the module using `flowctl typescript generate`
and simply write the function bodies.
[Learn more about this workflow.](./derivations.md#creating-typescript-modules)

If a TypeScript module exists, `flowctl` will never overwrite it,
even if you update or expand your catalog sources such that the required interfaces have changed.
