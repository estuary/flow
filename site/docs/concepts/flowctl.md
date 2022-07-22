---
sidebar_position: 5
---
# flowctl

There are two ways to work with Flow: through the web app, and using the flowctl command-line interface.
With flowctl, you can work on drafts and active catalogs created in the web app with a
higher degree of control.
You can also authorize Flow users and roles and generate Typescript modules to write custom transformations for your [derivations](derivations.md).

flowctl is the only Flow binary that you need to work with,
so distribution and upgrades are all simple.

## Installation

flowctl binaries for MacOS and Linux can be found [here](https://go.estuary.dev/flowctl).

Download the correct binary, make it executable, and add it to your `PATH`.

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
