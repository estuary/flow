---
description: Artifacts that are output from flowctl commands
---

# flowctl outputs

When you [run `flowctl check|develop|apply`](../concepts/flowctl.md), several things happen in order to wire up your transformation lambda code with the catalog you defined in your [catalog spec](catalog-reference/). This work happens within the working directory of `flowctl`, but this can be overridden by the `--directory` option. &#x20;

{% hint style="info" %}
The following applies to TypeScript projects, as currently only TypeScript lambdas are supported.
{% endhint %}

Within the working directory, the following files and directories are created:

* `flow_generated/`: ♻ Directory that holds the generated TypeScript classes and interfaces. See [Typescript code generation](flowctl-build-outputs.md#typescript-code-generation), below.
* `flow.ts` and `*.flow.ts`: ⚓ The implementation code for the [lambdas](../concepts/catalog-entities/derivations/lambdas.md) for your transformations. These will only be created if they don't already exist. See [Typescript code generation](flowctl-build-outputs.md#typescript-code-generation), below.
* `dist/`: ♻ Directory holding the JavaScript and source map files produced during TypeScript compilation
* `node_modules`: ♻☢ Location where `npm` will download your node dependencies.
* `package.json` and `package-lock.json`: ⚓ Files used by the `npm` build process. You may customize `package.json`, for example, to add dependencies you want to use in your lambdas.
* `.eslintrc.js`: ⚓ Configures the JavaScript linter that's run as part of the build process
* `.prettierrc.js`: ⚓ Configures the formatter that's used to format Typescript files
* `catalog.db`: ♻ File holding temporary data about the entities described by your `flow.yaml` files
* `flowctl-develop/`: ♻ Working directory that's used by the runtime when you run `flowctl develop`. This is also where data is written during local development.

{% hint style="info" %}
⚓: Will only be generated if it does not exist. Flowctl will never delete or modify this file if it does exist.&#x20;

♻ : Flowctl may re-generate and overwrite the contents. Do not modify these yourself.&#x20;

☢ : Meme warning
{% endhint %}

### TypeScript code generation

TypeScript code generation in Flow saves you time by automatically generating Typescript classes based on the JSON schemas of your collections and registers.

There are two primary code outputs. The first and most important is the `flow_generated` directory, which contains the TypeScript class and interface definitions on which your transformation depends. The contents of this directory should not be modified manually. Every time you run a `flowctl develop|check|apply` command, Flow may update the code here.

The second output is the TypeScript implementation stubs. These files have the same name as your catalog spec, so for the file `foo.flow.yaml`, `flowctl` would generate `foo.flow.ts`. These implementation stubs are _only_ generated if they do not already exist. `flowctl` will never modify or remove an existing TypeScript file outside of the `flow_generated` directory.

{% hint style="info" %}
The `flow_generated` directory does not need to be checked into source control, but it won't hurt anything if you do.
{% endhint %}
