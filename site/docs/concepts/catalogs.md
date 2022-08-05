---
sidebar_position: 1
---
# Catalogs

Every data flow has a **catalog**, which describes its components' details and behavior.
A catalog comprises some combination of data **captures**, stored **collections**, and **materializations** to other systems.
Optionally, it may also include other entities, like **derivations** and **tests**.

A catalog can be represented as a graph of your data flow.
The simplest has just three components.

import Mermaid from '@theme/Mermaid';

<Mermaid chart={`
	graph LR;
		Capture-->Collection;
        Collection-->Materialization;
`}/>

It may also be more complex, combining multiple entities of each type.

<Mermaid chart={`
	graph LR;
		capture/two-->collection/D;
		capture/one-->collection/C;
		capture/one-->collection/A;
        collection/A-->derivation/B;
        collection/D-->derivation/E;
        collection/C-->derivation/E;
        derivation/B-->derivation/E;
		collection/D-->materialization/one;
		derivation/E-->materialization/two;
`}/>

## The catalog YAML

Catalogs are written in the form of YAML configuration files.
The various catalog entities' configuration must follow set specifications.

There are two ways to create and work with these files.

### In the Flow web app

You don't need to write or edit the YAML files directly — the web app is designed to generate them for you.
You do have the option to review and edit the generated YAML as you create captures and materializations using the **Catalog Editor**.

### With flowctl

If you prefer a developer workflow, you can also with these files directly in your local environment using [flowctl](./flowctl.md).

Here, you'll have access to the complete directory structure of your catalog.

Your entire catalog may be described by one YAML file, or by many, so long as a top-level YAML file [imports](./import.md) all the others.

The files use the extension `*.flow.yaml` or are simply be named `flow.yaml` by convention.
Using this extension activates Flow's VS Code integration and auto-complete.
Flow integrates with VS Code for development environment support, like auto-complete,
tooltips, and inline documentation.

Depending on your catalog, you may also have TypeScript modules,
JSON schemas, or test fixtures.

## Namespace

All catalog entities (captures, materializations, and collections) are identified by a **name**
such as `acmeCo/teams/manufacturing/anvils`. Names have directory-like
prefixes and every name within Flow is globally unique.

If you've ever used database schemas to organize your tables and authorize access,
you can think of name prefixes as being akin to database schemas with arbitrary nesting.

All catalog entities exist together in a single **namespace**.
As a Flow customer, you're provisioned one or more high-level prefixes for your organization.
Further division of the namespace into prefixes is up to you.

Prefixes of the namespace, like `acmeCo/teams/manufacturing/`,
are the foundation for Flow's [authorization model](../reference/authentication.md).
