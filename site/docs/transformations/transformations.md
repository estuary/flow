---
sidebar_position: 1
description: Learn about Estuary's data transformation options for ETL and ELT pipelines, such as derivations using TypeScript, SQL, or Python, as well as dbt integrations.
---

# Transforming Data

Estuary supports various ways to transform your data, whether you do so before or after loading data to your destination.

## Transformations _before_ data load

ETL data pipelines that transform data before loading it to the destination are useful in cases where you want to:
* Reduce warehouse costs by filtering out unnecessary data before load
* Load the same transformed data to multiple different destinations
* Apply transformations outside the source system before loading data back into the source without the intermediate step of a warehouse

Estuary's [derivations](/concepts/derivations) support all these use cases.
Derivations apply transformations to one or more data collections to create a new _derived_ collection.

Find guides on working with derivations using:
* [TypeScript](/guides/transform_data_using_typescript)
* [SQL](/guides/derivation_tutorial_sql)
* [Python](/guides/transform_data_using_python)

Once you have the basics down, more advanced guides cover specific types of transformations,
like [joining two collections](/guides/howto_join_two_collections_typescript) or
[flattening an array](/guides/flatten-array).

## Transformations _after_ data load

ELT data pipelines that transform data after loading it into a destination are particularly
useful when you want to keep a complete record of your data and how it was transformed
together in your destination. It can also allow for more flexible ad-hoc queries, since
data lands as-is in the destination.

Estuary supports ELT pipelines with:
* A [dbt Cloud integration](/guides/dbt-integration)
* [Materialization triggers](/concepts/materialization-triggers), which can kick off dbt Core jobs via a GitHub action or similar workflow
