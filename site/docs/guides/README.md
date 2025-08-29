# Using Estuary Flow

In this section, you'll find step-by-step guides that walk you through common Flow tasks.

These guides are designed to help you work with Data Flows in production — we assume you have your own data and are familiar with your source and destination systems. You might be here to [get your data moving with Flow](./create-dataflow.md) as quickly as possible, [reshape your collection with a derivation](./flowctl/create-derivation.md), or [create a secure connection to your database](./connect-network.md).

If you'd prefer a tailored learning experience with sample data, check out the [Flow tutorials](../getting-started/tutorials/).

## Guides by topic

### Using the platform

* [Create a basic data flow](/guides/create-dataflow)
* [Edit data flows in the web app](/guides/edit-data-flows)
* [How to generate a refresh token](/guides/how_to_generate_refresh_token)

### Customizing data flows

* [Secure connections](/guides/connect-network)
* [Customize materialized fields](/guides/customize-materialization-fields)
* [dbt cloud integration](/guides/dbt-integration)
* [Schema evolution](/guides/schema-evolution)
* [How to read collections as Kafka topics using Dekaf](/guides/dekaf_reading_collections_from_kafka)

### Using `flowctl`

* [Getting started with `flowctl`](/guides/get-started-with-flowctl)
* [Edit a Flow specification locally](/guides/flowctl/edit-specification-locally)
* [Edit a draft created in the web app](/guides/flowctl/edit-draft-from-webapp)
* [Using `flowctl` for automation](/guides/flowctl/ci-cd)
* [Troubleshoot a task with `flowctl`](/guides/flowctl/troubleshoot-task)

### Derivations

* [Create a derivation with `flowctl`](/guides/flowctl/create-derivation)
* [How to transform data using SQL](/guides/derivation_tutorial_sql)
* [How to transform data using TypeScript](/guides/transform_data_using_typescript)
* [How to flatten an array using TypeScript](/guides/flatten-array)
* [How to join two collections using TypeScript](/guides/howto_join_two_collections_typescript)
* [Implementing derivations for AcmeBank](/getting-started/tutorials/derivations_acmebank)
