# Flow user guides

In this section, you'll find step-by-step guides that walk you through common Flow tasks.

These guides are designed to help you work with Data Flows in production — we assume you have your own data and are familiar with your source and destination systems. You might be here to [get your data moving with Flow](./create-dataflow.md) as quickly as possible, [reshape your collection with a derivation](./flowctl/create-derivation.md), or [create a secure connection to your database](./connect-network.md).

If you'd prefer a tailored learning experience with sample data, check out the [Flow tutorials](../getting-started/tutorials/).

## Guides by topic

### Using the platform

* [Create a basic data flow](./create-dataflow.md)
* [Edit data flows in the web app](./edit-data-flows.md)
* [How to generate a refresh token](./how_to_generate_refresh_token.md)

### Customizing data flows

* [Secure connections](./connect-network.md)
* [Customize materialized fields](./customize-materialization-fields.md)
* [dbt cloud integration](./dbt-integration.md)
* [Schema evolution](./schema-evolution.md)
* [How to read collections as Kafka topics using Dekaf](./dekaf_reading_collections_from_kafka.md)

### Using `flowctl`

* [Getting started with `flowctl`](./get-started-with-flowctl.md)
* [Edit a Flow specification locally](./flowctl/edit-specification-locally.md)
* [Edit a draft created in the web app](./flowctl/edit-draft-from-webapp.md)
* [Using `flowctl` for automation](./flowctl/ci-cd.md)
* [Troubleshoot a task with `flowctl`](./flowctl/troubleshoot-task.md)

### Derivations

* [Create a derivation with `flowctl`](./flowctl/create-derivation.md)
* [How to transform data using SQL](./derivation_tutorial_sql.md)
* [How to transform data using TypeScript](./transform_data_using_typescript.md)
* [How to flatten an array using TypeScript](./flatten-array.md)
* [How to join two collections using TypeScript](./howto_join_two_collections_typescript.md)
* [Implementing derivations for AcmeBank](../getting-started/tutorials/derivations_acmebank.md)
