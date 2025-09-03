
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `flowctl` in Automated and Programmatic Contexts

While Estuary's UI is a convenient way to create and manage resources, some users may prefer to treat their captures, materializations, and other resources as infrastructure-as-code.
This allows resource specifications to be checked into your own version control system with a clearly logged history of changes.
You can then set your infra-as-code repositories up with a CI/CD pipeline to automate deployment.

This guide will show you how to configure Estuary Flow resources programmatically for use in CI/CD workflows or other automation.

For instructions on using the UI instead, see information on Estuary's [web application](../../concepts/web-app.md).

## Creating Estuary Resources Locally

During development, you can locally create, manage, and test your resources before committing your changes.

Before creating these resources, you will need:
* An [Estuary account](https://dashboard.estuary.dev/register)
* `flowctl` [installed](../get-started-with-flowctl.md) on your machine
* An Estuary [access token](../how_to_generate_refresh_token.md)

You can authenticate your `flowctl` session in one of two ways:

* Set the `FLOW_AUTH_TOKEN` environment variable to your Estuary access token. This is the recommended way to handle a CI or automation setup.

* Or run the `flowctl auth login` command and paste in your token. This is handy for local development.

You will then be able to connect with Estuary to set up your resources.

Programmatically, all Estuary resources start with a `flow.yaml` configuration file. You can create and test this file locally, and can upload it to Estuary when ready to create your resources.

You can specify all of your resources (captures, collections, and materializations) in one `flow.yaml` file or separate them out based on resource type, schema definition, or desired data plane.

The sections below provide example configurations.

### Capture Configuration

To create a [capture](../../concepts/captures.md), start with a local `flow.yaml` file.
You will need to use the capture connector's [reference](../../reference/Connectors/capture-connectors/README.md) for details on the available settings, authorization methods, and required fields for the configuration.
The connector's reference page will include an example specification you can use to get started.

At a minimum, the configuration will need to specify:
* The capture name
* The connector image for the capture
* Any credentials needed for source system authentication
* The resource streams from the source system you wish to use

Consider these example specifications:

<Tabs>
<TabItem value="Stripe (API) Capture" default>

```yaml
captures:
  Artificial-Industries/ci-cd/source-stripe-native:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-stripe-native:dev
        config:
          credentials:
            credentials_title: Private App Credentials
            access_token: <Access Token>
    bindings:
      - resource:
          stream: charges
          syncMode: incremental
        target: Artificial-Industries/ci-cd/stripe_charges
      - resource:
          stream: plans
          syncMode: full_refresh
        target: Artificial-Industries/ci-cd/stripe_plans
```

</TabItem>

<TabItem value="PostgreSQL (Database) Capture">

```yaml
captures:
  Artificial-Industries/ci-cd/source-postgres:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-postgres:dev
        config:
          address: <host>:<port>
          database: postgres
          user: flow_capture
          password: <password>
    bindings:
      - resource:
          stream: shipments
          namespace: public
          syncMode: incremental
        target: Artificial-Industries/ci-cd/postgres_shipments
```

</TabItem>
</Tabs>

Note that you will not be able to successfully publish a capture by itself. You will also need to define the collections that relate to the capture's bindings.

### Collection Configuration

When you create a capture configuration, you will also need to create associated [collection](../../concepts/collections.md) configurations. Your resource streams will flow into these collections as a staging area before final materialization.

Collections provide an opportunity to enforce schemas and transform data with [derivations](#derivation-configuration).
When using the UI, Estuary will intelligently infer these schemas for you. When you're creating your own specifications from scratch, however, you will need to be very aware of your source system's schema in order to replicate it accurately.

Create a collection specification for each **target** you identified in your capture bindings. The collection specification should, at a minimum, include:
* The schema, with its properties and their types
* Any required fields in the schema, including the key field
* The key field used to identify and order documents
    * Since the key can be a composite, JSON pointers are used to identify relevant fields, so field names should begin with `/`

Consider these example specifications:

<Tabs>
<TabItem value="Stripe (API) Collections" default>

```yaml
collections:
  Artificial-Industries/ci-cd/stripe_charges:
    schema:
      type: object
      required:
        - id
      properties:
        id:
          type: string
        created:
          type: string
        amount:
          type: integer
        {...}
    key:
      - /id
  Artificial-Industries/ci-cd/stripe_plans:
    schema:
      type: object
      required:
        - id
      properties:
        id:
          type: string
        active:
          type: boolean
        interval:
          type: string
        {...}
    key:
      - /id
```

</TabItem>

<TabItem value="PostgreSQL (Database) Collections">

```yaml
collections:
  Artificial-Industries/ci-cd/postgres_shipments:
    schema:
      type: object
      required:
        - id
      properties:
        id:
          type: integer
        created_at:
          type: string
        is_priority:
          type: boolean
        {...}
    key:
      - /id
```

</TabItem>
</Tabs>

### Derivation Configuration

[Derivations](../../concepts/derivations.md) are often more complex than other Estuary resources.
Besides a `flow.yaml` configuration file, you may also need a TypeScript or SQL script to define the transformation. Not all pipelines will require derivation resources.

A derivation is a type of collection--one that is derived from one or more existing collections. To create a specification for a derivation, you will therefore need to [define the key, schema properties, and required fields](#collection-configuration) as for any collection.

In addition, you will need to specify how to derive this schema:
* Provide a pointer to the TypeScript or SQL file that handles the transformation
    * Or you may opt to define a lambda function within `flow.yaml` for simple transformations
* List the existing collections that provide source data for the derivation

A full example specification may therefore look like:

<Tabs>
<TabItem value="Join Collections" default>

```yaml
collections:
  Artificial-Industries/customers-with-orders:
    schema:
      type: object
      properties:
        customer_id:
          type: string
        name:
          type: string
        orders:
          type: array
          items:
            type: object
            properties:
              order_id:
                type: string
          reduce:
            strategy: merge
            key:
              - /order_id
      required:
        - customer_id
      reduce:
        strategy: merge
    key:
      - /customer_id

    derive:
      using:
        typescript:
          module: full-outer-join.flow.ts
      transforms:
        - name: fromOrders
          source:
            name: Artificial-Industries/join-collections/orders
          shuffle:
            key:
              - /customer_id
        - name: fromCustomers
          source:
            name: Artificial-Industries/join-collections/customers
          shuffle:
            key:
              - /customer_id
```

</TabItem>

<TabItem value="In-Line Derivation Function">

```yaml
collections:
  Artificial-Industries/line-item-totals:
    schema:
      type: object
      properties:
        id:
          type: string
        order_number:
          type: string
        item:
          type: string
        total:
          type: string
      required:
        - id
    key:
      - /id
  derive:
    using:
      sqlite: {}
    transforms:
      - name: fromLineItems
        source: Artificial-Industries/line-items
        shuffle: any
        lambda:
          SELECT $id,
          $order_number,
          $item,
          PRINTF('$%.2f', $price + $sales_tax) AS total;
```

</TabItem>
</Tabs>

If you specify a separate module for a transformation in your `flow.yaml`, you can generate stub files to help get started with your derivation.
When you're finished with the specification, run the following command:

```
flowctl generate --source path/to/your/flow.yaml
```

This `flowctl` command requires Docker. Successfully running it will generate relevant stub files, which you can modify to return your expected schema.

For more on configuring transformations, see other [derivation guides](../README.md#derivations).

### Materialization Configuration

Creating a new [materialization](/concepts/materialization) resource is similar to creating a capture.
In a local `flow.yaml` file, you can fill out a specification according to the materialization connector's [reference guide](../../reference/Connectors/materialization-connectors/README.md).
The reference will indicate which fields are required and how you can authenticate. It will also provide an example specification you can use to get started.

The configuration, at a minimum, will need to specify:
* The materialization name
* The connector image for the materialization
* Any credentials needed for destination system authentication
* The data collections to use as sources and which tables they should map to

Consider this example specification:

<Tabs>
<TabItem value="Snowflake (Warehouse) Materialization" default>

```yaml
materializations:
  Artificial-Industries/ci-cd/materialize-snowflake:
    endpoint:
  	  connector:
        image: ghcr.io/estuary/materialize-snowflake:dev
    	config:
          host: orgname-accountname.snowflakecomputing.com
          database: estuary_db
          schema: estuary_schema
          credentials:
            auth_type: jwt
            user: estuary_user
            privateKey: |
              -----BEGIN PRIVATE KEY-----
              MIIEv....
              ...
              ...
              -----END PRIVATE KEY-----
    bindings:
  	  - resource:
      	  table: shipments
        source: Artificial-Industries/ci-cd/postgres_shipments
```

</TabItem>
</Tabs>

## Testing Specifications

You can add [tests](../../concepts/tests.md) to your specifications to ensure baseline expected behavior. Tests are defined as any other resource. You can specify `ingest` and `verify` steps to provide and evaluate test documents.

While Estuary performs basic tests by default, it's best practice to define your own tests when working in a programmatic context.
That way, you can incorporate testing into your CI/CD workflow to ensure you only publish changes that conform with your requirements for your data.

Consider this example specification:

<Tabs>
<TabItem value="Test Derivation Output" default>

```yaml
tests:
  Artificial-Industries/tests/example:
    - ingest:
        collection: Artificial-Industries/line-items
        documents:
          - { id: "1", item: "popcorn", price: 499, sales_tax: 25 }
          - { id: "2", item: "hot dog", price: 650, sales_tax: 32 }
    - verify:
        collection: Artificial-Industries/line-item-totals
        documents:
          - { id: "1", item: "popcorn", total: "$5.24" }
          - { id: "2", item: "hot dog", total: "$6.82" }
```

</TabItem>
</Tabs>

Run tests using:

```
flowctl catalog test --source <SOURCE>
```

Your output will be similar to the following:

<Tabs>
<TabItem value="Successful Test" default>

```
test:1> Running  1  tests...
test:1> ✔️ flow://test/Artificial-Industries/tests/example :: Artificial-Industries/tests/example
test:1>
test:1> Ran 1 tests, 1 passed, 0 failed
Tests successful
```

</TabItem>
<TabItem value="Failed Test">

```
test:1> Running  1  tests...
test:1> ❌ flow://test/Artificial-Industries/tests/example failure at step /1/verify :
test:1> verify: actual and expected document(s) did not match:
test:1> mismatched document at index 0:
{...}
test:1> Ran 1 tests, 0 passed, 1 failed
```

</TabItem>
</Tabs>

## Publishing Resources

Once you're happy with your resources and any tests have passed, your automation can publish your changes to Estuary.

Your automation will need to be authenticated to use `flowctl` for your resources on your behalf. You can do so by setting the `FLOW_AUTH_TOKEN` environment variable:

```
export FLOW_AUTH_TOKEN=your_refresh_token
```

The session will then be authenticated to use the `catalog publish` command:

```
flowctl catalog publish --source <SOURCE>
```

The command's default behavior is to summarize the resource configurations to publish and prompt for confirmation. You can skip this prompt with the `--auto-approve` option.

The `catalog publish` command will [automatically encrypt any secrets](../../concepts/flowctl.md#using-flowctls-auto-encryption) in your endpoint configurations.

### Choosing a Data Plane

The `catalog publish` command defaults to publishing resources to the `ops/dp/public/gcp-us-central1-c2` data plane.
You can also specify a different public data plane or your own [private or BYOC](../../private-byoc/README.md) data plane.

You can retrieve the full name of your desired data plane from the dashboard:

1. Log into the Estuary dashboard and navigate to the [Admin](https://dashboard.estuary.dev/admin/) page.

2. Under the **Settings** tab, scroll to the **Data Planes** section.

3. Choose between the **Public** or **Private** data plane tabs.

4. Next to your desired data plane, click the **Copy** button. The full name of the data plane will be copied to your clipboard.

When publishing resources to a data plane besides the default, make sure to specify this data plane name in an option:

```
flowctl catalog publish --default-data-plane ops/dp/public/aws-eu-west-1-c2 --source ./flow.yaml
```

All resources that interact with each other (such as derivations or materializations along with their relevant sources) must be part of the same data plane.
If you wish to publish resources to different data planes, you will need to save the specifications in different files and run separate commands for each.

## Editing Existing Resources Locally

If you are starting from a published resource, you can pull the latest version from Estuary to your local directory with the `flowctl catalog pull-specs` command.

You can add options to the `pull-specs` command to target certain resources or customize your experience:

* `--captures`, `--collections`, `--materializations`: Only pull specifications from a specific type of resource
* `--name <NAME>`: Pull the specification for a single, named resource
* `--target <TARGET>`: Local root specification to write to (defaults to `flow.yaml`)
* `--overwrite`: Determine whether existing specs are overwritten by copies from the Flow control plane; useful if existing local copies have gotten out of date
* `--flat`: Determine whether specs are written to a single specification file or follow a canonical layout

:::tip
When you begin local development to update a resource specification, you may want to pull a fresh copy of the spec directly from Estuary, even if you check changes into your own infra-as-code repo. This will ensure any changes made via the UI or as part of an extended support session get captured.
:::

See more on how to [edit your specifications locally](./edit-specification-locally.md).
