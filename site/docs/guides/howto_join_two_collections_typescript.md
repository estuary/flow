# How to join two collections (TypeScript)

This guide will teach you how to write and publish a TypeScript derivation, which will join two collections together on a common key.


## Introduction<a id="introduction"></a>

This tutorial will show you how to implement a stateless transformation using TypeScript. You’ll learn how to implement a flow that matches orders to customers in real-time.


## Setting up your development environment<a id="setting-up-your-development-environment"></a>

The data sources used in this tutorial are available in two Google Sheets. This one for the [orders](https://docs.google.com/spreadsheets/d/1glzIgMIeS5Fd2unb-m9J6czXdWBXK_x_ZsgzxbVUclQ/edit#gid=0) and this one for the [customers](https://docs.google.com/spreadsheets/d/1WUyM9hmRwa8B1Kz2buFcscZegPA35nvTdaHC-L3xr7U/edit#gid=0). Make a copy of each so you’ll be able to test out the pipeline by adding, editing or removing records.

Customers table sample

|              |                      |            |              |
| -----------: | -------------------- | ---------- | ------------ |
| customer\_id | email                | name       | phone        |
|          101 | customer1\@email.com | John Doe   | 123-456-7890 |
|          102 | customer2\@email.com | Jane Smith | 987-654-3210 |
|          103 | customer3\@email.com | Alex Lee   | 555-123-4567 |

Orders table sample

|           |              |                     |               |
| --------: | -----------: | ------------------: | ------------: |
| order\_id | customer\_id |         order\_date | total\_amount |
|         1 |          101 |  2024-05-10 8:00:00 |            50 |
|         2 |          102 | 2024-05-09 12:00:00 |          75.5 |
|         3 |          103 | 2024-05-08 15:30:00 |        100.25 |

As you can see, both tables contain a field called `customer_id`. This is what we’re going to use as the key in our join operation. One customer can have multiple orders, but one order can only belong to one customer. There are also some customers without any orders.

Let’s say you want to see all customers and all of their orders in the results. This means, you’ll be looking to implement a full outer join.

To create the collections in Estuary Flow, head over to the dashboard and [create](https://dashboard.estuary.dev/captures/create) a new Google Sheet capture. Give it a name and add one of the previously copied sheet’s URL as the “Spreadsheet Link”. Repeat this process for the other sheet, which should leave you with 2 collections.

You can take a look into each via the data preview window on the Collections page to verify that the sample data has already landed in Flow.

![Orders collection](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//orders_sheet_collection_b99f3c9c84/orders_sheet_collection_b99f3c9c84.png)

In order to implement transformations through [derivations](https://docs.estuary.dev/concepts/#derivations), you’ll need to set up your development environment. You’ll need a text editor and [flowctl](https://docs.estuary.dev/concepts/flowctl/), the CLI-tool for Flow installed on your machine. Check out the [docs page](https://docs.estuary.dev/concepts/flowctl/#installation-and-setup) on installation instructions.

To verify that you’re able to access Flow via `flowctl`, see if you can execute these commands successfully to view documents in your collections.

```shell
flowctl collections read --collection <your_collection_id> --uncommitted

{"_meta":{"uuid":"9790e50e-0ed3-11ef-8401-6d9be407e4b8"},"customer_id":"101","order_date":"2024-05-10 8:00:00","order_id":"1","row_id":203,"total_amount":"50"}
{"_meta":{"uuid":"9790e50e-0ed3-11ef-8801-6d9be407e4b8"},"customer_id":"102","order_date":"2024-05-09 12:00:00","order_id":"2","row_id":204,"total_amount":"75.5"}
{"_meta":{"uuid":"9790e50e-0ed3-11ef-8c01-6d9be407e4b8"},"customer_id":"103","order_date":"2024-05-08 15:30:00","order_id":"3","row_id":205,"total_amount":"100.25"}
{"_meta":{"uuid":"9790e50e-0ed3-11ef-9001-6d9be407e4b8"},"customer_id":"101","order_date":"2024-05-07 10:00:00","order_id":"4","row_id":206,"total_amount":"25.75"}
{"_meta":{"uuid":"9790e50e-0ed3-11ef-9401-6d9be407e4b8"},"customer_id":"103","order_date":"2024-05-06 14:45:00","order_id":"5","row_id":207,"total_amount":"60.2"}
{"_meta":{"uuid":"9790e50e-0ed3-11ef-9801-6d9be407e4b8"},"customer_id":"102","order_date":"2024-05-05 11:20:00","order_id":"6","row_id":208,"total_amount":"45.9"}
{"_meta":{"uuid":"9790e50e-0ed3-11ef-9c01-6d9be407e4b8"},"customer_id":"104","order_date":"2024-05-04 9:30:00","order_id":"7","row_id":209,"total_amount":"80.1"}
```

If you see something similar, you’re good to continue!


## Writing the derivation<a id="writing-the-derivation"></a>

Set up your folder structure so you can organize the resources required for the derivation. Create a working directory to follow along, and inside, create a `flow.yaml` file.

Inside your `flow.yaml `file, add the following contents:

```yaml
collections:

  Dani/join-tutorial-typescript/customers_with_orders:
    schema:
      description: >-
        A document that represents the joined result of orders with customer
        information
      type: object
      properties:
        customer_id:
          type: string
        email:
          type: string
        name:
          type: string
        phone:
          type: string
        orders:
          type: array
          items:
            $ref: orders.schema.yaml
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
            name: Dani/join-tutorial-orders/Sheet1_v2
          shuffle:
            key:
              - /customer_id
        - name: fromCustomers
          source:
            name: Dani/join-tutorial-customers/Sheet1
          shuffle:
            key:
              - /customer_id
```

Let’s take a look at this in a bit more detail. Essentially, we define one collection which it’s a `derivation` that is the result of two transformations.

In the schema definition, we specify what structure we want the documents of the result collection to take on.

```yaml
  Dani/join-tutorial-typescript/customers_with_orders:
    schema:
      description: >-
        A document that represents the joined result of orders with customer
        information
      type: object
      properties:
        customer_id:
          type: string
        email:
          type: string
        name:
          type: string
        phone:
          type: string
        orders:
          type: array
          items:
            $ref: orders.schema.yaml
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
```

Because you are going to implement a 1-to-many join using the two source collections, it’s important to pay attention to what reduction strategy Flow uses.

There are two `merge` strategies defined here, one for the `customers_with_orders `collection and for the nested `orders` array. 

:::note Merge reduces the left-hand side and right-hand side by recursively reducing shared document locations. The LHS and RHS must either both be objects, or both be arrays.
:::

For the nested merge, you have to define a key, which is one or more JSON pointers that are relative to the reduced location. If both sides are arrays and a merge key is present, then a deep sorted merge of the respective items is done, as ordered by the key. In this case, setting it to `order_id` will cause the reduction to collect all orders for a given customer.

The items in the nested array of orders are defined by the schema in a separate file, to which we refer to using `$ref: orders.schema.yaml`.

The derivation details are defined in the next section of the yaml:

```yaml
    derive:
      using:
        typescript:
          module: full-outer-join.flow.ts
      transforms:
        - name: fromOrders
          source:
            name: Dani/join-tutorial-orders/Sheet1_v2
          shuffle:
            key:
              - /customer_id
        - name: fromCustomers
          source:
            name: Dani/join-tutorial-customers/Sheet1
          shuffle:
            key:
              - /customer_id
```

This tells flow that the transformation code is defined in a TypeScript file called `full-outer-join.flow.ts` (which doesn’t exist – yet!) and that there are in fact two transformations that it expects, one for each source collection.

Shuffles let Flow identify the shard that should process a particular source document, in order to co-locate that processing with other documents it may need to know about.

Both transformations shuffle data on the same key. An important detail is that if a derivation has more than one transformation, the shuffle keys of all transformations must align with one another in terms of the extracted key types (string vs integer) as well as the number of components in a composite key.

Let’s generate the scaffolding for the derivation using `flowctl`.

```shell
flowctl generate --source flow.yaml
```

This command will create a few new files in your current working directory.

```shell
➜  tree
.
├── deno.json
├── flow.yaml
├── flow_generated
│   └── typescript
│       └── Dani
│           └── join-tutorial-typescript
│               └── customers_with_orders.ts
├── full-outer-join.flow.ts
└── orders.schema.yaml

5 directories, 5 files
```

The folder `flow_generated` along with the `deno.json` file are two things you won’t have to modify during this tutorial. If you take a look at file that `flowctl` generated under `flow_generated/typescript/<your_working_directory>/<your_prefix>/customers_with_orders.ts` you can see the types you are able to use in your transformations.

```typescript
// Generated for published documents of derived collection customers_with_orders.
export type Document = /* A document that represents the joined result of orders with customer information */ {
    customer_id: string;
    email?: string;
    name?: string;
    orders?: unknown[];
    phone?: string;
};

// Generated for read documents of sourced collection Sheet1.
export type SourceFromOrders = {
    customer_id?: string;
    order_date?: string;
    order_id?: string;
    row_id: number;
    total_amount?: string;
};

// Generated for read documents of sourced collection Sheet1.
export type SourceFromCustomers = {
    customer_id?: string;
    email?: string;
    name?: string;
    phone?: string;
    row_id: number;
};
```


Now, the actual transformation code will live in the following file: `full-outer-join.flow.ts`. Take a look at its contents.

```typescript
import { IDerivation, Document, SourceFromOrders, SourceFromCustomers } from 'flow/Dani/join-tutorial-typescript/customers_with_orders.ts';

// Implementation for derivation Dani/join-tutorial-typescript/customers_with_orders.
export class Derivation extends IDerivation {
    fromOrders(_read: { doc: SourceFromOrders }): Document[] {
      throw new Error("Not implemented");
    }
    fromCustomers(_read: { doc: SourceFromCustomers }): Document[] {
      throw new Error("Not implemented");
  }
}
```

Helpfully, `flowctl` provides two skeleton functions. Update the function body to implement the filter functionality. Modify the Derivation class like this:

```typescript
import { IDerivation, Document, SourceFromOrders, SourceFromCustomers } from 'flow/Dani/join-tutorial-typescript/customers_with_orders.ts';

// Implementation for derivation Dani/join-tutorial-typescript/customers_with_orders.
export class Derivation extends IDerivation {
    fromOrders(_read: { doc: SourceFromOrders }): Document[] {
      return [{
        customer_id: _read.doc.customer_id || "",
        orders: [_read.doc],
      }];
    }
    fromCustomers(_read: { doc: SourceFromCustomers }): Document[] {
      return [{
        customer_id: _read.doc.customer_id || "",
        email: _read.doc.email,
        name: _read.doc.name,
        phone: _read.doc.phone
      }];
  }
}
```

As you can see here, all we do is return the fields we need from each document, there’s no code required to define the actual “join” – all the heavy lifting is done in the reduction phase during materialization by the Flow runtime based on the schema you defined earlier.

Publish the derivation using `flowctl`:

```shell
flowctl catalog publish --source flow.yaml
```

After it’s successfully published, head over to the Flow dashboard to see the new collection.

![Customers with Orders collection](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//customers_with_orders_collection_d3c09d237f/customers_with_orders_collection_d3c09d237f.png)

If you take a look at the preview window at the bottom of the page, you might notice that the documents are not yet in their final, reduced form. As mentioned earlier, the reduction happens during materialization. Let's create one to show the results!

Head over to the [materialization creation page](https://dashboard.estuary.dev/materializations/create), search for Google Sheets and configure a new connector. Create a fresh Google Sheet and copy its URL as the Spreadsheet Link.

In the third configuration step, select the derivation you created as the source collection.

![Link source collection to materialization](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//source_collection_capture_e3946cd3a0/source_collection_capture_e3946cd3a0.png)

After everything looks good, press the “Save and Publish” button in the top-right corner to provision your materialization connector.

And that’s it! Go check out the sheet you created to store the results. You should see all orders associated with their respective customer in the nested array.

![Reduced results in a Google Sheet](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//results_8c3a566b86/results_8c3a566b86.png)

To test the data flow, head over to the source “Orders” sheet, and add a new order for a customer. After a few seconds, you should see the new order added to the array of existing orders of the customer. Take a few minutes to play around with different actions as well; deleting an order, adding a customer, editing details of either entity.


## Wrapping up<a id="wrapping-up"></a>

In this guide you learned how to write a TypeScript derivation to join two collections. After finishing with the tutorial, don’t forget to delete resources you don’t need anymore.
