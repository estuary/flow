---
slug: /guides/derivation_tutorial_sql/
---

# How to Transform Data Using SQL

This guide will teach you how to write and publish a simple SQL derivation that you can use to transform data from one collection to another.


## Introduction<a id="introduction"></a>

This tutorial will show you how to implement a stateless transformation using SQL. You’ll learn how to implement a flow that transforms events coming from the live, real-time Wikipedia API.


## Setting up your development environment<a id="setting-up-your-development-environment"></a>

In order to implement transformations through [derivations](https://docs.estuary.dev/concepts/#derivations), you’ll need to set up your development environment. You’ll need a text editor and [flowctl](https://docs.estuary.dev/concepts/flowctl/), the CLI-tool for Flow installed on your machine. Check out the [docs page](https://docs.estuary.dev/concepts/flowctl/#installation-and-setup) on installation instructions.

Before continuing, sign in to the Estuary Flow dashboard, make sure you enable access to the Wikipedia demo. Using `flowctl`, quickly verify you are able to view the demo collections used in this guide.

Execute the below command to display the documents in the `demo/wikipedia/recentchange-sampled` collection:

:::note
This collection is a 3% sample of the enormous `demo/wikipedia/recentchange` collection which contains millions of documents. Since the purpose of this tutorial is to demonstrate a proof of concept, we avoid publishing a derivation that processes hundreds of gigabytes of data.
:::

```shell
flowctl collections read --collection demo/wikipedia/recentchange-sampled --uncommitted
```

If you see a stream of JSON documents on your terminal, you’re all good - feel free to cancel the process by pressing `C^C`.

Examine a sample JSON that lives in the demo collection, as this is the data you’ll be using as the input for our derivation.

```json
{
  "$schema": "/mediawiki/recentchange/1.0.0",
  "_meta": {
    "file": "recentchange",
    "offset": 12837,
    "uuid": "f8f07d87-f5bf-11ee-8401-4fdf95f7b91a"
  },
  "bot": false,
  "comment": "[[:File:Jeton. Ordinaire des guerres - btv1b10405460g (1 of 2).jpg]] added to category",
  "id": 2468434138,
  "meta": {
    "domain": "commons.wikimedia.org",
    "dt": "2024-04-08T15:52:13Z",
    "id": "d9e8698f-4eac-4262-a451-b7ca247e401c",
    "offset": 5008568732,
    "partition": 0,
    "request_id": "b5372124-63fa-45e1-b35e-86784f1692bc",
    "stream": "mediawiki.recentchange",
    "topic": "eqiad.mediawiki.recentchange",
    "uri": "https://commons.wikimedia.org/wiki/Category:Jetons"
  },
  "namespace": 14,
  "notify_url": "https://commons.wikimedia.org/w/index.php?diff=866807860&oldid=861559382&rcid=2468434138",
  "parsedcomment": "<a href=\"/wiki/File:Jeton._Ordinaire_des_guerres_-_btv1b10405460g_(1_of_2).jpg\" title=\"File:Jeton. Ordinaire des guerres - btv1b10405460g (1 of 2).jpg\">File:Jeton. Ordinaire des guerres - btv1b10405460g (1 of 2).jpg</a> added to category",
  "server_name": "commons.wikimedia.org",
  "server_script_path": "/w",
  "server_url": "https://commons.wikimedia.org",
  "timestamp": 1712591533,
  "title": "Category:Jetons",
  "title_url": "https://commons.wikimedia.org/wiki/Category:Jetons",
  "type": "categorize",
  "user": "DenghiùComm",
  "wiki": "commonswiki"
}
```

The transformation in this tutorial will make use of the `length`, `bot` and `user_id` fields to calculate how many lines a given non-bot user has modified on a day.

```json
{
 ...
 "user_id": "User"
 "bot": 0
 "length": 1253
 ...
}
```

## Writing the derivation<a id="writing-the-derivation"></a>

Set up your folder structure so you can organize the resources required for the derivation. Create a working directory to follow along, and inside, create a `flow.yaml` file.

Inside your `flow.yaml` file, add the following contents:

```yaml
---
collections:
  Dani/derivation-tutorial/edits-by-users:

    schema:
      type: object
      properties:
        user_id:
          type: string
        date:
          format: date
          type: string
        total_edits:
          reduce:
            strategy: sum
          type: number
        total_new_lines:
          reduce:
            strategy: sum
          type: number
      reduce:
        strategy: merge
      required:
        - date
        - user_id

    key:
      - /date
      - /user_id

    derive:
      using:
        sqlite: {}
      transforms:
        - name: edits_by_users
          source: demo/wikipedia/recentchange-sampled
          shuffle: any
          lambda: |
            select
            $user as user_id,
            substr($meta$dt,1,10) as date,
            1 as total_edits,
            coalesce($length$new - $length$old, 0) as total_new_lines
            where $type = 'edit' and $user is not null and $bot = 0;
```
 

The Flow consists of just one collection, which is what you define here, called `edits-by-users`.

Let’s go over this in a bit more detail.

First of all, the collection needs a schema. The schema of the incoming data (also called the [“write” schema](https://docs.estuary.dev/concepts/schemas/#write-and-read-schemas)) is already defined by the demo, you only have to define the schema of the documents the transformation will output, which is the “read” schema.

In the `flow.yaml` file, the schema is defined in-line with the rest of the configuration.

```yaml
schema:
  type: object
  properties:
    user_id:
      type: string
    date:
      format: date
      type: string
    total_edits:
      reduce:
        strategy: sum
      type: number
    total_new_lines:
      reduce:
        strategy: sum
      type: number
  reduce:
    strategy: merge
  required:
    - date
    - user_id
```

As you can see, this schema includes less fields than what is available in the incoming documents, this is expected, but if you wish to include more, this is where you would add them first.

The `user_id` and `date` fields do not contain any modifications, but the other two have their reduction strategy defined as well to be `sum`. This strategy reduces two numbers or integers by adding their values.

To learn more about how reduction strategies work, check out the [documentation](https://docs.estuary.dev/reference/reduction-strategies/) page.

Moving on, the next section in the yaml file defines the key of the documents.

```yaml
key:
  - /date
  - /user_id
```

Every Flow collection must declare a key which is used to group its documents. Keys are specified as an array of JSON pointers to document locations. The important detail here is to know that a collection key instructs Flow how documents of a collection are to be reduced, such as while being materialized to an endpoint.

The final section is where you specify that this collection is derived from another collection.

```yaml
derive:
  using:
    sqlite: {}
  transforms:
    - name: edits_by_users
      source: demo/wikipedia/recentchange-sampled
      shuffle: any
      lambda: |
        select
        $user as user_id,
        substr($meta$dt,1,10) as date,
        1 as total_edits,
        coalesce($length$new - $length$old, 0) as total_new_lines
        where $type = 'edit' and $user is not null and $bot = 0;
```

Here you define the SQL statement that gets executed on the documents of the source collection.

The `source: demo/wikipedia/recentchange-sampled` property lets Flow know that the source collection is the demo collection from mentioned at in the beginning of the tutorial while `shuffle` tells Flow how to colocate documents while processing, which in this case is set to `any`, meaning source documents can be processed by any available compute.

The SQL is straightforward

```sql
select
    $user as user_id,
    substr($meta$dt,1,10) as date,
    1 as total_edits,
    coalesce($length$new - $length$old, 0) as total_new_lines
where $type = 'edit' and $user is not null and $bot = 0
```

We select the `user_id`, parse the event `date` and calculate the amount of line changes. We also select `1` for the value of `total_edits`, this is important because during the reduction phase, due to having selected `sum` as the strategy, these values will get added together to form the total number of edits in the result. We also filter out non-edit events, bot users or events without a user\_id to have a somewhat clean dataset.


## Verify<a id="verify"></a>

You can use `flowctl` to quickly verify your derivation before publishing it. Use the `preview` command to get an idea of the resulting collections.

```shell
flowctl preview --source flow.yaml --name Dani/derivation-tutorial/edits-by-users

{"date":"2024-04-08","total_edits":3,"total_new_lines":110,"user_id":"Renamerr"}
{"date":"2024-04-08","total_edits":1,"total_new_lines":769,"user_id":"Sebring12Hrs"}
{"date":"2024-04-08","total_edits":5,"total_new_lines":3360,"user_id":"Sic19"}
{"date":"2024-04-08","total_edits":1,"total_new_lines":82,"user_id":"Simeon"}
^C
```

As you can see, the output format matches the defined schema.  The last step would be to publish your derivation to Flow, which you can also do using `flowctl`.

:::warning
Publishing the derivation will initialize the transformation on the live, real-time Wikipedia stream, make sure to delete it after completing the tutorial.
:::

```shell
flowctl catalog publish --source flow.yaml
```

After successfully publishing your derivation, head over to the Collections page on the Web UI and you will be able to see your derivation in action!

![Verifying derivation on Web UI](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//Screenshot_2024_05_09_at_17_04_08_7aa8dc036d/Screenshot_2024_05_09_at_17_04_08_7aa8dc036d.png)


## Wrapping up<a id="wrapping-up"></a>

In this guide you learned how to write your first stateless SQL derivation to filter data in a collection.
