# How to transform data using TypeScript

This guide will teach you how to write and publish a simple TypeScript derivation.


## Introduction<a id="introduction"></a>

This tutorial will show you how to implement a stateless transformation using TypeScript. You’ll learn how to implement a flow that filters events coming from the live, real-time Wikipedia API.


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

There’s a bunch of fields available, but as mentioned earlier, the scope of the transformation for this tutorial is limited to only one field, which lives nested inside the `meta` object.

```json
{
 ...
 "meta": {
   ...
   "domain": "commons.wikimedia.org",
   ...
  },
 ...
}
```

This field is composed of the various wikipedia domains that are used to serve different sites of the organization. This is what you’ll use as the base of the filter derivation. Let's say that the goal is to only keep events that originate from the English-language wikipedia page, which is running under the domain `en.wikipedia.org`.


## Writing the derivation<a id="writing-the-derivation"></a>

Set up your folder structure so you can organize the resources required for the derivation. Create a working directory to follow along, and inside, create a `flow.yaml` file.

Inside your `flow.yaml `file, add the following contents:

```yaml
---
collections:
  Dani/derivation-tutorial/recentchange-filtered-typescript:
    schema: recentchange-filtered.schema.yaml
    key:
      - /_meta/file
      - /_meta/offset
    derive:
      using:
        typescript:
          module: recentchange-filtered.ts
      transforms:
        - name: filter_values_typescript
          source: demo/wikipedia/recentchange-sampled
          shuffle: any
```
 

The Flow consists of just one collection, which is what you define here, called `Dani/derivation-tutorial/recentchange-filtered-typescript`.

Let’s go over this in a bit more detail.

First of all, the collection needs a schema. The schema of the incoming data (also called the [“write” schema](https://docs.estuary.dev/concepts/schemas/#write-and-read-schemas)) is already defined by the demo, you only have to define the schema of the documents the transformation will output, which is the “read” schema.

Let’s define what the final documents will look like.

```yaml
---
$schema: "http://json-schema.org/draft-07/schema#"
properties:
  _meta:
    properties:
      file:
        type: string
      offset:
        type: integer
      uuid:
        type: string
    required:
      - file
      - offset
    type: object
  domain:
    type: string
  title:
    type: string
  user:
    type: string
type: object
```

Save this schema as `recentchange-filtered.schema.yaml` next to your `flow.yaml` file.

As you can see, this schema definition includes a lot less fields than what is available in the incoming documents, this is expected, but if you wish to include more, this is where you would add them first.

In the collection yaml definition, the next section defines the key of the documents.

```yaml
key:
  - /_meta/file
  - /_meta/offset
```

Every Flow collection must declare a key which is used to group its documents. Keys are specified as an array of JSON pointers to document locations. The important detail here is to know that a collection key instructs Flow how documents of a collection are to be reduced, such as while being materialized to an endpoint. For this tutorial, you are just going to reuse the key definition of the base collection.

The final section is where you specify that this collection is derived from another collection.

```yaml
derive:
  using:
    typescript:
      module: recentchange-filtered.ts
  transforms:
    - name: filter_values_typescript
      source: demo/wikipedia/recentchange-sampled
      shuffle: any
```

Here you configure the name of the Typescript file that will contain the code for the actual transformation (don’t worry about the file not existing yet!) and give a name to the transformation. 

The `source: demo/wikipedia/recentchange-sampled` property lets Flow know that the source collection is the demo collection from mentioned at in the beginning of the tutorial while `shuffle` tells Flow how to colocate documents while processing, which in this case is set to `any`, meaning source documents can be processed by any available compute.

Alright, the configuration required for the derivation is in place, all that’s left is to write some TypeScript!


## The transformation code<a id="the-transformation-code"></a>

The next step is to use `flowctl` to generate TypeScript stubs you can use as aid when writing the transformation code.

Execute the following command:

```shell
flowctl generate --source flow.yaml
```

If everything went well, you’ll see a bunch of new files that `flowctl` generated for you in your working directory.

```shell
➜ tree
.
├── deno.json
├── flow.yaml
├── flow_generated
│   └── typescript
│       └── Dani
│           └── derivation-tutorial
│               └── recentchange-filtered-typescript.ts
├── recentchange-filtered.schema.yaml
└── recentchange-filtered.ts

5 directories, 5 files
```

The folder `flow_generated` along with the `deno.json` file are two things you won’t have to modify during this tutorial. If you take a look at file that `flowctl` generated under `flow_generated/typescript/<your_working_directory>/<your_prefix>/recentchange-filtered-typescript.ts` you can see the types you are able to use in your transformations.

```typescript
// Generated for published documents of derived collection Dani/derivation-tutorial/recentchange-filtered-typescript.
export type Document = {
    "_meta"?: {
        file: string;
        offset: number;
        uuid?: string;
    };
    domain?: string;
    title?: string;
    user?: string;
};
```

Now, the actual transformation code will live in the following file: `recentchange-filtered.ts`. Take a look at the default contents.

```typescript
import { IDerivation, Document, SourceFilterValuesTypescript } from 'flow/Dani/derivation-tutorial/recentchange-filtered-typescript.ts';

// Implementation for derivation Dani/derivation-tutorial/recentchange-filtered-typescript.
export class Derivation extends IDerivation {
    filterValuesTypescript(_read: { doc: SourceFilterValuesTypescript }): Document[] {
        throw new Error("Not implemented");
    }
}
```

Helpfully, `flowctl` provides a skeleton function. Update the function body to implement the filter functionality.

```typescript
export class Derivation extends IDerivation {
    filterValuesTypescript(_read: { doc: SourceFilterValuesTypescript }): Document[] {
        if (_read.doc.meta?.domain == 'en.wikipedia.org') {
            return [{
                "_meta": {
                    "file": _read.doc._meta.file,
                    "offset": _read.doc._meta.offset,
                    "uuid": _read.doc._meta.uuid,
                },
                "domain": _read.doc.meta.domain,
                "title": _read.doc.title,
                "user": _read.doc.user
            }];
        }
        else {
            return []
        }
    }
}
```

As you can see, only documents which contain the “en.wikipedia.org” domain are being returned, in addition to discarding most fields from the incoming record, and just keeping the ones defined in the collection schema.


## Verify<a id="verify"></a>

You can use `flowctl` to quickly verify your derivation before publishing it. Use the `preview` command to get an idea of the resulting collections.

```shell
➜ flowctl preview --source flow.yaml --name Dani/derivation-tutorial/recentchange-filtered-typescript

{"_meta":{"file":"recentchange","offset":13757,"uuid":"079296fe-f5c0-11ee-9401-4fdf95f7b91a"},"domain":"en.wikipedia.org","title":"Adoption","user":"JustBeCool"}
{"_meta":{"file":"recentchange","offset":13772,"uuid":"082ae4fc-f5c0-11ee-8801-4fdf95f7b91a"},"domain":"en.wikipedia.org","title":"Wikipedia:Teahouse","user":"Subanark"}
{"_meta":{"file":"recentchange","offset":13774,"uuid":"082ae4fc-f5c0-11ee-9001-4fdf95f7b91a"},"domain":"en.wikipedia.org","title":"Islandia, New York","user":"204.116.28.102"}
^C
```

As you can see, the output format matches the defined schema.  The last step would be to publish your derivation to Flow, which you can also do using `flowctl`.

:::warning Publishing the derivation will initialize the transformation on the live, real-time Wikipedia stream, make sure to delete it after completing the tutorial.
:::

```shell
flowctl catalog publish --source flow.yaml
```

After successfully publishing your derivation, head over to the Collections page on the Web UI and you will be able to see your derivation in action!

![Verify Derivation on Web UI](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//Screenshot_2024_05_09_at_14_06_03_90f8bb7c34/Screenshot_2024_05_09_at_14_06_03_90f8bb7c34.png)


## Wrapping up<a id="wrapping-up"></a>

In this guide you learned how to write your first stateless TypeScript derivation to filter data in a collection.
