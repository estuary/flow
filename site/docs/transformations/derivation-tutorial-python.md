---
slug: /guides/transform_data_using_python/
---

# How to Transform Data Using Python

This guide will teach you how to write and publish a simple Python derivation using async functions and Pydantic models.

:::tip
Python derivations can only be deployed to [private or BYOC data planes](/private-byoc).
:::

## Introduction<a id="introduction"></a>

This tutorial will show you how to implement a stateless transformation using Python. You'll learn how to transform raw Wikipedia events into enriched, analytics-ready edit events by extracting structured information and categorizing page types.


## Setting up your development environment<a id="setting-up-your-development-environment"></a>

In order to implement transformations through [derivations](https://docs.estuary.dev/concepts/#derivations), you'll need to set up your development environment. You'll need a text editor and [flowctl](https://docs.estuary.dev/concepts/flowctl/), the CLI-tool for Estuary installed on your machine. Check out the [docs page](https://docs.estuary.dev/concepts/flowctl/#installation-and-setup) on installation instructions.

Before continuing, sign in to the Estuary dashboard, make sure you enable access to the Wikipedia demo. Using `flowctl`, quickly verify you are able to view the demo collections used in this guide.

Execute the below command to display the documents in the `demo/wikipedia/recentchange-sampled` collection:

:::note
This collection is a 3% sample of the enormous `demo/wikipedia/recentchange` collection which contains millions of documents. Since the purpose of this tutorial is to demonstrate a proof of concept, we avoid publishing a derivation that processes hundreds of gigabytes of data.
:::

```shell
flowctl collections read --collection demo/wikipedia/recentchange-sampled --uncommitted
```

If you see a stream of JSON documents on your terminal, you're all good - feel free to cancel the process by pressing `Ctrl+C`.

Examine a sample JSON that lives in the demo collection, as this is the data you'll be using as the input for our derivation.

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

There's a bunch of fields available. For this tutorial, you'll transform these raw events into enriched edit events that are ready for analytics. You'll extract:

1. **Language code** from the domain (`"en.wikipedia.org"` → `"en"`, `"pt.wikipedia.org"` → `"pt"`)
2. **Page category** from the namespace field (0 = Article, 1 = Talk, 2 = User page, etc.)
3. **Structured event data** combining relevant fields into a clean schema

The goal is to create an output collection that a data analyst would actually want to work with - clean, enriched events with meaningful properties instead of nested raw fields.


## Writing the derivation<a id="writing-the-derivation"></a>

Set up your folder structure so you can organize the resources required for the derivation. Create a working directory to follow along, and inside, create a `flow.yaml` file.

Inside your `flow.yaml` file, add the following contents (replace `AcmeCo` with your own username or organization prefix):

```yaml
---
collections:
  AcmeCo/derivation-tutorial/wiki-edit-events:
    schema: wiki-edit-events.schema.yaml
    key:
      - /edit_id
    derive:
      using:
        python:
          module: wiki-edit-events.flow.py
      transforms:
        - name: enrichEvents
          source: demo/wikipedia/recentchange-sampled
          shuffle: any
```

The Data Flow consists of just one collection, which is what you define here, called `AcmeCo/derivation-tutorial/wiki-edit-events`.

Let's go over this in a bit more detail.

First of all, the collection needs a schema. The schema of the incoming data is already attached to the `source` collection we're using in this demo, you only have to define the schema of the documents the transformation will output.

Create a new file `wiki-edit-events.schema.yaml` alongside your `flow.yaml` with the following contents:

```yaml
type: object
properties:
  edit_id:
    type: integer
    description: Unique identifier for this edit
  wiki_language:
    type: string
    description: Language code extracted from domain (en, es, pt, etc.)
  page_title:
    type: string
    description: Title of the page being edited
  page_category:
    type: string
    description: Category of page (Article, Talk, User, etc.)
  editor_name:
    type: string
    description: Username of the editor
  is_bot:
    type: boolean
    description: Whether this edit was made by a bot
  timestamp:
    type: string
    format: date-time
    description: When the edit occurred
  edit_url:
    type: string
    description: URL to view the edit
required:
  - edit_id
  - wiki_language
  - page_title
  - page_category
  - editor_name
  - is_bot
  - timestamp
```

This schema defines clean, analytics-ready fields. Instead of keeping nested raw data like `meta.domain`, you'll extract the language code into `wiki_language`. Instead of cryptic namespace numbers, you'll map them to readable categories like "Article" or "Talk".

In the collection yaml definition, the next section defines the key of the documents.

```yaml
key:
  - /edit_id
```

Every Estuary collection must declare a key which is used to group its documents. Keys are specified as an array of JSON pointers to document locations. Since each Wikipedia edit has a unique `id` field, you'll use the transformed `edit_id` as the key. This ensures each edit event is uniquely identifiable in your collection.

The final section is where you specify that this collection is derived from another collection.

```yaml
derive:
  using:
    python:
      module: wiki-edit-events.flow.py
  transforms:
    - name: enrichEvents
      source: demo/wikipedia/recentchange-sampled
      shuffle: any
```

Here you configure the name of the Python file that will contain the transformation code and give a name to the transformation: `enrichEvents`.

The `source: demo/wikipedia/recentchange-sampled` property specifies the source collection, while `shuffle` tells Estuary how to colocate documents while processing, which in this case is set to `any`, meaning source documents can be processed by any scaled-out instance of the derivation.

Now that you have both `flow.yaml` and `wiki-edit-events.schema.yaml` created, you're ready to generate the Python scaffolding.


## Generating types<a id="generating-types"></a>

The next step is to use `flowctl` to generate Python type stubs you can use as an aid when writing the transformation code.

Execute the following command:

```shell
flowctl generate --source flow.yaml
```

If everything went well, you'll see a bunch of new files that `flowctl` generated for you in your working directory.

```shell
➜ tree
.
├── flow.yaml
├── flow_generated
│   └── python
│       └── AcmeCo
│           └── derivation_tutorial
│               └── wiki_edit_events
│                   └── __init__.py
├── pyproject.toml
├── pyrightconfig.json
├── wiki-edit-events.flow.py
└── wiki-edit-events.schema.yaml

6 directories, 6 files
```

:::note
The tree output may show additional `__init__.py` files in intermediate directories - this is expected Python package structure.
:::

The folder `flow_generated` along with the `pyproject.toml` and `pyrightconfig.json` files are things you won't have to modify during this tutorial. The `wiki-edit-events.flow.py` file was generated as a skeleton that you'll implement in the next section. If you take a look at the file that `flowctl` generated under `flow_generated/python/AcmeCo/derivation_tutorial/wiki_edit_events/__init__.py`, you can see the types you are able to use in your transformations.

```python
# Generated for published documents of derived collection AcmeCo/derivation-tutorial/wiki-edit-events
class Document(pydantic.BaseModel):
    edit_id: int
    wiki_language: str
    page_title: str
    page_category: str
    editor_name: str
    is_bot: bool
    timestamp: str
    edit_url: typing.Optional[str] = None
```

Estuary has automatically generated Pydantic models based on your collection schemas. These models give you full type safety and IDE autocomplete while writing your transformation code. Notice how these match the schema you defined - clean, structured fields for analytics.

You'll also see types for reading from source collections:

```python
# Generated for read documents of sourced collection demo/wikipedia/recentchange-sampled
class SourceEnrichEvents(pydantic.BaseModel):
    class Meta(pydantic.BaseModel):
        domain: str
        dt: str
        # ... other meta fields

    id: int
    meta: Meta
    title: str
    user: str
    namespace: int
    bot: bool
    notify_url: typing.Optional[str] = None
    # ... many other fields from the Wikipedia schema
```

The source type includes all fields from the Wikipedia events. You'll use these in your transformation to extract and map data into your enriched format.

Now, the actual transformation code will live in the file `wiki-edit-events.flow.py`. Take a look at the default contents that `flowctl generate` created:

```python
"""Derivation implementation for AcmeCo/derivation-tutorial/wiki-edit-events."""
from collections.abc import AsyncIterator
from AcmeCo.derivation_tutorial.wiki_edit_events import IDerivation, Document, Request

# Implementation for derivation AcmeCo/derivation-tutorial/wiki-edit-events.
class Derivation(IDerivation):
    async def enrich_events(self, read: Request.ReadEnrichEvents) -> AsyncIterator[Document]:
        raise NotImplementedError("enrich_events not implemented")
        if False:
            yield  # Mark as a generator.
```

Helpfully, `flowctl` provides a skeleton function. Note that the transform name `enrichEvents` has been converted to the snake_case method name `enrich_events` following Python conventions.


## The transformation code<a id="the-transformation-code"></a>

Update the function body to implement the enrichment logic:

```python
"""Derivation implementation for AcmeCo/derivation-tutorial/wiki-edit-events."""
from collections.abc import AsyncIterator
from AcmeCo.derivation_tutorial.wiki_edit_events import IDerivation, Document, Request


# Mapping of Wikipedia namespace IDs to human-readable categories
NAMESPACE_CATEGORIES = {
    0: "Article",
    1: "Talk",
    2: "User",
    3: "User Talk",
    4: "Wikipedia",
    5: "Wikipedia Talk",
    6: "File",
    14: "Category",
    # ... and many more
}


class Derivation(IDerivation):
    async def enrich_events(self, read: Request.ReadEnrichEvents) -> AsyncIterator[Document]:
        """Transform raw Wikipedia events into enriched, analytics-ready edit events."""

        # Extract language code from domain (e.g., "en.wikipedia.org" -> "en")
        domain = read.doc.meta.domain if read.doc.meta else ""
        wiki_language = domain.split('.')[0] if domain else "unknown"

        # Map namespace number to readable category
        page_category = NAMESPACE_CATEGORIES.get(read.doc.namespace, "Other")

        # Build the enriched event
        yield Document(
            edit_id=read.doc.id,
            wiki_language=wiki_language,
            page_title=read.doc.title,
            page_category=page_category,
            editor_name=read.doc.user,
            is_bot=read.doc.bot,
            timestamp=read.doc.meta.dt if read.doc.meta else "",
            edit_url=read.doc.notify_url,
        )
```

Let's break down what's happening here:

1. **Async Iterator**: The method is defined as an async generator using `async def` and `yield`. This allows Estuary to process documents efficiently in an asynchronous manner.

2. **Type Safety**: The `read` parameter is typed as `Request.ReadEnrichEvents`, which is a Pydantic model. This gives you autocomplete in your IDE and catches type errors early.

3. **Data Extraction**: You extract the language code from the domain using Python's string manipulation. For example, `"en.wikipedia.org"` becomes `"en"`.

4. **Mapping Logic**: The `NAMESPACE_CATEGORIES` dictionary maps Wikipedia's numeric namespace IDs to human-readable categories. Namespace 0 is "Article", 1 is "Talk", 2 is "User", etc.

5. **Document Construction**: You construct a new `Document` instance with clean, transformed fields. This is what makes the output analytics-ready - instead of nested raw fields, you have structured, meaningful properties.

6. **Yielding**: You use `yield` to emit the transformed document. Python derivations use async generators to efficiently process streams of documents.

Every incoming Wikipedia event gets transformed into a clean, enriched edit event with extracted metadata and human-readable categorization.


## Adding dependencies (optional)<a id="adding-dependencies"></a>

If your derivation needs additional Python packages, you can specify them in the `flow.yaml` configuration:

```yaml
derive:
  using:
    python:
      module: wiki-edit-events.flow.py
      dependencies:
        requests: ">=2.31.0"
        pandas: ">=2.0"
  transforms:
    - name: enrichEvents
      source: demo/wikipedia/recentchange-sampled
      shuffle: any
```

Estuary uses [uv](https://docs.astral.sh/uv/), a fast Python package manager, to automatically install and manage your dependencies. The `pydantic` and `pyright` packages are always included automatically.


## Verify<a id="verify"></a>

You can use `flowctl` to quickly verify your derivation before publishing it. Use the `preview` command to see the enriched events in action:

```shell
➜ flowctl preview --source flow.yaml

{"edit_id":1951027655,"wiki_language":"en","page_title":"User:Jengod/sandbox","page_category":"User","editor_name":"Jengod","is_bot":false,"timestamp":"2025-10-02T00:00:00.084Z","edit_url":"https://en.wikipedia.org/w/index.php?diff=1314539935&oldid=1314539378"}
{"edit_id":1951027661,"wiki_language":"en","page_title":"Talk:Cassini's Division","page_category":"Talk","editor_name":"Wizardman","is_bot":false,"timestamp":"2025-10-02T00:00:02.594Z","edit_url":"https://en.wikipedia.org/w/index.php?diff=1314539937&oldid=1312331396"}
{"edit_id":138916568,"wiki_language":"pt","page_title":"Cowboy Carter Tour","page_category":"Article","editor_name":"Haineee","is_bot":false,"timestamp":"2025-10-02T00:00:02.879Z","edit_url":"https://pt.wikipedia.org/w/index.php?diff=70956670&oldid=70937508"}
{"edit_id":1951027714,"wiki_language":"en","page_title":"Talk:2000 Hong Kong-Macau Interport","page_category":"Talk","editor_name":"AnomieBOT","is_bot":true,"timestamp":"2025-10-02T00:00:09.535Z","edit_url":"https://en.wikipedia.org/w/index.php?diff=1314539964&oldid=781663774"}
^C
```

Perfect! The output shows clean, enriched events with extracted language codes (`"en"`, `"pt"`), readable page categories (`"User"`, `"Talk"`, `"Article"`), and all the structured fields you defined. This is exactly what you'd want for analytics or monitoring dashboards.

You can now publish your derivation to make it run continuously:

```shell
flowctl catalog publish --source flow.yaml
```

:::warning
Publishing will activate your derivation to continuously process the Wikipedia sample stream and store the results. This will consume storage and compute resources. Make sure to delete the collection after completing the tutorial to avoid unnecessary costs.
:::

After successfully publishing your derivation, head over to the Collections page on the Web UI and you will be able to see your derivation in action!


## Wrapping up<a id="wrapping-up"></a>

In this guide you learned how to write your first stateless Python derivation to transform raw events into enriched, analytics-ready data. You've seen how:

* Estuary automatically generates Pydantic models from your JSON schemas
* Python derivations use async generators to efficiently process documents
* You can extract and transform data using simple Python logic
* Type safety helps catch errors during development
* The Python connector integrates seamlessly with the broader Python ecosystem

The enriched events you created demonstrate a real-world pattern: taking raw operational data and transforming it into clean, structured data ready for analytics, dashboards, or machine learning pipelines.

### Next steps

This tutorial covered the basics of stateless Python derivations. For more advanced patterns, check out:

* **Stateful derivations**: Learn how to maintain persistent state across documents and task restarts in the [stateful example](https://github.com/estuary/flow/blob/master/examples/derive-patterns/stateful.flow.py)
* **Async pipelining**: Process documents with bounded concurrency for API calls and I/O operations in the [pipeline example](https://github.com/estuary/flow/blob/master/examples/derive-patterns/pipeline.flow.py)
* **All derivation patterns**: Explore the complete set of examples in the [derive-patterns directory](https://github.com/estuary/flow/tree/master/examples/derive-patterns)
