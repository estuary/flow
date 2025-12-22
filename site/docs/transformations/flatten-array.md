---
slug: /guides/flatten-array/
---

# How to Flatten an Array Using TypeScript

This guide will show you how to flatten an array field in a collection by creating a TypeScript derivation in Estuary.

:::note
We'll be using TypeScript for our derivation in this guide. Check out our other guides if you're interested in using [SQL](/guides/derivation_tutorial_sql) or [Python](/guides/transform_data_using_python) for transformations.
:::

The collection we'll be working with (`user_content`) contains a field called `tags`, which is an array of objects. Each object in the array has a name and a value. We'll be flattening this array into a new collection, with two separate fields: `tag_name` and `tag_value`.

The original data looks like this:

```json
{
  "id": "1",
  "name": "example",
  "tags": [
    {
      "name": "tag1",
      "value": "value1"
    },
    {
      "name": "tag2",
      "value": "value2"
    }
  ]
}
```

The resulting data will have the following structure:

```json
{
  "tag_name": "tag1",
  "tag_value": "value1"
}
```

## Step 1: Set up your development environment

1. Ensure you have `flowctl` [installed and authenticated](/guides/get-started-with-flowctl).
2. In the Estuary dashboard, note the name of the collection you'd like to transform.
3. In your local development environment, create a working directory with a new `flow.yaml` file.

## Step 2: Set up your schema

Open your new `flow.yaml` file. This file will contain the schema for your derived collection. You'll need to modify this file to match what we want our derived collection to look like.

We'll be using the `tags` field from the original data, so we'll need to add a new property for each field we want to include in the derived collection. We'll also need to set a key for the derived collection.

Your `flow.yaml` file will therefore look something like this:

```yaml
---
collections:
  <your_tenant>/<derivation_name>:
    schema:
      type: object
      properties:
        tag_name:
          type: string
        tag_value:
          type: string
      required:
        - tag_name
        - tag_value
    key:
      - /tag_name
    derive:
      using:
        typescript:
          module: <derivation_name>.ts
      transforms:
        - name: user_content
          source: <your_tenant>/<capture_name>/public/<collection_name>
          shuffle: any
```

Copy this into your `flow.yaml` file. Replace resource names within angle brackets (eg. `<your_tenant>`) with your own information and save your changes.

## Step 3: Write your TypeScript derivation

Note that the YAML specification references a TypeScript file (`<derivation_name>.ts`) that doesn't exist yet.
You can generate a stub file for your TypeScript transformation using:

```bash
flowctl generate --source flow.yaml
```

This file is where you'll write your TypeScript code to flatten the array.

1. Open the new `<derivation_name>.ts` file.

   You'll see a basic structure for your TypeScript code. It should look something like this:

   ```typescript
   import {
     IDerivation,
     Document,
     SourceUserContent,
   } from "flow/sean-estuary/test-derivation.ts";

   export class Derivation extends IDerivation {
     userContent(_read: { doc: SourceUserContent }): Document[] {
       throw new Error("Not implemented");
     }
   }
   ```

2. Now, let's modify the `userContent` function to flatten the array. We'll loop through each document in the `SourceUserContent`, and for each document, we'll loop through the `tags` array. For each tag, we'll create a new document with the `tag_name` and `tag_value` fields.

   Update the `userContent` function to look like this:

   ```typescript
   import {
     IDerivation,
     Document,
     SourceUserContent,
   } from "flow/sean-estuary/test-derivation.ts";

   export class Derivation extends IDerivation {
     userContent(_read: { doc: SourceUserContent }): Document[] {
       const doc = _read.doc;
       const output: Document[] = [];

       if (doc.tags) {
         const tagsJson = JSON.parse(doc.tags); // Since our tags are arriving as a string from Google Sheets
         for (const tag of tagsJson) {
           output.push({
             tag_name: tag.name,
             tag_value: tag.value,
           });
         }
       }
       return output;
     }
   }
   ```

3. Save the `<derivation_name>.ts` file.

## Step 4: Preview your derivation

1. Run the following command to test your derivation:

   ```bash
   flowctl preview --source flow.yaml
   ```

2. This will show you a preview of the derived collection, including the flattened fields. Make sure everything looks good.

   For example, an original row like this:

   ```json
   {
     "_meta": {
       ...
     },
     "id": "1",
     "name": "test1",
     "tags": "[{"name":"PFJUjs6Wec","value":"HB668r7MfN"},{"name":"aIWpjtpNnj","value":"elQ9948Wpf"}]"
   }
   ```

   Should appear in your preview as two individual records:

   ```json
   {
     "_meta": {
       ...
     },
     "tag_name": "PFJUjs6Wec",
     "tag_value": "HB668r7MfN"
   }
   {
     "_meta": {
       ...
     },
     "tag_name": "aIWpjtpNnj",
     "tag_value": "elQ9948Wpf"
   }
   ```

3. Once you've confirmed your results, you can proceed to publish your derivation to Estuary:

   ```bash
   flowctl catalog publish --source flow.yaml
   ```

Congratulations! You've successfully flattened an array in TypeScript using Estuary. You can now use this technique to flatten other arrays in your data as well.
