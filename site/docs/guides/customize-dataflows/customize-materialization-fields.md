---
slug: /guides/customize-materialization-fields/
---

# Customize Materialized Fields

Estuary Flow can auto-discover data resources and schemas, and implements a priority-based system that intelligently selects fields to materialize.
However, you may wish to override these defaults to customize the final format of your materialized tables.
For example, columns you require may be missing or may need specific names to work with downstream systems.
Or you might wish to keep columns with sensitive data from materializing entirely.

This happens when the collection's JSON schema doesn't map to a table schema appropriate for your use case.

You can control the shape and appearance of materialized tables using a two-step process.

First, you modify the source collection **schema**.
You can change column names by adding **[projections](/concepts/advanced/projections)**:
JSON pointers that turn locations in a document's JSON structure into custom named fields.

Then, you add the `fields` stanza to the materialization specification, telling Flow which fields to materialize.

You can manage both of these options through Estuary's dashboard or modify them directly in the resource specification file.

The following sections break down the process in more detail.

:::info Hint
If you just need to add a field that isn't included by default and it's already present in the schema
with a name you like, skip ahead to [include desired fields in your materialization](#field-selection-for-materializations).
:::

## Capture desired fields and generate projections

Any field you eventually want to materialize must be included in the collection's schema.
It's ok if the field is nested in the JSON structure; you'll flatten the structure with **projections**.

:::caution
In this workflow, you'll edit a collection. This change can impact other downstream materializations and derivations.
Use caution and be mindful of any edit's consequences before publishing.
:::

### Captured collections

If the collection you're using was captured directly, follow these steps.

1. Go to the [Captures](https://dashboard.estuary.dev/captures) page of the Flow web app
and locate the capture that produced the collection.

2. Select your capture and click the **Edit** button.

3. Under **Target Collections**, choose the binding that corresponds to the collection.
Then, click the **Collection** tab.

4. In the list of fields, look for the fields you want to materialize.
If they're present and correctly named, you can skip to
[including them in the materialization](#field-selection-for-materializations).

:::info hint
Compare the field name and pointer.
For nested pointers, you'll probably want to change the field name to omit slashes.
:::

5. If you need to change your fields, you can edit the collection schema.

   If your desired fields aren't present and your capture does not automatically keep schemas up to date, you can edit the schema directly:

   1. Click **Edit**.

   2. Add missing fields to the schema in the correct location based on the source data structure.

   3. Click **Close**.

   If you simply want to rename existing fields, you can provide alternate names for individual fields:

   1. In the Schema table, click the **Rename** button for the field you wish to change.

   2. In the **Alternate Name** modal, provide the field's **New Name**.

   3. Click **Apply**.

6. Repeat steps 3 through 5 with other collections, if necessary.

7. You can [backfill](/reference/backfilling-data) affected collections to ensure historical data is populated with your new projections.

8. Click **Save and Publish**.

:::info
You can also add projections manually with `flowctl`.
Refer to the guide to [editing with flowctl](/guides/flowctl/edit-specification-locally) and
[how to format projections](/concepts/collections/#projections).
:::

### Derived collections

If the collection you're using came from a derivation, follow these steps.

1. [Pull the derived collection's specification locally](/guides/flowctl/edit-specification-locally/#pull-specifications-locally) using `flowctl`.

```
flowctl catalog pull-specs --name <yourOrg/full/collectionName>
```

2. Review the collection's schema to see if the fields of interest are included. If they're present, you can skip to
[including them in the materialization](#field-selection-for-materializations).

3. If your desired fields aren't present or are incorrectly named, add any missing fields to the schema in the correct location based on the source data structure.

4. Use schema inference to generate projections for the fields.

```
flowctl preview --infer-schema --source <full\path\to\flow.yaml> --collection <yourOrg/full/collectionName>
```

5. Review the updated schema. Manually change the names of projected fields. These names will be used by the materialization and shown in the endpoint system as column names or the equivalent.

6. [Re-publish the collection specification](/guides/flowctl/edit-specification-locally/#edit-source-files-and-re-publish-specifications).

## Field selection for materializations

Now that all your fields are present in the collection schema as projections,
you can choose which ones to include in the materialization.

Estuary automatically detects fields and uses a priority-based selection system to determine the fields to include or exclude in the materialization.

This means that, for each field, a stronger selection reason will override a weaker rejection reason, and vice versa.
This helps ensure that critical fields get materialized.

Every included field will be mapped to a table column or equivalent in the endpoint system.

1. If you haven't created the materialization, [begin the process](/guides/create-dataflow/#create-a-materialization). Pause once you've selected the collections to materialize.

   If your materialization already exists, navigate to the [edit materialization](/guides/edit-data-flows/#edit-a-materialization) page.

2. In the Collection Selector, choose the collection whose output fields you want to change.

3. In the **Config** tab, scroll down to the **Field Selection** table.

4. Review the listed fields in the field selection table.

   Estuary checks each field against a number of selection and rejection criteria to inform the default materialized fields.
   You can customize this behavior further with **modes** and individual **field overrides**.

   The field selection table will provide an **Outcome** for each field:

   * **Field included**: The field will be included in the materialization. Symbolized by a filled bookmark.
   * **Field excluded**: The field will not be included in the materialization. Symbolized by an empty bookmark.
   * **Conflict**: The field matches criteria for both selection and rejection.
   Symbolized by a warning sign. The outcome tooltip provides detailed information on the conflict.

5. Choose whether to start with one of Flow's field selection **modes**. You can customize individual fields later. Modes include and exclude fields based on field depth:

   * **Depth Zero:** Only selects top-level fields
   * **Depth One:** Selects object fields with one degree of nesting
   * **Depth Two:** Selects object fields with two degrees of nesting
   * **Unlimited Depth:** Selects all fields

   Selecting a depth limit can help prevent over-materializing complex document structures.
   If you don't select a mode, Estuary will default to **Depth One**.

6. You can modify individual fields by choosing to **require** or **exclude** them.

   ![Field selection modes and individual options](../guide-images/field-selection.png)

7. Repeat steps 2 through 5 with other collections, if necessary.

8. Click **Save and Publish**.

The named, included fields will be reflected in the endpoint system.

### Group-By Keys

In addition to selecting fields to materialize, you can also specify which of those fields should be used as group-by keys.
This lets you choose keys independent of the collection key structure.

To set custom group-by keys for your materialized bindings:

1. View the [Field Selection](#field-selection-for-materializations) table for a binding.

2. At the top of the table, click the **Group By** button.

   This will open a modal where you can configure your keys.

3. Select fields that you would like to use as **keys** from the dropdown list of available options.

   You can select multiple fields to specify an ordered array of primary keys.
   Selected fields will be displayed as distinct chips, which you can click and drag to reorder.

   :::tip
   You can only select fields with a defined scalar type. Objects and other complex data types are not viable keys.
   :::

4. Click **Apply**.

Key fields will be pinned to the top of the Field Selection table with a key icon.

If you are editing group-by keys for an existing materialization, changes may result in affected bindings being [**backfilled**](/reference/backfilling-data/#materialization-backfill).

### Usage in Specifications

You can define selected fields and group-by keys directly in your [materialization's specification](/concepts/materialization/#specification) file rather than through the UI.

If you are configuring your connector using the **Advanced Specification Editor** or through local `flow.yaml` files, you will need to update the `fields` stanza for your binding.

Field selection and group-by stanzas are used as follows:

```yaml
bindings:
  - source: acmeCo/example/collection
    resource: { table: example_table }

    # Modify the binding's 'fields' stanza
    fields:
      # Add a 'groupBy' array to define custom collection keys
      groupBy:
        - id
        - secondKey

      # Recommends fields for selection up to a specified depth (in this case, 2)
      recommended: 2

      # Require individual fields that may not otherwise be automatically selected
      require:
        _meta/field: {}
        deeplyNestedField: {}

      # Exclude individual fields that may be selected by default
      exclude:
        - sensitiveData
        - pii
```
