.. _products-csv-ingestion:

Products CSV Ingestion
======================

We'll walk through how to populate our "products" Collection from a CSV file. Here's the schema
for a product:

.. literalinclude:: product.schema.yaml
    :caption: product.schema.yaml
    :language: yaml

We want to ingest a CSV with all our products from the old system. This works by sending the CSV
over a websocket to flow-ingester, which will convert each CSV row to a JSON document and add it
to our products Collection. Here's a sample of our CSV data:

.. literalinclude:: products.csv
    :caption: products.csv
    :lines: 1-3

The ``price`` and ``name`` columns match the properties in our schema exactly, so it's obvious how those will end up in the final JSON document. But we'll need to
tell Flow that the ``product_num`` column should be mapped to the ``id`` field. We do this by
adding a :ref:`projection <concepts-projections>` to our products Collection.

.. literalinclude:: products.flow.yaml
    :language: yaml
    :lines: 2-
    :emphasize-lines: 6

With this projection, we'll be able to simply send the CSV to flow-ingester over a websocket:

.. literalinclude:: add-products.sh
    :language: sh
    :lines: 5

We'll see the usual JSON response from flow-ingester. For larger CSVs, we may see many such responses as flow-ingester will break it down into multiple smaller transactions.

Next
----

* Continue the example with the :ref:`shopping cart implementation <shopping-carts>`.
* Learn about ingestion details in the :ref:`flow-ingester reference <flow-ingester-reference>`.
* Learn about projection details in the :ref:`projections documentation <concepts-projections>`.
