An Introduction to Flow
=======================

If you're a brand new Flow user, you're in the right place! We're going to walk
through the basics of Flow by building a shopping cart backend.

Your First collection
~~~~~~~~~~~~~~~~~~~~~

To start with, we're going to define a Flow collection that holds data about each user. We'll
have this collection accept user JSON documents via the REST API, and we'll materialize the data
in a Postgres table to make it available to our marketing team. Our devcontainer comes with a
Postgres instance that's started automatically, so all of this should "just work" in that
environment.

Flow collections are declared in a YAML file, like so:

.. literalinclude:: users.flow.yaml
    :language: yaml
    :lines: 1-4

Note that the schema is defined in a separate file. This is a common pattern because it allows
your schemas to be reused and composed. The actual schema is defined as:

.. literalinclude:: user.schema.yaml
    :caption: user.schema.yaml
    :language: yaml

We can apply our collection to a local Flow instance by running:

.. code-block:: console

    $ flowctl build && flowctl develop

Now that it's applied, we'll leave that terminal running and open a new one to simulate some
users being added.

.. literalinclude:: add-users.sh
    :language: sh
    :lines: 3-

This will print out some JSON with information about the writing of the new data, which we'll
come back to later. Let's check out our data in Postgres:

.. code-block::

    $ psql 'postgresql://flow:flow@localhost:5432/flow?sslmode=disable' -c "select id, email, name from shopping_users;"
    id |         email         |      name 
    ----+-----------------------+----------------
    6 | bigguy@dk.com         | Donkey Kong
    7 | explorer@ocean.net    | Echo
    8 | freeman@apeture.com | Gordon Freeman
    (3 rows)

As new users are added to the collection, they will continue to appear here. One of our users
wants to update their email address, though. This is done by ingesting a new document with
the same ``id``.

.. literalinclude:: update-user.sh
    :language: sh
    :lines: 3-

If we re-run the Postgres query, we'll see that the row for Gordon Freeman has been updated.
Since we declared the collection key of ``[ /id ]``, Flow will automatically combine the new
document with the previous version. In this case, the most recent document for each ``id`` will
be materialized. But Flow allows you to control how these documents are combined using
:ref:`reduction annotations <concepts-reductions>`, so you have control over how this works for
each collection. The users collection is simply using the default reduction strategy
``lastWriteWins``.

Writing Tests
-------------

Before we go, let's add some tests that verify the reduction logic in our users collection. The
:ref:`tests section <concepts-tests-section>` allows us to ingest documents and verify the fully
reduced results automatically. Most examples from this point on will use tests instead of shell
scripts for ingesting documents and verifying expected results.

.. literalinclude:: users.flow.yaml
    :language: yaml
    :lines: 6-

Each test is a sequence of ``ingest`` and ``verify`` steps, which will be executed in the order
written. In this test, we are first ingesting documents for the users Jane and Jill. The second
``ingest`` step provides a new email address for Jane. The ``verify`` step includes both
documents, and will fail if any of the properties do not match. 

We can run the tests using:

.. code-block:: console

    $ flowctl build && flowctl test

Next Steps
~~~~~~~~~~

Now that our users collection is working end-to-end, Here's some good topics to check out next:

* Learn the basics of CSV ingestion by building the :ref:`Products collection <products-csv-ingestion>`
* Explore reduction annotations by building the :ref:`Shopping Cart collection <shopping-carts>`

.. toctree::
    :hidden:

    Ingesting Products from CSV <products-csv-ingestion>
    Shopping Cart Derived Collections <carts>
    Handling Purchases <purchases>
