.. _shopping-carts:

Shopping Carts
==============

At this point, we have Captured collections setup for users and products, and we're ready to
start building the shopping carts. We'll start by defining a Captured collection for cart
updates, which will hold a record of each time a user adds or modifies a product in their cart.
These cart updates will then be joined with the product information so that we'll have the price
of each item. Then we'll create a "carts" collection that rolls up all the joined updates into a
single document that includes all the items in a users cart, along with their prices.

Summing Item Quantities
-----------------------

Here's the collection and schema for the cart updates:

.. literalinclude:: cart-updates.flow.yaml
    :caption: cart-updates.flow.yaml
    :language: yaml

.. literalinclude:: cart-update.schema.yaml
    :caption: cart-update.schema.yaml
    :language: yaml
    :emphasize-lines: 20

Each cartUpdate document represents a request to add or subtract a quantity of a particular
product in a user's cart. If we get multiple updates for the same user and product, the
quantities will be summed. This is because of the ``reduce`` annotation in the above schema.


Joining Cart Updates and Products
---------------------------------

We'll define a new :ref:`derived collection <concepts-derivations>` that performs a streaming
inner join of ``cartUpdate`` to ``product`` documents.

.. literalinclude:: cart-updates-with-products.flow.yaml
    :language: yaml
    :lines: 6-

There's two main concepts being used here. The first is the :ref:`register <concepts-registers>`. We'll have a unique register value for each product id, since that's the value we're joining on. For each product id, the register value can either be a product document, or null, and the ``initial`` value is always ``null``.

Now let's look at the two :ref:`transforms <concepts-derivations>`, starting with ``products``. This
will read documents from the products collection, and update the register for each one. Note that
the default shuffle key is implicitly the key of the source collection, in this case the ``/id``
field of a ``product``. The return value of ``[source]`` is simply adding the source product to the
set of register values. We simply return the value(s) that we'd like to be saved in registers,
rather than calling some sort of "save" function. The key for each value must be included in the
document itself, and this is verified at build/compile time. We always return a single value here
because we're doing a 1-1 join.

Whenever a document is read from the ``cartUpdates`` collection, the ``cartUpdates`` transform will
read the current value of the register and publish a new document that includes both the
``cartUpdate`` event and the ``product`` it joined to. If the register value is not null, then it
means that the ``products`` update lambda has observed a product with this id, and we'll emit a new
``cartUpdatesWithProducts`` document. This is what makes it an inner join, since we only return a
document if the register is not null.

.. note:

    The ``key`` of the ``cartUpdatesWithProducts`` collection is a composite key of both the user
    id and product id. This allows us to store different quantities of each product for each
    user. In addition, we're doing a "point-in-time" join here (as opposed to a "reactive" join).
    This means that the price and product details are associated with the users cart when they
    are added, and are not updated later. Flow absolutely supports fully reactive joins, but
    we're trying to keep things simple for this example.


Rolling Up Carts
----------------

Now that we have the product information joined to each item, we're ready to aggregate all of the
joined documents into a single cart for each user. This is an excellent use case for the ``set``
reduction strategy. In this case, we're going to apply the reduction annotations to the register
schema, and leave the collection schema as ``lastWriteWins``. This means that the state will
accumulate in the register (one per ``userId``), and the collection documents will each reflect
the last known state.

Initially, all we'll need is a single transform:

.. literalinclude:: carts.flow.yaml
    :language: yaml
    :lines: 1-44
    :emphasize-lines: 18,36,43

In the update lambda, we're adding the combined update-product document to the ``cartItems``. The
use of the ``set`` reduction strategy means that the item we provided will be deeply merged with
the existing set. So if there's already a product with the same ``id`` in the set, then the
``sum`` reduction strategy will apply.

Let's take a look at a test case that demonstrates it working end to end. Here we're ingesting
some products followed by a series of cart updates. Then we verify the final cart.

.. literalinclude:: cart-tests.flow.yaml
    :caption: cart-tests.flow.yaml
    :language: yaml
    :lines: 1-53

Next
----

* :ref:`Handle purchases <shopping-purchases>` to learn how to reset the state of a cart.
* Check out the :ref:`Derivation Patterns <derive-patterns>` docs for more examples of how to manage stateful
  transformations.

