.. _shopping-purchases:

Handling Purchases
------------------

To put it all together, we'll create a captured collection for requests to purchase a cart, and a final derived collection to hold the complete purchase details. 

Here's the schema and Captured collection:

.. literalinclude:: cart-purchase-requests.flow.yaml
    :language: yaml

We'll read these purchase events in a couple of places. First, we'll create a purchases derivation that stores the most recent cart for each user in a register. When it reads a purchase event, it will publish the complete cart contents.

.. literalinclude:: purchases.flow.yaml
    :language: yaml

The timestamp is again part of the key in order to uniquely identify multiple purchases from the same user. If we were to materialize the purchases collection, we'd get a separate row for each purchase. We can see this work end to end in the following test case. 

.. literalinclude:: purchase-tests.flow.yaml
    :caption: purchase-tests.flow.yaml
    :language: yaml
    :lines: 1-53

The last thing we'll do is to reset the state of a user's cart after they complete a purchase. Here we'll leverage Flow's capability to have multiple readers of each collection, and add a ``clearAfterPurchase`` transform to our ``carts`` collection.

.. literalinclude:: cart.flow.yaml
    :language: yaml
    :lines: 46-
    :emphasize-lines: 55

Here we have both update and publish lambdas. The update lambda clears the set of items in the register by intersecting it with ``[]``, using the same ``set`` reduction strategy. The publish lambda ensures that other readers of the carts collection (and materializations) will observe the now empty cart. This behavior is required in order for the ``cart is cleared after purchase`` test case to pass:

.. literalinclude:: purchase-tests.flow.yaml
    :language: yaml
    :lines: 57-


You Made It!
------------

If you've made it this far, then you've seen all the major elements of the Flow programming model. Some recommended next steps are:

* :ref:`Try out Flow yourself <getting-started-vscode>` using our pre-built dev container.
* Read about :ref:`how flow compares to other systems <comparisons>`.
* Check out some more examples.
