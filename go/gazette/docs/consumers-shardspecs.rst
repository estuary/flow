ShardSpecs
============

ShardSpecs declare and configure shards managed by a Gazette consumer application.
Users generally work with ShardSpecs in YAML form using the ``gazctl`` tool,
in a analogous way to how Kubernetes resources are managed with ``kubectl``.

A collection of shards often share configuration. To make them easier
to work with, the ``gazctl`` tool converts to and from a hoisted ``common``
YAML map for end-user presentation. Each shard having no specific value for a
configuration property inherits from the ``common`` map.

This conversion happens entirely within the tool -- ShardSpecs sent to or
queried from consumer member processes are full and complete individual
specifications.

A common operational pattern is to define ShardSpecs that maintain a
one-to-one correspondence with journal partitions of a topic, as is done with
the below example. However ShardSpecs are highly configurable and a variety
of other patterns are also possible. Labels can be used to attach metadata to
ShardSpecs, and applications can interpret labels of the spec to drive customized
processing behaviors.

Example YAML
--------------

.. literalinclude:: ../kustomize/bases/example-word-count/shard_specs.yaml
   :language: yaml
