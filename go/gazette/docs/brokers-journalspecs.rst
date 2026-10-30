JournalSpecs
=============

JournalSpecs declare and configure journals managed by a Gazette broker cluster.
Users generally work with JournalSpecs in YAML form using the ``gazctl`` tool,
in a very similar way to the management of Kubernetes resources using ``kubectl``.

Journals names form a flat key-space. While it's common practice to capture
some semantic hierarchy in the path components of journal names, it's important
to understand that these have no particular meaning to the broker cluster.

To make it easy to work with JournalSpecs in YAML form, however, the ``gazctl``
tool converts to and from a hierarchical representation for end-user
presentation. Intermediate nodes of the hierarchy are "directories"
(indicated by a trailing '/' in their name), and terminal nodes represent
journals (which never have a trailing '/').

Journal YAMLs are mapped to complete JournalSpecs by merging configuration
from parent to child. In other words, where a journal provides no value for a
configuration property it derives a value from its closest parent which specifies
a non-zero value.

When deriving new YAML to present a set of selected JournalSpecs, ``gazctl``
"hoists" property values shared by JournalSpecs to a representative parent
directory node which has a common prefix of those JournalSpecs.

This conversion happens entirely within the tool -- JournalSpecs sent to or
queried from brokers are full and complete individual specifications.

Example YAML
-------------

It's common for teams to version-control journal YAMLs which configure
journals owned by applications under the team's purview. For example the
Gazette repository versions YAML for journals used by example applications
of the repository:

.. literalinclude:: ../kustomize/test/bases/environment/examples.journalspace.yaml
   :language: yaml

Etcd Revisions
---------------

JournalSpecs retrieved by the ``gazctl`` tool will include their respective
Etcd modification revisions as field ``revision`` within the rendered YAML.

When applying YAML specs via ``gazctl journals apply``
explicitly specified ``revision`` can only exist on the leaf-level journals
of the spec. Any specs which omit ``revision`` assume a value of zero
(implying the journal must not exist).  The revisions of specs are always compared
to the current Etcd store revision,
and an apply will fail if there's a mismatch. This prevents a ``gazctl journals edit``
or list => modify => apply sequence from overwriting specification changes which
may have been made in the meantime.

.. code-block:: yaml
    :emphasize-lines: 3

    name: examples/foobar
    replication: 3
    revision: 6
    fragment:
        ... etc ...

A applied revision value ``-1`` can be used to explicitly signal that the Etcd
store revision should be ignored. This is helpful when the desired source-of-truth
for a set of JournalSpecs is versioned source control, and where an apply
of those specs should always overwrite any existing Etcd versions.

Deleting JournalSpecs
----------------------

One or more JournalSpecs may be deleted by adding a ``delete: true`` stanza to the
YAML returned by ``gazctl`` and then applying it -- for example, as part of a
``gazctl journals edit`` workflow. A ``delete`` stanza set on a parent node also
applies to all children.

.. code-block:: yaml
    :emphasize-lines: 2

    name: examples/foobar
    delete: true
    revision: 6
    replication: 3
    fragment:
        ... etc ...

Once applied, brokers will immediately stop serving the journal. Note that existing
journal fragments are not impacted and must be manually deleted.
