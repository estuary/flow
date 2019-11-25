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

