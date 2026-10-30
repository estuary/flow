Build and Test 
===============

Most binaries and packages are "pure" Go and can be directly go-installed and go-tested:

.. code-block:: console

   $ go install go.gazette.dev/core/cmd/gazette
   $ go install go.gazette.dev/core/cmd/gazctl
   $ go test go.gazette.dev/core/broker/...
   $ go test go.gazette.dev/core/consumer

Certain packages used by consumer applications, like ``go.gazette.dev/core/consumer/store-rocksdb``,
require CGO to build and also require appropriate development libraries for RocksDB.
On Debian and Ubuntu, the ``librocksdb-dev`` and ``libsqlite3-dev`` packages are sufficient,
paired with the ``libsqlite3`` Go build tag.


Continuous Integration
-----------------------

Gazette uses a :githubsource:`Make-based build system<Makefile>` which pulls down and stages
development dependencies into a ``.build`` sub-directory of the repository root.

Run CI tests as:

.. code-block:: console

   $ make go-test-ci

Continuous integration builds of Gazette run tests 15 times, with race detection enabled.

A ``go-test-fast`` target is also available, wich runs tests once without race detection.

Build release binaries for Gazette broker and examples using
``go-build`` or ``go-build-arm64`` targets.


Building the Docs
------------------

To build these docs locally, you'll need a few more dependencies. To start with, you'll need
``python`` and ``pip``. Note that on some systems, these may be called ``python3`` and ``pip3``.
Next you'll need to install the following python packages using ``pip install --user <package>``.

* ``sphinx``
* ``sphinxcontrib-programoutput``
* ``sphinx_rtd_theme``

Once you have all those installed, you can change directory into ``docs/`` and run ``make html``.
This will write the output to ``docs/_build``, and then you can open any of the html files in your
browser.

