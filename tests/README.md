# Tests

End-to-end tests that exercise Flow through its real binaries and connectors,
rather than in-crate unit tests (which live beside the code they cover).

* [`preview/`](preview/README.md): Python snapshot tests of connector sessions
  run through `flowctl raw preview-next`. Fast, and needs only Docker.
* [`soak/`](soak/README.md): a long-running catalog of captures, derivations,
  and materializations, published to a data plane to shake out correctness and
  stability bugs under sustained load.
