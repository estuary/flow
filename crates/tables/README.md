# Why are "utils" in the "tables" crate?

We already have a lot of shared functionality inside the trable crate. So this was the easiest place to put this for right now. Eventually, we should probably look at a single place to house all shared functionality. We also have "agent" that houses a lot of shared code. However, that requires more dependencies to be running on a system to get a full build.

## Authorization scopes

`UserGrant::reachable_nodes` in `src/behaviors.rs` evaluates a subject's grants
and optional capability mask. A `prefix_scope` list restricts that traversal to
the union of each scope prefix and its reachable role-grant nodes. Each user/scope
path intersection retains only their shared capability bits. `None` leaves the
user's grants unrestricted; an empty list yields no nodes, including for legacy
authorization. `reachable_prefixes` unions capabilities at matching prefixes.
