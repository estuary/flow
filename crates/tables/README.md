# Why are "utils" in the "tables" crate?

We already have a lot of shared functionality inside the trable crate. So this was the easiest place to put this for right now. Eventually, we should probably look at a single place to house all shared functionality. We also have "agent" that houses a lot of shared code. However, that requires more dependencies to be running on a system to get a full build.
