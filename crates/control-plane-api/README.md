# Control plane API

This crate exposes functions for interacting with the control plane. It includes:

- The GraphQL API
- The REST API
- Rust functions, which typically require passing a `sqlx::PgPool` or `sqlx::Transaction`

The `agent` crate depends on this crate to run the api server, and the automations executors and controllers call rust functions defined here.

This factoring is a work in progress. It started out as the `agent-sql` crate, which had originally been intended just to isolate all our `sqlx::query!`s into one crate. But that isolation didn't seem to really help us much. We've since started running two separate types of agent processes, one that serves only the API and another that serves only the (background) `automations` executors. It now seems more natual to have one crate for each of those purposes, even though for now they are still built as a single binary.
