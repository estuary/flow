# Flow ops collections

This is intended as a quick guide for developers on the ops collections.

Flow collects logs and statistics for each task in the catalog. Logs are written
to collections named like `ops/<tenant>/logs`, where `<tenant>` is the tenant
name of the task. So a capture called `acmeCo/my/capture` would have its logs
published to the collection `ops/acmeCo/logs`, and statistics written to a
collection named `ops/<tenant>/stats`. Together, these are the ops collections,
and they are the means by which users can observe their running tasks.

The ops collections are primarily designed for the _users_ of Flow, not the
operators of the Flow infrastructure. We may use the ops collections to help
assess the health of our service, but the primary beneficiary of is the user
who's responsible for each task. As operators of the service, we already have
access to logs and metrics via the usual instrumentation. But regular users have
only the ops collections. So the goal is to make these collections maximally
useful to them.

## Guidelines for Flow developers:

- The `logrus` package should still be used for logging anything that isn't
  appropriate for users. Nothing you log there will be published to any
  collections.
- The `ops.LogHandler` is a handler of `ops.Logs`, and should be passed around
  to any code that might produce logs that are appropriate to show to the user.
  A handler might write into the appropriate ops collection, or print to local
  stderr depending on the current invocation context.

### Log levels:

- error: Non-recoverable errors, either from Flow or the connector or
  derivation. Things that a user definitely needs to know about.
- warn: Re-tryable errors, things that a user should probably look into.
- info: Task lifecycle events, or things that users might want to get from their
  logs on an on-going basis.
- debug: Things you'd want to know if you're trying to debug an issue with a
  task.
- trace: Things you'd want to know if you were desperate enough to sift through
  a gigabyte of logs.

Flow does not have any log levels more sever than `error` for several reasons:

- If it says "error", then users should already be looking into it ASAP, so it
  doesn't help to make it sound more scary.
- If it's really more severe than `error`, then it's probably something that the
  infrastructure operator should look into, not the user.
