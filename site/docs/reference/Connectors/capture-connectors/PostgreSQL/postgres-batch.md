# PostgreSQL Batch Query Connector

This connector captures data from Postgres into Flow collections by periodically
executing queries and translating the results into JSON documents.

For local development or open-source workflows, [`ghcr.io/estuary/source-postgres-batch:dev`](https://ghcr.io/estuary/source-postgres-batch:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

We recommend using our [PostgreSQL CDC Connector](http://go.estuary.dev/source-postgres) instead
if possible. Using CDC provides lower latency data capture, delete and update events, and usually
has a smaller impact on the source database.

However there are some circumstances where this might not be feasible. Perhaps you need
to capture from a managed PostgreSQL instance which doesn't support logical replication.
Or perhaps you need to capture the contents of a view or the result of an ad-hoc query.
That's the sort of situation this connector is intended for.

The number one caveat you need to be aware of when using this connector is that **it will
periodically execute its update query over and over**. At the default polling interval of
5 minutes, a naive `SELECT * FROM foo` query against a 100 MiB view will produce 30 GiB/day
of ingested data, most of it duplicated.

This is why the connector's autodiscovery logic only returns ordinary tables of data, because
in that particular case we can use the `xmin` system column as a cursor and ask the database
to `SELECT xmin, * FROM foo WHERE xmin::text::bigint > $1;`.

If you start editing these queries or manually adding capture bindings for views or to run
ad-hoc queries, you need to either have some way of restricting the query to "just the new
rows since last time" or else have your polling interval set high enough that the data rate
`<DatasetSize> / <PollingInterval>` is an amount of data you're willing to deal with.