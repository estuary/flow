# MySQL Batch Query Connector

This connector captures data from MySQL into Flow collections by periodically
executing queries and translating the results into JSON documents.

We recommend using our [MySQL CDC Connector](http://go.estuary.dev/source-mysql)
instead where possible. Using CDC provides lower latency data capture, delete and
update events, and usually has a smaller impact on the source database.

However there are some circumstances where this might not be feasible. Perhaps you need
to capture from a managed MySQL instance which doesn't support logical replication. Or
perhaps you need to capture the contents of a view or the result of an ad-hoc query.
That's the sort of situation this connector is intended for.

The number one caveat you need to be aware of when using this connector is that **it will
periodically execute its update query over and over**. The default polling interval is set
to 24 hours to minimize the impact of this behavior, but even then it could mean a lot of
duplicated data being processed depending on the size of your tables.

If the dataset has a natural cursor which could be used to identify only new or updated
rows, this should be specified by editing the `Cursor` property of the binding. Common
examples of suitable cursors include:

  - Update timestamps, which are usually the best choice if available since they
    can be used to identify all changed rows, not just updates.
  - Creation timestamps, which can be used to identify newly added rows in append-only
    datasets but can't be used to identify updates.
  - Serially increasing IDs can also be used to identify newly added rows.
