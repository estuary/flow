# SingleStore Batch Query Connector

This connector captures data from SingleStore into Flow collections by periodically executing queries and translating
the results into JSON documents. It leverages SingleStore's MySQL wire compatibility to interact with the database.

**This connector periodically re-executes the query**. The default polling interval is set
to 24 hours to minimize this behavior's impact, but depending on table size, it may lead to duplicated data being
processed.

If the dataset has a natural cursor that can identify only new or updated rows, it should be specified by editing the
`Cursor` property of the binding. Common examples of suitable cursors include:

- Update timestamps, which are typically the best choice since they can capture all changed rows, not just new rows.
- Creation timestamps, which work for identifying newly added rows in append-only datasets but won’t capture updates.
- Serially increasing IDs, which can be used to track newly added rows.

## Setup

1. Ensure that [Estuary's IP addresses are allowlisted](/reference/allow-ip-addresses) to allow access. You can do by
   following [these steps](https://docs.singlestore.com/cloud/reference/management-api/#control-access-to-the-api)
2. Grab the following details from the SingleStore workspace.
    1. Workspace URL
    2. Username
    3. Password
    4. Database
3. Configure the Connector with the appropriate values. Make sure to specify the database name under the "Advanced"
   section.
