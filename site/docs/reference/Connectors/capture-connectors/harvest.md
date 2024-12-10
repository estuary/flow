
# Harvest

This connector captures data from Harvest into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-harvest:dev`](https://ghcr.io/estuary/source-harvest:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported through the Harvest APIs:

* [Client Contacts](https://help.getharvest.com/api-v2/clients-api/clients/contacts/)
* [Clients](https://help.getharvest.com/api-v2/clients-api/clients/clients/)
* [Company](https://help.getharvest.com/api-v2/company-api/company/company/)
* [Invoice Messages](https://help.getharvest.com/api-v2/invoices-api/invoices/invoice-messages/)
* [Invoice Payments](https://help.getharvest.com/api-v2/invoices-api/invoices/invoice-payments/)
* [Invoices](https://help.getharvest.com/api-v2/invoices-api/invoices/invoices/)
* [Invoice Item Categories](https://help.getharvest.com/api-v2/invoices-api/invoices/invoice-item-categories/)
* [Estimate Messages](https://help.getharvest.com/api-v2/estimates-api/estimates/estimate-messages/)
* [Estimates](https://help.getharvest.com/api-v2/estimates-api/estimates/estimates/)
* [Estimate Item Categories](https://help.getharvest.com/api-v2/estimates-api/estimates/estimate-item-categories/)
* [Expenses](https://help.getharvest.com/api-v2/expenses-api/expenses/expenses/)
* [Expense Categories](https://help.getharvest.com/api-v2/expenses-api/expenses/expense-categories/)
* [Tasks](https://help.getharvest.com/api-v2/tasks-api/tasks/tasks/)
* [Time Entries](https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/)
* [Project User Assignments](https://help.getharvest.com/api-v2/projects-api/projects/user-assignments/)
* [Project Task Assignments](https://help.getharvest.com/api-v2/projects-api/projects/task-assignments/)
* [Projects](https://help.getharvest.com/api-v2/projects-api/projects/projects/)
* [Roles](https://help.getharvest.com/api-v2/roles-api/roles/roles/)
* [User Billable Rates](https://help.getharvest.com/api-v2/users-api/users/billable-rates/)
* [User Cost Rates](https://help.getharvest.com/api-v2/users-api/users/cost-rates/)
* [User Project Assignments](https://help.getharvest.com/api-v2/users-api/users/project-assignments/)
* [Expense Reports](https://help.getharvest.com/api-v2/reports-api/reports/expense-reports/)
* [Uninvoiced Report](https://help.getharvest.com/api-v2/reports-api/reports/uninvoiced-report/)
* [Time Reports](https://help.getharvest.com/api-v2/reports-api/reports/time-reports/)
* [Project Budget Report](https://help.getharvest.com/api-v2/reports-api/reports/project-budget-report/)
By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* To set up the Harvest source connector, you'll need the [Harvest Account ID and API key](https://help.getharvest.com/api-v2/authentication-api/authentication/authentication/).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Harvest source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/account_id` | Account ID | Harvest account ID. Required for all Harvest requests in pair with Personal Access Token. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required |
| `/end_date` | End Date | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Default |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Harvest project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-harvest:dev
        config:
          account_id: <account id>
          start_date: 2017-01-25T00:00:00Z
          end_date: 2020-01-25T00:00:00Z
    bindings:
      - resource:
          stream: clients
          syncMode: incremental
        target: ${PREFIX}/clients
      {...}
```
