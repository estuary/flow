# Sage Intacct

This connector captures data from Sage Intacct into Flow collections.

It is available for use in the Flow web application. For local development or
open-source workflows,
[`ghcr.io/estuary/source-sage-intacct:dev`](https://ghcr.io/estuary/source-sage-intacct:dev)
provides the latest version of the connector as a Docker image. You can also
follow the link in your browser to see past image versions.

## Supported objects

The following objects support an incremental strategy for capturing new and
updated documents:
* CUSTOMER
* APTERM
* CLASS
* DEPARTMENT
* EMPLOYEE
* GLACCOUNT
* LOCATION
* TAXDETAIL
* VENDOR
* TRXCURRENCIES
* GLJOURNAL
* PROJECT
* ITEM
* TASK

These objects support capturing via periodic snapshotting:
* COMPANYPREF

## Prerequisites

To use this connector, you'll need:
* A [Sage Intacct Account](https://www.sage.com/en-us/sage-business-cloud/intacct/)
* An active Web Services developer license with sender ID and password provisioned

The Web Services subscription must be enabled, and the sender ID must be
authorized for your company to make API calls. Reference the [Sage Intacct Web
Services Doc](https://developer.intacct.com/web-services/) for more information.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the
catalog specification files. The values and specification sample below provide
configuration details specific to the Sage Intacct source connector.

### Properties

#### Endpoint

| Property               | Title           | Description                  | Type   | Required/Default |
|------------------------|-----------------|------------------------------|--------|------------------|
| **`/sender_id`**       | Sender ID       | Web Services Sender ID       | string | Required         |
| **`/sender_password`** | Sender Password | Web Services Sender Password | string | Required         |
| **`/company_id`**      | Company ID      | Sage Intacct Company ID      | string | Required         |
| **`/user_id`**         | User ID         | Sage Intacct User ID         | string | Required         |
| **`/password`**        | Password        | Sage Intacct Password        | string | Required         |

#### Bindings

| Property    | Title    | Description                 | Type   | Required/Default |
|-------------|----------|-----------------------------|--------|------------------|
| **`/name`** | Name     | Name of this resource       | string | Required         |
| `/interval` | Interval | Interval between data syncs | string | PT5M             |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-sage-intacct:dev
        config:
          sender_id: my_sender_id
          sender_password: secret_sender_password
          company_id: my_company
          user_id: my_user_id
          password: secret_user_password
    bindings:
      - resource:
          name: CUSTOMER
          interval: PT5M
        target: ${PREFIX}/CUSTOMER
      {...}
```
