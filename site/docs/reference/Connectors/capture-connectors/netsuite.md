---
sidebar_position: 5
---
# NetSuite

This connector captures data from NetSuite into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-netsuite:dev`](https://ghcr.io/estuary/source-netsuite:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

You can find their documentation [here](https://docs.airbyte.com/integrations/sources/netsuite/),
but keep in mind that the two versions may be significantly different.

## Supported data resources

Flow captures collections from any NetSuite object to which you grant access during [setup](#setup), including `Transactions`, `Reports`, `Lists`, and `Setup`.

## Setup

Step 1: Create NetSuite account
 - Create account on [Oracle NetSuite](https://www.netsuite.com/portal/home.shtml)
 - Confirm your Email
Step 2: Setup NetSuite account
 - Step 2.1: Obtain Realm info
     - Log back into your NetSuite account
     - Go to Setup -> Company -> Company Information
     - Copy your Account ID ("Realm"). It should look like 2345678 for a Production env. or 2345678_SB2 - for a Sandbox
- Step 2.2: Enable features
    - Navigate to "Setup" -> "Company" -> "Enable Features"
    - Click on the "SuiteCloud" tab
    - Scroll down to the "SuiteScript" section
    - Select the checkboxes labeled "CLIENT SUITESCRIPT" and "SERVER SUITESCRIPT"
    - Scroll to the "Manage Authentication" section
    - Select the checkbox labeled "TOKEN-BASED AUTHENTICATION"
    - Scroll down to "SuiteTalk (Web Services)"
    - Select the checkbox labeled "REST WEB SERVICES"
    - Save the changes
- Step 2.3: Create an "Integration" to obtain a Consumer Key and Consumer Secret
    - Navigate to "Setup" -> "Integration" -> "Manage Integrations" -> "New"
    - Fill the "Name" field (Ex. estuary-rest-integration)
    - Make sure the "State" option is enabled
    - Select the checkbox labeled "Token-Based Authentication" in the Authentication section
    - Save
    - Next, your Consumer Key and Consumer Secret will be shown once (copy them to the safe place)
 - Step 2.4: Setup Role
    - Go to Setup » Users/Roles » Manage Roles » New
    - Fill in the "Name" field (Ex. estuary-integration-role)
    - Scroll down to the "Permissions" tab
    - (IMPORTANT) Click "Transactions" and add all the dropdown entities with either full or view access level.
    - (IMPORTANT) Click "Reports" and add all the dropdown entities with either full or view access level.
    - (IMPORTANT) Click "Lists" and add all the dropdown entities with either full or view access level.
    - (IMPORTANT) Click "Setup" an add all the dropdown entities with either full or view access level.
    - Please edit these parameters again when you rename or customize any Object in Netsuite for the `estuary-integration-role` to reflect such changes.
 - Step 2.5: Setup User
     - Go to "Setup" -> "Users/Roles" -> "Manage Users"
     - In column "Name" click on the user’s name you want to give access to the estuary-integration-role
     - Click the Edit button under the user’s name
     - Scroll down to the "Access" tab at the bottom
     - Select from the dropdown list the "estuary-integration-role" role which you created in step 2.4
     - Save your changes
 - Step 2.6: Create "Access Token" for role
     - Go to "Setup" -> "Users/Roles" -> "Access Tokens" -> "New"
     - Select an "Application Name"
     - Under "User", select the user you assigned the estuary-integration-role in the step 2.4
     - Inside "Role" select the one you gave to the user in the step 2.5
     - Under "Token Name" you can give a descriptive name to the Token you are creating (Ex. estuary-rest-integration-token)
     - Save changes
     - Now, "Token ID" and "Token Secret" will be shown once (copy them to the safe place)
 - Step 2.7: Summary
     - You now have a properly configured account with the correct permissions and:
     - Realm (Account ID)
     - Consumer Key
     - Consumer Secret
     - Token ID
     - Token Secret


See the following [documentation](https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/bridgehead_N286284.html) from Oracle. Any objects to which you grant the estuary-role access will be captured.

## Prerequisites

* Oracle NetSuite [account](https://system.netsuite.com/pages/customerlogin.jsp?country=US)
* Allowed access to all Account permissions options
* A new integration with token-based authentication
* A custom role with access to objects you want to capture. See [setup](#setup).
* A new user assigned to the custom role
* Access token generated for the custom role

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the NetSuite source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/realm` | Realm | Netsuite realm e.g. 2344535, as for `production` or 2344535_SB1, as for the `sandbox` | string | Required |
| `/consumer_key` | Consumer Key | Consumer key associated with your integration. | string | Required |
| `/consumer_secret` | Consumer Secret | Consumer secret associated with your integration. | string | Required |
| `/token_key` | Token Key | Access token key | string | Required |
| `/token_secret` | Token Secret | Access token secret | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your NetSuite project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-netsuite:dev
        config:
          realm: <your account id>
          consumer_key: <key>
          consumer_secret: <secret>
          token_key: <key>
          token_secret: <secret>
    bindings:
      - resource:
          stream: items
          syncMode: full_refresh
        target: ${PREFIX}/items
      {...}
```
