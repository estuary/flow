# Gainsight NXT

This connector captures data from [Gainsight NXT](https://www.gainsight.com/), a customer success platform. It uses Gainsight's REST API to capture data in real-time.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-gainsight-nxt:dev`](https://github.com/estuary/estuary/pkgs/container/source-gainsight-nxt) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past images.

## Supported Streams

The connector supports the following Gainsight NXT resources. Most streams use the `/v1/data/objects/query/{object_name}` endpoint with predetermined object names that were discovered using the [Get Lite API Call OMD](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Data_Management_APIs/Data_Management_APIs#Get_Lite_API_Call_OMD).

The exceptions are `activity_timelines` and `companies` streams, which use their own dedicated API endpoints as documented in their respective API documentation links.

| Stream                                                                                                                                                                                                 | Capture Type | Cursor           | API Endpoint                            |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------ | ---------------- | --------------------------------------- |
| [`da_picklist`](<https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Cockpit_API/Call_To_Action_(CTA)_API_Documentation#Method_4>)                                                      | full-refresh | N/A              | `/v1/data/objects/query/da_picklist`    |
| [`activity_timelines`](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Timeline_API/Timeline_APIs#Read_API)                                                                         | incremental  | LastModifiedDate |                                         |
| [`call_to_actions`](<https://support.gainsight.com/gainsight_nxt/04Cockpit_and_Playbooks/00Cockpit_Horizon_Experience/Cockpit_API_Documentation/Call_To_Action_(CTA)_API_Documentation#Fetch_CTA_API>) | incremental  | ModifiedDate     | `/v1/data/objects/query/call_to_action` |
| [`companies`](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Company_and_Relationship_API/Company_API_Documentation#Read_API)                                                      | incremental  | ModifiedDate     |                                         |
| [`cs_tasks`](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Cockpit_API/Task_APIs#Fetch_Task_List_API)                                                                             | incremental  | ModifiedDate     | `/v1/data/objects/query/cs_task`        |
| `success_plans`                                                                                                                                                                                        | incremental  | ModifiedDate     | `/v1/data/objects/query/cta_grooup`     |
| [`users`](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/User_Management_APIs/User_Management_APIs#Fetch_User_Details)                                                             | incremental  | ModifiedDate     | `/v1/data/objects/query/gsuser`         |

## Prerequisites

To set up the Gainsight source connector, you'll need:

- A Gainsight NXT [API Access Key](https://support.gainsight.com/gainsight_nxt/Connectors/Connectors_Version_-_Archived/03API_Integration/Generate_API_Access_Key)
- Your Gainsight domain

## Configuration

You can configure the connector either in the Flow web app or by directly editing the catalog specification file. See connectors to learn more about using [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors). The values and specification sample below provide configuration details specific to the Gainsight NXT source connector.

### Properties

#### Endpoint

| Property                             | Title             | Description                                                                                             | Type   | Required/Default                      |
| ------------------------------------ | ----------------- | ------------------------------------------------------------------------------------------------------- | ------ | ------------------------------------- |
| **`/credentials/access_token`**      | API Access Key    | Gainsight API access key for authentication.                                                            | string | Required                              |
| **`/credentials/credentials_title`** | Credentials Title | Name of the credentials set.                                                                            | string | Required, `"Private App Credentials"` |
| **`/domain`**                        | Domain            | The domain for your Gainsight instance.                                                                 | string | Required                              |
| **`/start_date`**                    | Start Date        | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required, `30 day ago`                |

#### Bindings

| Property    | Title         | Description                                                   | Type   | Required/Default |
| ----------- | ------------- | ------------------------------------------------------------- | ------ | ---------------- |
| **`/name`** | Resource Name | Name of the Gainsight resource to capture.                    | string | Required         |
| `/interval` | Sync Interval | Interval between data syncs (e.g., PT2M for every 2 minutes). | string |                  |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-gainsight-nxt:dev
        config:
          credentials:
            credentials_title: Private App Credentials
            api_token: <secret>
          domain: yourdomain
          start_date: "2024-01-01T00:00:00Z"
    bindings:
      - resource:
          name: companies
        target: ${PREFIX}/companies
      - resource:
          name: users
        target: ${PREFIX}/users
```
