# Ashby

This connector captures data from Ashby into Estuary collections.

This connector is available for use in the Estuary web application.
For local development or open-source workflows, [`ghcr.io/estuary/source-ashby:v1`](https://ghcr.io/estuary/source-ashby:v1) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The connector captures the following Ashby resources, all using **incremental sync** via Ashby's [sync token mechanism](https://developers.ashbyhq.com/docs/pagination-and-incremental-sync#incremental-sync):

- [Applications](https://developers.ashbyhq.com/reference/applicationlist)
- [Approvals](https://developers.ashbyhq.com/reference/approvallist)
- [Archive Reasons](https://developers.ashbyhq.com/reference/archivereasonlist)
- [Candidate Tags](https://developers.ashbyhq.com/reference/candidatetaglist)
- [Candidates](https://developers.ashbyhq.com/reference/candidatelist)
- [Custom Fields](https://developers.ashbyhq.com/reference/customfieldlist)
- [Departments](https://developers.ashbyhq.com/reference/departmentlist)
- [Feedback Form Definitions](https://developers.ashbyhq.com/reference/feedbackformdefinitionlist)
- [Interview Events](https://developers.ashbyhq.com/reference/intervieweventlist)
- [Interview Plans](https://developers.ashbyhq.com/reference/interviewplanlist)
- [Interview Schedules](https://developers.ashbyhq.com/reference/interviewschedulelist)
- [Interview Stages](https://developers.ashbyhq.com/reference/interviewstagelist)
- [Interviewer Pools](https://developers.ashbyhq.com/reference/interviewerpoollist)
- [Interviews](https://developers.ashbyhq.com/reference/interviewlist)
- [Job Postings](https://developers.ashbyhq.com/reference/jobpostinglist)
- [Job Templates](https://developers.ashbyhq.com/reference/jobtemplatelist)
- [Jobs](https://developers.ashbyhq.com/reference/joblist)
- [Locations](https://developers.ashbyhq.com/reference/locationlist)
- [Offers](https://developers.ashbyhq.com/reference/offerlist)
- [Openings](https://developers.ashbyhq.com/reference/openinglist)
- [Projects](https://developers.ashbyhq.com/reference/projectlist)
- [Sources](https://developers.ashbyhq.com/reference/sourcelist)
- [Survey Form Definitions](https://developers.ashbyhq.com/reference/surveyformdefinitionlist)
- [Users](https://developers.ashbyhq.com/reference/userlist)

## Prerequisites

- An Ashby account with API access.

- An [Ashby API Key](https://developers.ashbyhq.com/reference/authentication) with the appropriate scopes for the resources you want to capture:

  | Scope                        | Resources                                                                                                                      |
  | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
  | `candidates:read`            | Applications, Candidates, Projects                                                                                             |
  | `approvals:read`             | Approvals                                                                                                                      |
  | `interviews:read`            | Interview Events, Interview Plans, Interview Schedules, Interview Stages, Interviews                                           |
  | `hiringProcessMetadata:read` | Archive Reasons, Candidate Tags, Custom Fields, Feedback Form Definitions, Interviewer Pools, Sources, Survey Form Definitions |
  | `organization:read`          | Departments, Locations, Users                                                                                                  |
  | `jobs:read`                  | Job Postings, Job Templates, Jobs, Openings                                                                                    |
  | `offers:read`                | Offers                                                                                                                         |

:::info
Resources that your API key doesn't have the required scopes to access are automatically omitted during discovery.
:::

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Ashby source connector.

### Properties

#### Endpoint

| Property                        | Title          | Description                    | Type   | Required/Default |
| ------------------------------- | -------------- | ------------------------------ | ------ | ---------------- |
| **`/credentials`**              | Authentication | API Key credentials for Ashby. | object | Required         |
| **`/credentials/access_token`** | Access Token   | Ashby API Key.                 | string | Required         |

#### Bindings

| Property    | Title    | Description                  | Type   | Required/Default |
| ----------- | -------- | ---------------------------- | ------ | ---------------- |
| **`/name`** | Name     | Name of the Ashby resource.  | string | Required         |
| `/interval` | Interval | Interval between data syncs. | string | PT5M             |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-ashby:v1
        config:
          credentials:
            access_token: <secret>
    bindings:
      - resource:
          name: applications
          interval: PT5M
        target: ${PREFIX}/applications
      - resource:
          name: candidates
          interval: PT5M
        target: ${PREFIX}/candidates
      - resource:
          name: jobs
          interval: PT5M
        target: ${PREFIX}/jobs
      {...}
```
