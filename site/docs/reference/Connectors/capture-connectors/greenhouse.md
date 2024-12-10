
# Greenhouse

This connector captures data from Greenhouse into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-greenhouse:dev`](https://ghcr.io/estuary/source-greenhouse:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.


## Supported data resources

The following data resources are supported through the Greenhouse APIs:

* [Activity Feed](https://developers.greenhouse.io/harvest.html#get-retrieve-activity-feed)
* [Applications](https://developers.greenhouse.io/harvest.html#get-list-applications)
* [Applications Interviews](https://developers.greenhouse.io/harvest.html#get-list-scheduled-interviews-for-application)
* [Approvals](https://developers.greenhouse.io/harvest.html#get-list-approvals-for-job)
* [Candidates](https://developers.greenhouse.io/harvest.html#get-list-candidates)
* [Close Reasons](https://developers.greenhouse.io/harvest.html#get-list-close-reasons)
* [Custom Fields](https://developers.greenhouse.io/harvest.html#get-list-custom-fields)
* [Degrees](https://developers.greenhouse.io/harvest.html#get-list-degrees)
* [Departments](https://developers.greenhouse.io/harvest.html#get-list-departments)
* [Disciplines](https://developers.greenhouse.io/harvest.html#get-list-approvals-for-job)
* [EEOC](https://developers.greenhouse.io/harvest.html#get-list-eeoc)
* [Email Templates](https://developers.greenhouse.io/harvest.html#get-list-email-templates)
* [Interviews](https://developers.greenhouse.io/harvest.html#get-list-scheduled-interviews)
* [Job Posts](https://developers.greenhouse.io/harvest.html#get-list-job-posts)
* [Job Stages](https://developers.greenhouse.io/harvest.html#get-list-job-stages)
* [Jobs](https://developers.greenhouse.io/harvest.html#get-list-jobs)
* [Job Openings](https://developers.greenhouse.io/harvest.html#get-list-job-openings)
* [Jobs Stages](https://developers.greenhouse.io/harvest.html#get-list-job-stages-for-job)
* [Offers](https://developers.greenhouse.io/harvest.html#get-list-offers)
* [Offices](https://developers.greenhouse.io/harvest.html#get-list-offices)
* [Prospect Pools](https://developers.greenhouse.io/harvest.html#get-list-prospect-pools)
* [Rejection Reasons](https://developers.greenhouse.io/harvest.html#get-list-rejection-reasons)
* [Schools](https://developers.greenhouse.io/harvest.html#get-list-schools)
* [Scorecards](https://developers.greenhouse.io/harvest.html#get-list-scorecards)
* [Sources](https://developers.greenhouse.io/harvest.html#get-list-sources)
* [Tags](https://developers.greenhouse.io/harvest.html#get-list-candidate-tags)
* [Users](https://developers.greenhouse.io/harvest.html#get-list-users)
* [User Permissions](https://developers.greenhouse.io/harvest.html#get-list-job-permissions)
* [User Roles](https://developers.greenhouse.io/harvest.html#the-user-role-object)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* To set up the Greenhouse source connector, you'll need the [Harvest API key](https://developers.greenhouse.io/harvest.html#authentication) with permissions to the resources Estuary Flow should be able to access.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Greenhouse source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/apikey` | API Key | The value of the Greenhouse API Key generated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Greenhouse project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-greenhouse:dev
        config:
          apikey: <secret>
    bindings:
      - resource:
          stream: applications
          syncMode: full_refresh
        target: ${PREFIX}/applications
      {...}
```
