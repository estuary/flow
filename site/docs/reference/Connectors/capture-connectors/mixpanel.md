
# MixPanel

This connector captures data from MixPanel into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-mixpanel:dev`](https://ghcr.io/estuary/source-mixpanel:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the MixPanel APIs:

* [Export](https://developer.mixpanel.com/reference/raw-event-export)
* [Engage](https://developer.mixpanel.com/reference/engage-query)
* [Funnels](https://developer.mixpanel.com/reference/funnels-query)
* [Revenue](https://developer.mixpanel.com/reference/engage-query)
* [Annotations](https://developer.mixpanel.com/reference/overview-1)
* [Cohorts](https://developer.mixpanel.com/reference/cohorts-list)
* [Cohort Members](https://developer.mixpanel.com/reference/engage-query)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* To set up the Mixpanel source connector, you'll need a Mixpanel [Service Account](https://developer.mixpanel.com/reference/service-accounts) and it's [Project ID](https://help.mixpanel.com/hc/en-us/articles/115004490503-Project-Settings#project-id), the [Project Timezone](https://help.mixpanel.com/hc/en-us/articles/115004547203-Manage-Timezones-for-Projects-in-Mixpanel), and the Project region (`US` or `EU`).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the MixPanel source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/project_id` | Project ID | Your project ID number. See the [docs](https://help.mixpanel.com/hc/en-us/articles/115004490503-Project-Settings#project-id) for more information on how to obtain this. | integer | Required |
| `/attribution_window` | Attribution Window | A period of time for attributing results to ads and the lookback period after those actions occur during which ad results are counted. Default attribution window is 5 days. | integer | Default |
| `/project_timezone` | Project Timezone | Time zone in which integer date times are stored. The project timezone may be found in the project settings in the [Mixpanel console](https://help.mixpanel.com/hc/en-us/articles/115004547203-Manage-Timezones-for-Projects-in-Mixpanel) | string | Default |
| `/select_properties_by_default` | Select Properties By Default | boolean | Default |
| `/start_date` | Start Date | The date in the format YYYY-MM-DD. Any data before this date will not be replicated. If this option is not set, the connector will replicate data from up to one year ago by default. | string | Required |
| `/end_date` | End Date | The date in the format YYYY-MM-DD. Any data after this date will not be replicated. Left empty to always sync to most recent date. | string | Default |
| `/region` | Region | The region of mixpanel domain instance either US or EU. | string | Default |
| `/date_window_size` | Date slicing window | Defines window size in days, that used to slice through data. You can reduce it, if amount of data in each window is too big for your environment. | integer | Default |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your MixPanel project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-mixpanel:dev
        config:
          credentials:
            auth_type: ServiceAccount
            secret: {secret}
            username: {your_username}
          project_id: 1234567
          attribution_window: 5
          project_timezone: US/Pacific
          select_properties_by_default: true
          start_date: 2017-01-25T00:00:00Z
          end_date: 2019-01-25T00:00:00Z
          region: US
          date_window_size: 30
    bindings:
      - resource:
          stream: annotations
          syncMode: full_refresh
        target: ${PREFIX}/annotations
      {...}
```
