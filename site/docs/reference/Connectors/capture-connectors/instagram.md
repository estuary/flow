
# Instagram

This connector captures data from Instagram into Flow collections.

This [Instagram article](https://help.instagram.com/570895513091465) talks about how to connect a Facebook page to your Instagram business account.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-instagram:dev`](https://ghcr.io/estuary/source-instagram:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported through the Instagram APIs:

For more information, see the [Instagram Graph API](https://developers.facebook.com/docs/instagram-api/) and [Instagram Insights API documentation](https://developers.facebook.com/docs/instagram-api/guides/insights/).

* [User](https://developers.facebook.com/docs/instagram-api/reference/ig-user)
* [User Insights](https://developers.facebook.com/docs/instagram-api/reference/ig-user/insights)
* [Media](https://developers.facebook.com/docs/instagram-api/reference/ig-user/media)
* [Media Insights](https://developers.facebook.com/docs/instagram-api/reference/ig-media/insights)
* [Stories](https://developers.facebook.com/docs/instagram-api/reference/ig-user/stories/)
* [Story Insights](https://developers.facebook.com/docs/instagram-api/reference/ig-media/insights)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* [Meta for Developers account](https://developers.facebook.com)
* [Instagram business account](https://www.facebook.com/business/help/898752960195806) to your Facebook page
* [Instagram Graph API](https://developers.facebook.com/docs/instagram-api/) to your Facebook app
* [Facebook OAuth Reference](https://developers.facebook.com/docs/instagram-basic-display-api/reference)
* [Facebook ad account ID number](https://www.facebook.com/business/help/1492627900875762)

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Instagram source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/client_id` | Client ID | The Client ID of your Instagram developer application. | string | Required |
| `/client_secret` | Client Secret | The Client Secret of your Instagram developer application. | string | Required |
| `/access_token` | Access Token | The value of the access token generated with instagram_basic, instagram_manage_insights, pages_show_list, pages_read_engagement, Instagram Public Content Access permissions. | string | Required |
| `/start_date` | Replication Start Date | UTC date and time in the format YYYY-MM-DDT00:00:00Z. Any data before this date will not be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Instagram project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-instagram:dev
        config:
          client_id: <your client ID>
          client_secret: <secret>
          access_token: <secret>
          start_date: 2017-01-25T00:00:00Z
    bindings:
      - resource:
          stream: stories
          syncMode: full_refresh
        target: ${PREFIX}/stories
      {...}
```
