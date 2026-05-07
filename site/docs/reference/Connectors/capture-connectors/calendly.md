# Calendly

This connector captures data from Calendly into Estuary collections.

This connector is available for use in the Estuary web application.
For local development or open-source workflows, [`ghcr.io/estuary/source-calendly:v1`](https://ghcr.io/estuary/source-calendly:v1) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The connector captures the following Calendly resources:

- [Event Invitees](https://developer.calendly.com/api-docs/eb8ee72701f99-list-event-invitees)
- [Event Types](https://developer.calendly.com/api-docs/25a4ece03c1bc-list-user-s-event-types)
- [Groups](https://developer.calendly.com/api-docs/6rb6dtdln74sy-list-groups) (Teams accounts only)
- [Organization Memberships](https://developer.calendly.com/api-docs/b3A6NTkxNDI0-list-organization-memberships)
- [Routing Form Submissions](https://developer.calendly.com/api-docs/17db5cb915a57-list-routing-form-submissions) (Teams accounts only)
- [Routing Forms](https://developer.calendly.com/api-docs/9fe7334bec6ad-list-routing-forms) (Teams accounts only)
- [Scheduled Events](https://developer.calendly.com/api-docs/b3A6NTkxNDEy-list-events)

:::info
Resources that require a Calendly Teams account are automatically omitted during discovery when the authenticated account is not on a Teams plan.
:::

## Prerequisites

- A [Personal Access Token](https://developer.calendly.com/how-to-authenticate-with-personal-access-tokens) generated from your Calendly account.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Calendly source connector.

### Properties

#### Endpoint

| Property                            | Title                              | Description                                                                                                                                                      | Type    | Required/Default |
| ----------------------------------- | ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ---------------- |
| **`/credentials`**                  | Authentication                     | Personal Access Token credentials for Calendly.                                                                                                                  | object  | Required         |
| **`/credentials/access_token`**     | Access Token                       | Calendly Personal Access Token.                                                                                                                                  | string  | Required         |
| `/start_date`                       | Start Date                         | UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Data added on and after this date will be captured. If left blank, defaults to 30 days before the present. | string  | 30 days ago      |
| `/scheduled_event_lookback_months`  | Scheduled Event Lookback (Months)  | Number of months before the current date to include when fetching scheduled events. Must be at least 1.                                                          | integer | `1`              |
| `/scheduled_event_lookahead_months` | Scheduled Event Lookahead (Months) | Number of months after the current date to include when fetching scheduled events. Must be at least 1.                                                           | integer | `6`              |

#### Bindings

| Property    | Title    | Description                    | Type   | Required/Default |
| ----------- | -------- | ------------------------------ | ------ | ---------------- |
| **`/name`** | Name     | Name of the Calendly resource. | string | Required         |
| `/interval` | Interval | Interval between data syncs.   | string | PT5M             |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-calendly:v1
        config:
          credentials:
            access_token: <secret>
          start_date: "2024-01-01T00:00:00Z"
    bindings:
      - resource:
          name: event_types
          interval: PT5M
        target: ${PREFIX}/event_types
      - resource:
          name: scheduled_events
          interval: PT5M
        target: ${PREFIX}/scheduled_events
      - resource:
          name: event_invitees
          interval: PT5M
        target: ${PREFIX}/event_invitees
      {...}
```
