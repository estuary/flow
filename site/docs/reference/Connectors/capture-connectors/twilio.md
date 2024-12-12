# Twilio

This connector captures data from Twilio into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-twilio:dev`](https://ghcr.io/estuary/source-twilio:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Twilio APIs:

* [Accounts](https://www.twilio.com/docs/usage/api/account#read-multiple-account-resources)
* [Addresses](https://www.twilio.com/docs/usage/api/address#read-multiple-address-resources)
* [Alerts](https://www.twilio.com/docs/usage/monitor-alert#read-multiple-alert-resources)
* [Applications](https://www.twilio.com/docs/usage/api/applications#read-multiple-application-resources)
* [Available Phone Number Countries](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-resource#read-a-list-of-countries)
* [Available Phone Numbers Local](https://www.twilio.com/docs/phone-numbers/api/availablephonenumberlocal-resource#read-multiple-availablephonenumberlocal-resources)
* [Available Phone Numbers Mobile](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-mobile-resource#read-multiple-availablephonenumbermobile-resources)
* [Available Phone Numbers Toll Free](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-tollfree-resource#read-multiple-availablephonenumbertollfree-resources)
* [Calls](https://www.twilio.com/docs/voice/api/call-resource#create-a-call-resource)
* [Conference Participants](https://www.twilio.com/docs/voice/api/conference-participant-resource#read-multiple-participant-resources)
* [Conferences](https://www.twilio.com/docs/voice/api/conference-resource#read-multiple-conference-resources)
* [Conversations](https://www.twilio.com/docs/conversations/api/conversation-resource#read-multiple-conversation-resources)
* [Conversation Messages](https://www.twilio.com/docs/conversations/api/conversation-message-resource#list-all-conversation-messages)
* [Conversation Participants](https://www.twilio.com/docs/conversations/api/conversation-participant-resource)
* [Dependent Phone Numbers](https://www.twilio.com/docs/usage/api/address?code-sample=code-list-dependent-pns-subresources&code-language=curl&code-sdk-version=json#instance-subresources)
* [Executions](https://www.twilio.com/docs/phone-numbers/api/incomingphonenumber-resource#read-multiple-incomingphonenumber-resources)
* [Incoming Phone Numbers](https://www.twilio.com/docs/phone-numbers/api/incomingphonenumber-resource#read-multiple-incomingphonenumber-resources)
* [Flows](https://www.twilio.com/docs/studio/rest-api/flow#read-a-list-of-flows)
* [Keys](https://www.twilio.com/docs/usage/api/keys#read-a-key-resource)
* [Message Media](https://www.twilio.com/docs/sms/api/media-resource#read-multiple-media-resources)
* [Messages](https://www.twilio.com/docs/sms/api/message-resource#read-multiple-message-resources)
* [Outgoing Caller Ids](https://www.twilio.com/docs/voice/api/outgoing-caller-ids#outgoingcallerids-list-resource)
* [Queues](https://www.twilio.com/docs/voice/api/queue-resource#read-multiple-queue-resources)
* [Recordings](https://www.twilio.com/docs/voice/api/recording#read-multiple-recording-resources)
* [Transcriptions](https://www.twilio.com/docs/voice/api/recording-transcription?code-sample=code-read-list-all-transcriptions&code-language=curl&code-sdk-version=json#read-multiple-transcription-resources)
* [Usage Records](https://www.twilio.com/docs/usage/api/usage-record#read-multiple-usagerecord-resources)
* [Usage Triggers](https://www.twilio.com/docs/usage/api/usage-trigger#read-multiple-usagetrigger-resources)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* Twilio Auth Token for authentication.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Twilio source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/account_sid` | Account ID | Twilio account SID | string | Required |
| `/auth_token` | Auth Token | Twilio Auth Token. | string | Required |
| `/start_date` | Replication Start Date | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required |
| `/lookback_window` | Lookback window | How far into the past to look for records. (in minutes) | integer | Default |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Twilio project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-twilio:dev
        config:
          account_sid: <your account ID>
          auth_token: <secret>
          start_date: 2017-01-25T00:00:00Z
          lookback_window: 7
    bindings:
      - resource:
          stream: accounts
          syncMode: full_refresh
        target: ${PREFIX}/accounts
      {...}
```