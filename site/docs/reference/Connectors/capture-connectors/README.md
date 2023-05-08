# Capture connectors

Estuary's available capture connectors are listed in this section. Each connector has a unique set of requirements for configuration; these are linked below the connector name.

Also listed are links to the most recent Docker images for each connector. You'll need these to write Flow specifications manually (if you're [developing locally](../../../concepts/flowctl.md)). If you're using the Flow web app, they aren't necessary.

Estuary is actively developing new connectors, so check back regularly for the latest additions. We’re prioritizing the development of high-scale technological systems, as well as client needs.

## Available capture connectors

### Estuary connectors

These connectors are created by Estuary. We prioritize high-scale technology systems for development.

All Estuary connectors capture data in real time, as it appears in the source system

* AlloyDB
  * [Configuration](./alloydb.md)
  * Package - ghcr.io/estuary/source-alloydb:dev
* Alpaca
  * [Configuration](./alpaca.md)
  * Package - ghcr.io/estuary/source-alpaca:dev
* Amazon Kinesis
  * [Configuration](./amazon-kinesis.md)
  * Package — ghcr.io/estuary/source-kinesis:dev
* Amazon S3
  * [Configuration](./amazon-s3.md)
  * Package — ghcr.io/estuary/source-s3:dev
* Apache Kafka
  * [Configuration](./apache-kafka.md)
  * Package — ghcr.io/estuary/source-kafka:dev
* Google Cloud Storage
  * [Configuration](./gcs.md)
  * Package — ghcr.io/estuary/source-gcs:dev
* Google Firestore
  * [Configuration](./google-firestore.md)
  * Package - ghcr.io/estuary/source-firestore:dev
* HTTP file
  * [Configuration](./http-file.md)
  * Package - ghcr.io/estuary/source-http-file:dev
* HTTP ingest (webhook)
  * [Configuration](./http-ingest.md)
  * Package - ghcr.io/estuary/source-http-ingest:dev
* MariaDB
  * [Configuration](./mariadb.md)
  * Package - ghcr.io/estuary/source-mariadb:dev
* Microsoft SQL Server
  * [Configuration](./sqlserver.md)
  * Package - ghcr.io/estuary/source-sqlserver:dev
* MongoDB
  * [Configuration](./mongodb.md)
  * Package - ghcr.io/estuary/source-mongodb:dev
* MySQL
  * [Configuration](./MySQL.md)
  * Package - ghcr.io/estuary/source-mysql:dev
* PostgreSQL
  * [Configuration](./PostgreSQL.md)
  * Package — ghcr.io/estuary/source-postgres:dev
* Salesforce (for real-time data)
  * [Configuration](./salesforce-real-time.md)
  * Package - ghcr.io/estuary/source-salesforce-next:dev


### Third party connectors

Estuary supports open-source connectors from third parties. These connectors operate in a **batch** fashion,
capturing data in increments. When you run these connectors in Flow, you'll get as close to real time as possible
within the limitations set by the connector itself.

Typically, we enable SaaS connectors from third parties to allow more diverse data flows.

All the third-party connectors available currently were created by [Airbyte](https://airbyte.com/connectors).
The versions made available in Flow have been adapted for compatibility.

* Airtable
  * [Configuration](./airtable.md)
  * Package - ghrc.io/estuary/source-airtable.dev
* Amazon Ads
  * [Configuration](./amazon-ads.md)
  * Package - ghrc.io/estuary/source-amazon-ads.dev
* Amplitude
  * [Configuration](./amplitude.md)
  * Package - ghcr.io/estuary/source-amplitude:dev
* Bing Ads
  * [Configuration](./bing-ads.md)
  * Package - ghcr.io/estuary/source-bing-ads:dev
* Chargebee
  * [Configuration](./chargebee.md)
  * Package - ghrc.io/estuary/source-chargebee.dev
* Exchange Rates API
  * [Configuration](./exchange-rates.md)
  * Package - ghcr.io/estuary/source-exchange-rates:dev
* Facebook Marketing
  * [Configuration](./facebook-marketing.md)
  * Package - ghcr.io/estuary/source-facebook-marketing:dev
* Freshdesk
  * [Configuration](./freshdesk.md)
  * Package - ghcr.io/estuary/source-freshdesk:dev
* GitHub
  * [Configuration](./github.md)
  * Package - ghcr.io/estuary/source-github:dev
* Google Ads
  * [Configuration](./google-ads.md)
  * Package - ghcr.io/estuary/source-google-ads:dev
* Google Analytics 4
  * [Configuration](./google-analytics-4.md)
  * Package - ghcr.io/estuary/source-google-analytics-data-api:dev
* Google Universal Analytics
  * [Configuration](./google-analytics.md)
  * Package - ghcr.io/estuary/source-google-analytics-ua:dev
* Google Search Console
  * [Configuration](./google-search-console.md)
  * Package - ghcr.io/estuary/source-google-search-console:dev
* Google Sheets
  * [Configuration](./google-sheets.md)
  * Package - ghcr.io/estuary/source-google-sheets:dev
* Greenhouse
  * [Configuration](./greenhouse.md)
  * Package - ghrc.io/estuary/source-greenhouse.dev
* Hubspot
  * [Configuration](./hubspot.md)
  * Package - ghcr.io/estuary/source-hubspot:dev
* Intercom
  * [Configuration](./intercom.md)
  * Package - ghcr.io/estuary/source-intercom:dev
* Iterable
  * [Configuration](./iterable.md)
  * Package - ghrc.io/estuary/source-iterable.dev
* Klaviyo
  * [Configuration](./klaviyo.md)
  * Package - ghrc.io/estuary/source-klaviyo.dev
* LinkedIn Ads
  * [Configuration](./linkedin-ads.md)
  * Package - ghcr.io/estuary/source-linkedin-ads:dev
* Mailchimp
  * [Configuration](./mailchimp.md)
  * Package - ghcr.io/estuary/source-mailchimp:dev
* Marketo
  * [Configuration](./marketo.md)
  * Package - ghrc.io/estuary/source-marketo.dev
* MixPanel
  * [Configuration](./mixpanel.md)
  * Package - ghrc.io/estuary/source-mixpanel.dev
* Notion
  * [Configuration](./notion.md)
  * Package - ghcr.io/estuary/source-notion:dev
* Paypal Transaction
  * [Configuration](./paypal-transaction.md)
  * Package - ghrc.io/estuary/source-paypal-transaction.dev
* Recharge
  * [Configuration](./recharge.md)
  * Package - ghcr.io/estuary/source-recharge:dev
* Salesforce (For historical data)
  * [Configuration](./salesforce.md)
  * Package - ghcr.io/estuary/source-salesforce:dev
* SendGrid
  * [Configuration](./sendgrid.md)
  * Package - ghcr.io/estuary/source-sendgrid:dev
* Sentry
  * [Configuration](./sentry.md)
  * Package - ghcr.io/estuary/source-sentry:dev
* Slack
  * [Configuration](./slack.md)
  * Package - ghcr.io/estuary/source-slack:dev
* Snapchat
  * [Configuration](./snapchat.md)
  * Package - ghcr.io/estuary/source-snapchat:dev
* Stripe
  * [Configuration](./stripe.md)
  * Package - ghcr.io/estuary/source-stripe:dev
* SurveyMonkey
  * [Configuration](./survey-monkey.md)
  * Package - ghcr.io/estuary/source-surveymonkey:dev
* Twilio
  * [Configuration](./twilio.md)
  * Package - ghcr.io/estuary/source-twilio:dev
* Zendesk Chat
  * [Configuration](./zendesk-chat.md)
  * Package - ghcr.io/estuary/source-zendesk-chat:dev
* Zendesk Support
  * [Configuration](./zendesk-support.md)
  * Package - ghcr.io/estuary/source-zendesk-support:dev
