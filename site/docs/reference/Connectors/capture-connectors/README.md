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
* MariaDB
  * [Configuration](./mariadb.md)
  * Package - ghcr.io/estuary/source-mariadb:dev
* MySQL
  * [Configuration](./MySQL.md)
  * Package - ghcr.io/estuary/source-mysql:dev
* PostgreSQL
  * [Configuration](./PostgreSQL.md)
  * Package — ghcr.io/estuary/source-postgres:dev


### Third party connectors

Estuary supports open-source connectors from third parties. These connectors operate in a **batch** fashion,
capturing data in increments. When you run these connectors in Flow, you'll get as close to real time as possible
within the limitations set by the connector itself.

Typically, we enable SaaS connectors from third parties to allow more diverse data flows.

All the third-party connectors available currently were created by [Airbyte](https://airbyte.com/connectors).
The versions made available in Flow have been adapted for compatibility.

* Amazon Ads
  * [Configuration](./amazon-ads.md)
  * Package - ghrc.io/estuary/source-amazon-ads.dev
* Amplitude
  * [Configuration](./amplitude.md)
  * Package - ghcr.io/estuary/source-amplitude:dev
* Bing Ads
  * [Configuration](./bing-ads.md)
  * Package - ghcr.io/estuary/source-bing-ads:dev
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
* Google Analytics
  * [Configuration](./google-analytics.md)
  * Package - ghcr.io/estuary/source-google-analytics-v4:dev
* Google Search Console
  * [Configuration](./google-search-console.md)
  * Package - ghcr.io/estuary/source-google-search-console:dev
* Google Sheets
  * [Configuration](./google-sheets.md)
  * Package - ghcr.io/estuary/source-google-sheets:dev
* Hubspot
  * [Configuration](./hubspot.md)
  * Package - ghcr.io/estuary/source-hubspot:dev
* Intercom
  * [Configuration](./intercom.md)
  * Package - ghcr.io/estuary/source-intercom:dev
* LinkedIn Ads
  * [Configuration](./linkedin-ads.md)
  * Package - ghcr.io/estuary/source-linkedin-ads:dev
* Mailchimp
  * [Configuration](./mailchimp.md)
  * Package - ghcr.io/estuary/source-mailchimp:dev
* Salesforce
  * [Configuration](./salesforce.md)
  * Package - ghcr.io/estuary/source-salesforce:dev
* Notion
  * [Configuration](./notion.md)
  * Package - ghcr.io/estuary/source-notion:dev
* Stripe
  * [Configuration](./stripe.md)
  * Package - ghcr.io/estuary/source-stripe:dev
* SurveyMonkey
  * [Configuration](./survey-monkey.md)
  * Package - ghcr.io/estuary/source-surveymonkey:dev
* Zendesk Support
  * [Configuration](./zendesk-support.md)
  * Package - ghcr.io/estuary/source-zendesk-support:dev
