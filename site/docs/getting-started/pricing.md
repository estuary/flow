
# Estuary Pricing

Estuary provides simple, intuitive pricing for data movement.

In general, costs can be divided between three main categories:
[**data volume**](#data-volume-pricing), [**connector** or **task hour** costs](#task-hour-pricing), and [**private or BYOC** deployment fees](#privatebyoc-pricing).

:::note
All prices in this document are listed in USD.
:::

## Data Volume Pricing

Data movement into and out of Estuary is priced at $0.50 / GB.

You can track data volume in the Estuary dashboard or with the [OpenMetrics API](/reference/openmetrics-api).

For materializations, volume is based on the amount of data Estuary reads **in**, and thus has to process.
Because of Estuary's [reduction capabilities](/concepts/schemas/#reductions), the final amount of data that you see land in your destination may be lower.

If you expect to process high data volumes, you can receive discounts when you opt for [an annual plan](#pricing-cadence).

## Task Hour Pricing

An account's first 6 connectors are priced at $100 / month while additional connectors cost $50 / month.

These costs are broken down by hour, so that if you discontinue using a connector at the beginning of a month or start up a new task near the end, you do not pay full price for that connector.

With 720 hours to an average month, connector costs are therefore converted into **task hours** at $0.14 / hour.
Beyond the 6-connector (or 4464-task hour) mark, additional task hours drop to the reduced price, at $0.07 / hour.

You may therefore see two separate **task usage** lines on your bill, totaling up task hours across different connectors.

![Example Monthly Bill](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//estuary_example_bill_2783bfd4fd/estuary_example_bill_2783bfd4fd.png)

## Private/BYOC Pricing

Private and BYOC deployments require [an annual plan](#pricing-cadence) and incur additional fees based on the cloud infrastructure you require.
As these costs are tightly tied to your chosen cloud provider and region, along with the infrastructure and dedicated management for your solution, prices can vary.

You can [contact sales](https://estuary.dev/contact-us) for a plan estimate tailored to your use case.

## Pricing Cadence

Users can choose between a discounted annual contract or a flexible pay-as-you-go (PAYG) plan.

**PAYG** customers receive a monthly bill based on data volume and task hour usage.
These users can add a payment method (credit card or U.S. bank account) on the [billing page](https://dashboard.estuary.dev/admin/billing) in the dashboard.
The saved payment method will be automatically charged at the end of each billing cycle.

**Annual** plans offer a negotiated price based on data volume commitments and any extra required cloud infrastructure, like for private deployments.

## Estuary for Free

Estuary includes several options for free data movement:

* **Free plan:**
   Use Estuary for free when working with one low-volume data pipeline.
   You can remain on the free plan as long as you like within these limitations:

   * Up to two connectors
   * Up to 10 GB data volume per month

   Exceeding these limits will automatically move you into a free trial.

* **Free trial:**
   Beyond free plan limits, you can trial Estuary using a public deployment for free for 30 days.

   If you are interested in trialing a private or BYOC deployment for a proof of concept, [contact us](https://estuary.dev/contact-us).

* **Self hosting:**
   The Estuary runtime has been made available with specific usage stipulations under the [Business Source License](https://github.com/estuary/flow/blob/master/LICENSE-BSL).
   Setup and additional self-hosting support are currently not guaranteed and we recommend the hosted version of Estuary for the best experience.
