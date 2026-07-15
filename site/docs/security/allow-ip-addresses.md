---
description: Find Estuary data plane IP addresses to allowlist to connect with source and destination systems. Provides dashboard lookup steps and common public data plane IPs.
slug: /reference/allow-ip-addresses/
---

# Allowlisting IP Addresses for Estuary

When configuring systems that interact with Estuary, it's crucial to ensure that the necessary IP addresses are
allowlisted. This allows communication between Estuary and your data systems.
The IP addresses you need to allowlist depend on the data plane you use.

These fixed IPs can be used in conjunction with _connectors_.

Note that Estuary's storage does not use fixed IPs.
To limit IP access to collection storage, you will need to set up a [PrivateLink](/private-byoc/privatelink) connection instead.

## Data Plane IP Addresses

You can find the IP addresses relevant to your use case in the **Admin** section of your dashboard. To do so:

1. On the **[Settings](https://dashboard.estuary.dev/admin/settings)** page, find the **Data Planes** section.

2. Choose between the **Private** and **Public** tabs [based on your use case](../getting-started/deployment-options.md).

   Make sure to select the desired data plane when configuring a connector as well.

   If you wish to use a public data plane, Estuary offers several options across US, EU, and APAC regions with AWS and GCP.

3. Select your desired data plane from the table to open the **Details and Configuration modal**.

4. The **IPs** field provides a comma-separated list of associated IP addresses. You can choose between **v4** and **v6** options.
Click the clipboard to copy the addresses.

   Ensure that these IP addresses are allowlisted on both the source and destination systems that interact with Estuary.
