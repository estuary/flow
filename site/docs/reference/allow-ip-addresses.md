# Allowlisting IP Addresses for Estuary Flow

When configuring systems that interact with Estuary Flow, it's crucial to ensure that the necessary IP addresses are
allowlisted. This allows communication between Estuary Flow and your data systems.
The IP addresses you need to allowlist depend on the data plane you use.

## Data Plane IP Addresses

You can find the IP addresses relevant to your use case in the **Admin** section of your dashboard. To do so:

1. On the **[Settings](https://dashboard.estuary.dev/admin/settings)** page, find the **Data Planes** section.

2. Choose between the **Private** and **Public** tabs [based on your use case](../getting-started/deployment-options.md).

   Make sure to select the desired data plane when configuring a connector as well.

   If you wish to use a public data plane, Estuary currently offers AWS `eu-west-1` and GCP `us-central1` options.

3. Find the **CIDR Blocks** column in the Data Planes table. This column includes a comma-separated list of IP addresses for that data plane.

   Ensure that these IP addresses are allowlisted on both the source and destination systems that interact with Estuary Flow.

## IP Addresses to Allowlist

While your dashboard is the best location to find accurate, up-to-date IP addresses to allowlist, you may also find the current public data plane IP addresses below.

**US** (`us-central1 c1`):

- **34.121.207.128**
- **35.226.75.135**
- **34.68.62.148**

**Europe** (`eu-west-1 c1`):

- **18.200.127.124/32**
- **34.247.94.19/32**
