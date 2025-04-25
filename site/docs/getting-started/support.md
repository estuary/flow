
# Support

There are multiple ways to receive support if you run into issues or have questions while working with Estuary Flow.

:::tip
Be aware of downtime affecting Estuary components by subscribing to [status updates](https://status.estuary.dev/).
:::

## How to reach us

* Use [Slack](https://go.estuary.dev/slack) for general questions about the product.

* [Email us](mailto:support@estuary.dev) if your question or issue includes information specific to your company.

* You can also [report issues](https://github.com/estuary/flow/issues) directly in our GitHub repo.

## Grant data sharing access for troubleshooting

During certain troubleshooting sessions, it can be helpful for a member of the Estuary team to have access to your tenant data. An Estuary team member may request an access grant to help you manage your pipelines at these times.

:::warning
Estuary will _only_ ever request access for the `estuary_support/` prefix.
:::

You can temporarily grant Estuary access using the following steps:

1. Go to the [Admin page](https://dashboard.estuary.dev/admin/) in your Estuary dashboard.

2. Under the **Account Access** tab, find the **Data Sharing** table.

3. Select **Grant Access** and fill out the following details in the **Share Data** modal:

    * **Shared Prefix:** your tenant prefix
    * **Shared With:** `estuary_support/`
    * **Capability:** may depend on the troubleshooting session, but typically `admin`

4. Click **Grant Access** to confirm.

You should then see `estuary_support/` in the list of entities that you're sharing data with.

When your issue is resolved, you can revoke access again by selecting the `estuary_support/` row and clicking **Remove**.

Learn more about [access grants](../concepts/web-app.md#account-access).
