---
sidebar_position: 8
---
# Storage mappings

Flow stores the documents that comprise your collections in a cloud storage bucket.
Your **storage mapping** tells Flow which bucket to use.

When you first register for Flow, your storage mapping is Estuary's secure Google Cloud Storage bucket.
Data in Flow's cloud storage bucket is deleted 20 days after collection.

For production workflows, you should [set up your own cloud storage bucket as a storage mapping](../getting-started/installation.mdx).

You can set up a [bucket lifecycle policy](#bucket-lifecycle-policies) to manage data retention in your storage mapping;
for example, to remove data after six months.

## Recovery logs

In addition to collection data, Flow uses your storage mapping to temporarily store **recovery logs**.

Flow tasks — captures, derivations, and materializations — use recovery logs to durably store their processing context as a backup.
Recovery logs are an opaque binary log, but may contain user data.

The recovery logs of a task are always prefixed by `recovery/`,
so a task named `acmeCo/produce-TNT` would have a recovery log called `recovery/acmeCo/produce-TNT`

Flow prunes data from recovery logs once it is no longer required.

:::warning
Deleting data from recovery logs while it is still in use can
cause Flow processing tasks to fail permanently.
:::

## Bucket lifecycle policies

You can add a lifecycle policy to your storage bucket to limit how long to keep collection data.
This is similar to Estuary's 20-day limit on collection data when using the trial bucket.

Bucket lifecycle policies should **only** be applied to the `collection-data/` subfolder in a bucket.
Deleting data in the [`recovery/`](#recovery-logs) folder can cause tasks to fail.

You can apply a lifecycle policy to your storage bucket in:

* [AWS](#add-a-lifecycle-policy-in-aws)
* [Google Cloud](#add-a-lifecycle-policy-in-gcp)
* [Azure](#add-a-lifecycle-policy-in-azure)

### Add a lifecycle policy in AWS

To add a lifecycle policy in AWS:

1. Select your storage bucket in the AWS S3 console.

2. If `collection-data/` isn't located at the top level of your bucket, note the full path for the directory.

3. In the **Management** tab, click **Create lifecycle rule**.

4. Add a name for the rule.

5. Choose to **Limit the scope to specific prefixes or tags** and enter the full path for `collection-data/` as the prefix.

6. Select a desired action to take, such as deleting data or setting it to a different storage class.

7. Enter the number of days for the action to take effect and any other information required for the action.

8. Click **Create rule**.

For full instructions on creating and managing a lifecycle policy in AWS, see the [AWS docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/how-to-set-lifecycle-configuration-intro.html).

### Add a lifecycle policy in GCP

To add a lifecycle policy in GCP:

1. Select your storage bucket in the GCP Cloud Storage console.

2. If `collection-data/` isn't located at the top level of your bucket, note the full path for the directory.

3. In the **Lifecycle** tab, click **Add a rule**.

4. Choose a desired action, such as deleting data or setting it to a different storage class, and click **Continue**.

5. Under **Set Rule Scopes**, check **Object name matches prefix** and enter the path for `collection-data/`.

6. Under **Set Conditions**, configure one or more desired conditions, such as age in days.

7. Click **Create**.

For full instructions on creating and managing a lifecycle policy in GCP, see the [Google Cloud docs](https://cloud.google.com/storage/docs/lifecycle).

### Add a lifecycle policy in Azure

To add a lifecycle policy in Azure:

1. Go to your Azure storage account.

2. Note the full path for your `collection-data/` directory, including the container name.

3. Under **Data Management**, select **Lifecycle Management**.

4. In the **List View** tab, click **Add a rule**.

5. Provide a name.

6. Under **Rule scope**, select **Limit blobs with filters**.

7. Choose affected blob types and subtypes and click **Next**.

8. Fill out an if/then rule, choosing a timeline and action to take, such as deleting objects or changing their storage class after a number of days. Click **Next**.

9. Add your `collection-data/` path under **Blob prefix** as a **Filter set**.

10. Click **Add**.

For full instructions on creating and managing a lifecycle policy in Azure, see the [Azure docs](https://learn.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-overview).
