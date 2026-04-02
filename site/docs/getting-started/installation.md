---
title: Configure Cloud Storage
description: Learn how to configure Estuary to use your cloud storage
slug: installation
sidebar_position: 5
---

# Configuring Cloud Storage for Estuary

New Estuary accounts use Estuary's secure cloud storage bucket to store collection data.
For production workflows, you should configure your own bucket.

Estuary supports Google Cloud Storage, Amazon S3, and Azure Blob Storage.

## Create a Storage Mapping

To connect your own cloud storage, create a storage mapping from the Estuary dashboard:

1. Go to the **Admin** page and select the [**Settings**](https://dashboard.estuary.dev/admin/settings) tab.

2. In the **Cloud Storage** section, click **Add Storage Mapping**.

3. Choose the catalog prefix that this storage mapping will cover.
   Collections under this prefix will use the configured bucket unless covered
   by a more specific storage mapping.

4. Select one or more data planes and designate a default.
   You can also select [private data planes](/private-byoc) if available.

5. Select your cloud provider (AWS, GCP, or Azure) and fill in
   the bucket or container name along with any provider-specific fields.

:::tip
Choosing a data plane in the same cloud provider and region as your storage bucket
reduces latency and avoids cross-region or cross-provider egress fees.
:::

6. Click **Test Connection** to verify that each data plane can access
   your storage bucket. The storage mapping dialog tests every data-plane/storage-location pair and reports
   pass or fail status for each.

   If a test fails, expand it to see the error and the setup instructions for your
   cloud provider. These instructions include the specific values you need — such as
   service account emails, IAM ARNs, application names, and bucket policies — so you
   can grant Estuary access in your cloud provider's console.

   See [provider requirements](#provider-requirements) below if you want to
   review the required permissions ahead of time.

7. Once all connection tests pass, save the storage mapping.

## Updating a Storage Mapping

To update an existing storage mapping, click on it in the **Cloud Storage** table on the
Admin Settings page. From the update dialog, you can add or remove data planes,
change the primary storage location, and re-run connection tests.

Adding a data plane triggers a connection test against all configured storage locations.
When you change the primary storage location, the previous location is kept as inactive
so that historical data remains accessible. Connection tests run automatically when the
dialog opens and can be re-run at any time.

## Migrating Existing Data

Once you've created your new storage mapping, new data will be written to your bucket.
Existing data from your previous storage mapping is not automatically migrated.

Most tenants will have been using the estuary-public storage mapping on the Free plan,
which expires data after 20 days. To ensure you retain a full view of your data,
backfill all of your captures after configuring the new storage mapping.

## Bucket Lifecycle Policies

You can set a [bucket lifecycle policy](../concepts/storage-mappings.md#bucket-lifecycle-policies) to limit
how long collections keep data.
Lifecycle policies should be specific to the `collection-data/` sub-directory that Estuary creates,
**not** cover the entire bucket.

## Provider Requirements

During connection testing, the storage mapping dialog provides step-by-step instructions with
the exact values needed to connect your selected data plane to your cloud storage bucket. Below is a summary of what
Estuary requires from each provider.

:::tip
When configuring your bucket, don't limit access based on a data plane's IPv4 addresses.

You can use the data plane's IPv6 addresses to restrict access.
Or, storage for private data planes can use [PrivateLink](/private-byoc/privatelink) to restrict access.
:::

### Google Cloud Storage

For a [GCS bucket](https://cloud.google.com/storage/docs/creating-buckets), update the bucket's [IAM policy](https://cloud.google.com/storage/docs/access-control/using-iam-permissions#bucket-add) to include:

* The data plane's service account as the **principal**
* [`roles/storage.admin`](https://cloud.google.com/storage/docs/access-control/iam-roles) as the **role**

You can configure this through the [Cloud Console](https://console.cloud.google.com/) or
the `gsutil` CLI.

### Amazon S3

For an [S3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html),
add a new [bucket policy](https://docs.aws.amazon.com/AmazonS3/latest/userguide/add-bucket-policy.html).
Estuary's data plane IAM user will need the following actions:

* `s3:GetObject`
* `s3:PutObject`
* `s3:DeleteObject`
* `s3:ListBucket`
* `s3:GetBucketPolicy`

You can apply the policy through the [AWS Console](https://console.aws.amazon.com/s3/) or the `aws` CLI.
The storage mapping dialog provides a ready-to-use policy JSON during connection testing.

### Azure Blob Storage

For an [Azure storage account](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create)
and [blob container](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container):

* Make sure the storage account's `Hierarchical Namespace` option is **disabled**.

  This option can be found under the **Advanced** tab during creation.

* Grant Estuary's Azure application
  [admin consent](https://learn.microsoft.com/en-us/azure/active-directory/manage-apps/grant-admin-consent)
  in your Azure AD tenant.

* Assign Estuary's Azure application the
  [`Storage Blob Data Contributor`](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-blob-data-contributor)
  role on the storage account.

The storage mapping dialog provides the specific consent link and application
name for your use case during connection testing.
