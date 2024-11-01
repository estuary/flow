# Configuring your cloud storage bucket for use with Flow

New Flow accounts are connected to Flow's secure cloud storage bucket to store collection data.
To switch to your own bucket, choose a cloud provider and complete the setup steps:

* [Google Cloud Storage](#google-cloud-storage-buckets)

* [Amazon S3](#amazon-s3-buckets)

* [Azure Blob Storage](#azure-blob-storage)

Once you're done, [get in touch](#give-us-a-ring).

## Google Cloud Storage buckets

You'll need to grant Estuary Flow access to your GCS bucket.

1. [Create a bucket to use with Flow](https://cloud.google.com/storage/docs/creating-buckets), if you haven't already.

2. Follow the steps
   to [add a principal to a bucket level policy](https://cloud.google.com/storage/docs/access-control/using-iam-permissions#bucket-add).
   As you do so:

    - For the principal, enter `flow-258@helpful-kingdom-273219.iam.gserviceaccount.com`

    - Select the [`roles/storage.admin`](https://cloud.google.com/storage/docs/access-control/iam-roles) role.

## Amazon S3 buckets

You'll need to grant Estuary Flow access to your S3 bucket.

1. [Create a bucket to use with Flow](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html),
   if you haven't already.

2. Follow the steps
   to [add a bucket policy](https://docs.aws.amazon.com/AmazonS3/latest/userguide/add-bucket-policy.html), pasting the
   policy below.
   Be sure to replace `YOUR-S3-BUCKET` with the actual name of your bucket.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowUsersToAccessObjectsUnderPrefix",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::789740162118:user/flow-aws"
      },
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::YOUR-S3-BUCKET/*"
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::789740162118:user/flow-aws"
      },
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::YOUR-S3-BUCKET"
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::789740162118:user/flow-aws"
      },
      "Action": "s3:GetBucketPolicy",
      "Resource": "arn:aws:s3:::YOUR-S3-BUCKET"
    }
  ]
}
```

## Azure Blob Storage

You'll need to grant Estuary Flow access to your storage account and container.
You'll also need to provide some identifying information.

1. [Create an Azure Blob Storage container](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container)
   to use with Flow, if you haven't already.

2. Gather the following information. You'll need this when you contact us to complete setup.

    - Your **Azure AD tenant ID**. You can find this in the **Azure Active Directory** page.
      ![Azure AD Tenant ID](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//Azure_AD_Tenant_ID_1b60184837/Azure_AD_Tenant_ID_1b60184837.png)

    - Your **Azure Blob Storage account ID**. You can find this in the **Storage Accounts** page.
      ![Azure Storage Account Name](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//Azure_Storage_Account_Name_82aa30ae17/Azure_Storage_Account_Name_82aa30ae17.png)

    - Your **Azure Blob Storage container ID**. You can find this inside your storage account.
      ![Azure Container ID](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//Azure_Container_ID_1980bbc9f3/Azure_Container_ID_1980bbc9f3.png)

   You'll grant Flow access to your storage resources by connecting to Estuary's
   [Azure application](https://learn.microsoft.com/en-us/azure/active-directory/manage-apps/what-is-application-management).

3. Add Estuary's Azure application to your tenant.

import { AzureAuthorizeComponent } from "./azureAuthorize";
import BrowserOnly from "@docusaurus/BrowserOnly";

<BrowserOnly>{() => <AzureAuthorizeComponent />}</BrowserOnly>

4. Grant the application access to your storage account via the
   [
   `Storage Blob Data Owner`](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-blob-data-owner)
   IAM role.

    - Inside your storage account's **Access Control (IAM)** tab, click **Add Role Assignment**.

    - Search for `Storage Blob Data Owner` and select it.

    - On the next page, make sure `User, group, or service principal` is selected, then click **+ Select Members**.

    - You must search for the exact name of the application, otherwise it won't show up: `Estuary Storage Mappings Prod`

    - Once you've selected the application, finish granting the role.

   For more help, see
   the [Azure docs](https://learn.microsoft.com/en-us/azure/role-based-access-control/role-assignments-portal).

## Add the Bucket

If your bucket is for Google Cloud Storage or AWS S3, you can add it yourself. Once you've finished the above steps,
head to "Admin", "Settings" then "Configure Cloud Storage"
and enter the relevant information there and we'll start to use your bucket for all data going forward.

If your bucket is for Azure, send support@estuary.dev an email with the name of the storage bucket and any other
information you gathered per the steps above.
Let us know whether you want to use this storage bucket to for your whole Flow account, or just a
specific [prefix](../concepts/catalogs.md#namespace).
We'll be in touch when it's done!