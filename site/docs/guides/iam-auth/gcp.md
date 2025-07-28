# Google Cloud IAM Authentication

Flow supports IAM authentication with Google Cloud Platform services such as Cloud SQL and Storage, and here you can find instructions for setting up your GCP account to prepare for using IAM authentication. Note however that not all connectors currently support using IAM authentication.

## Service Account with Resource Access

In order to authenticate using Google Cloud IAM, you need to have a service account set up in your project which has access to the resource you are trying to authenticate with. Follow the guide [here](https://cloud.google.com/iam/docs/service-accounts-create) to create a service account and navigate to IAM & Admin -> Service Accounts, find your service account and in the "Principals with access" tab, use "Grant access" to grant the "Service Account Token Creator" to the service account itself.

## Workload Identity Pool and Provider

We use [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation) to enable IAM authentication in a secure manner. The first step to prepare for IAM authentication is creating a Workload Identity Pool. Navigate to Google Console and find [IAM & Admin] -> [Workload Identity Federation](https://console.cloud.google.com/iam-admin/workload-identity-pools) and use the "Create pool" button:

![Workload Identity Create Pool Button](../guide-images/gcp-iam-0-create-pool.png)

Give your workload identity pool your desired name, and select OpenID Connect (OIDC) as the provider, with the details below. If you are using a private deployment or BYOC, your issuer address will be provided to you by our team. At this step, take note of the audience value as you will need this when configuring connectors with GCP IAM:

| Field | Value |
|---|---|
| Provider Name | estuary-flow-google |
| Issuer (US GCP central-1 data plane) | https://openid.estuary.dev/gcp-us-central1-c2.dp.estuary-data.com/ |
| Issuer (EU AWS west-1 data plane) | https://openid.estuary.dev/aws-eu-west-1-c1.dp.estuary-data.com/ |

![Workload Identity Provider Configuration](../guide-images/gcp-iam-1-provider.png)

Finally set up provider attributes to the following values, replacing your tenant name in the attribute condition:

| Field | Value |
|---|---|
| google.subject | assertion.sub |
| attribute.task_name | assertion.task_name |
| Attribute Conditions | attribute.task_name.startsWith("yourTenantName/") |

![Workload Identity Provider Attributes Configuration](../guide-images/gcp-iam-2-provider-attributes.png)

Next, copy the IAM principal you see in the workload identity pool details page and replace SUBJECT_ATTRIBUTE_VALUE with one of the following values depending on your data plane:

| Data Plane | SUBJECT_ATTRIBUTE_VALUE |
|---|---|
| US central-1 GCP data plane | gcp-us-central1-c2.dp.estuary-data.com |
| EU west-1 AWS data plane | aws-eu-west-1-c1.dp.estuary-data.com |

![Workload Identity Pool Principal](../guide-images/gcp-iam-3-principal.png)

Now in IAM & Admin -> Service Accounts, find your service account which has access to the resource you want to authenticate to, and in its Principals with access tab, grant the "Workload Identity User" role to the workload identity principal with the SUBJECT_ATTRIBUTE_VALUE filled in, e.g.

```
principal://iam.googleapis.com/projects/12345/locations/global/workloadIdentityPools/estuary-flow-internal/subject/flow-258@helpful-kingdom-273219.iam.gserviceaccount.com
```

![Workload Identity User Access Granted to Principal](../guide-images/gcp-iam-4-identity-user-access.png)

Now when configuring the connector which supports GCP, you will need to provide the service account identifier which has access to the resource and the workload identity pool's audience (which you can find in the pool provider details page, also noted in the step when creating the provider).
