# AWS IAM Authentication

Estuary supports IAM authentication with Amazon Web Services such as RDS and S3 using a role created by you which has access to the resources, and has trusted identity tokens signed by us as the OIDC (OpenID Connect) provider. Note however that not all connectors currently support using IAM authentication.

## Role with Resource Access

In order to authenticate using AWS IAM, you need a IAM role which has access to the resource you are trying to authenticate with.  Before we can setup the Identity Provider and the Role's Trust Relationship we need to know the Role ARN, so initially we will just create a placeholder role and later update it with the final Trust Relationship.

To create the role, select "AWS Account" and click next, select the required permissions for your resource, set the role name, and create the role.

For more information about role creation check the [IAM User Guide](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create.html).

## Identity Provider for Estuary

Next, you need to create an IAM OIDC (OpenID Connect) provider by heading to IAM -> Identity Providers and creating a new provider with the Audience set to the ARN of the role you just created.
The **issuer** will depend on your chosen data plane in Estuary.

To find the correct issuer value:

1. Navigate to the [Admin section](https://dashboard.estuary.dev/admin) of your Estuary dashboard.

2. Select the **Settings** tab.

3. Find the **Data Planes** table and make sure you're viewing the correct tab for your data plane (either **public** or **private**).

4. Copy the value from the **IAM OIDC** column. This should look something like: `https://openid.estuary.dev/your-data-plane-identifier.dp.estuary-data.com/`

For example, these are the issuer values for a few common public data planes:

| Data Plane | Issuer |
|---|---|
| US east-1 AWS data plane | https://openid.estuary.dev/aws-us-east-1-c1.dp.estuary-data.com/ |
| US central-1 GCP data plane | https://openid.estuary.dev/gcp-us-central1-c2.dp.estuary-data.com/ |
| US west-2 AWS data plane | https://openid.estuary.dev/aws-us-west-2-c1.dp.estuary-data.com/ |
| EU west-1 AWS data plane | https://openid.estuary.dev/aws-eu-west-1-c1.dp.estuary-data.com/ |

![Add Identity Provider](../guide-images/aws-iam-1.png)

# Trust Relationship in Role

Finally, return to the details page of your role, head to "Trust relationships" tab and add the following trust policy, replacing the principal with the ARN of the Identity Provider depending on the data plane (if you use a private deployment or BYOC, we will provide you with this value) you use and `:sub` condition with your tenant name so only tasks from your tenant are allowed to assume this role:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Federated": "<ARN OF IDENTITY PROVIDER>"
            },
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringEquals": {
                    "openid.estuary.dev/gcp-us-central1-c2.dp.estuary-data.com/:aud": "<ARN OF ROLE>"
                },
                "StringLike": {
                    "openid.estuary.dev/gcp-us-central1-c2.dp.estuary-data.com/:sub": "acmeCo/*"
                }
            }
        }
    ]
}
```
