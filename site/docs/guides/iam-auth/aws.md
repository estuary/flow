# AWS IAM Authentication

Flow supports IAM authentication with Amazon Web Services such as RDS and S3 using a role created by you which has access to the resources, and has trusted our AWS user to be able to assume it. Note however that not all connectors currently support using IAM authentication.

## Role with Resource Access

In order to authenticate using AWS IAM, you need to have a role set up which has access to the resource you are trying to authenticate with. Follow the guide [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create.html) to create a role which has access to your resource and allows a maximum session of 12 hours. Then in the details page of your role, head to "Trust relationships" tab and add the following trust policy, replacing the principal with one of the AWS user ARNs in the table below depending on the data plane (if you use a private deployment or BYOC, we will provide you with this value) you use and `ExternalId` with your tenant name so only tasks from your tenant are allowed to assume this role:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "<DATA PLANE USER>"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringLike": {
                    "sts:ExternalId": "yourTenantName/*"
                }
            }
        }
    ]
}
```

| Data Plane | SUBJECT_ATTRIBUTE_VALUE |
|---|---|
| US central-1 GCP data plane | `arn:aws:iam::789740162118:user/flow-aws` |
| EU west-1 AWS data plane | `arn:aws:iam::770785070253:user/data-planes/data-plane-342o84ecos0opkov` |
