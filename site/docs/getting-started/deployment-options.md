---
id: deployment-options
title: Deployment options
sidebar_position: 2
---

# Estuary Flow Deployment Options

Estuary Flow offers flexible deployment options to meet a range of organizational needs, from simple SaaS setups to
fully customized cloud deployments. This guide provides a detailed overview of the three main deployment options
available: **Public Deployment**, **Private Deployment**, and **BYOC** (Bring Your Own Cloud).

Public Deployment is ideal for teams seeking a fast, hassle-free solution with minimal operational overhead. It's
perfect for those who don't require heavy customization or enhanced data security.

## Public Deployment

Public Deployment is Estuary Flow's standard Software-as-a-Service (SaaS) offering, designed for ease of use and quick
setup.

![Public Deployment](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//public_deployment_39e1de1537/public_deployment_39e1de1537.png)

- **Fully managed by Estuary**: Estuary handles all operational aspects, including updates and maintenance.
- **Quick setup**: Minimal configuration is needed to get started.
- **Multiple data processing regions**: You can choose between various regions for data planes, like EU or US.
- **Automatic updates**: New features and security patches are automatically applied.
- **Suitable for less stringent security requirements**: Best for organizations without strict data compliance needs.

---

## Private Deployment

Private Deployment offers the security and control of a dedicated infrastructure while retaining the simplicity of a
managed service.

Private Deployment is suited for large enterprises and organizations with strict data governance requirements, such as
those in regulated industries (e.g., healthcare, finance) or those handling highly sensitive data.

:::note
If you are interested in setting up Private Deployments, reach out to us via [email](mailto:support@estuary.dev) or
join our [Slack channel](https://go.estuary.dev/slack) and send us a message!
:::

![Private Deployment](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//private_deployment_18e21ce056/private_deployment_18e21ce056.png)

- **Enhanced security**: Data and all processing remains within your private network, offering improved protection.
- **Immutable infrastructure**: Security updates are seamlessly integrated without disruption.
- **Compliant with strict data security standards**: Supports industries with rigorous compliance needs.
- **Cross-region data movement**: Allows for seamless data migration between regions.

---

## BYOC (Bring Your Own Cloud)

With BYOC, customers can deploy Estuary Flow directly within their own cloud environment, allowing for greater
flexibility and control.

BYOC is the ideal solution for organizations that have heavily invested in their cloud infrastructure and want to
maintain full control while integrating Estuary Flow’s capabilities into their stack. This option offers the highest
flexibility in terms of customization and compliance.

:::note
If you are interested in BYOC, reach out to us via [email](mailto:support@estuary.dev) or join
our [Slack channel](https://go.estuary.dev/slack) and send us a message!
:::

![BYOC Deployment](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//byoc_deployment_f88f0a3e94/byoc_deployment_f88f0a3e94.png)

- **Complete control over cloud infrastructure**: Manage the cloud environment according to your organization's
  policies.
- **Utilize existing cloud resources**: Leverage your current cloud setup, including any existing cloud credits or
  agreements.
- **Customizable**: Tailor Estuary Flow's deployment to fit specific needs and compliance requirements.
- **Cost savings**: Potential to reduce costs by using existing cloud infrastructure and negotiated pricing.
- **Flexible data residency**: You choose where data is stored and processed, ensuring compliance with regional
  regulations.

## Self-hosting Flow

The Flow runtime is available under
the [Business Source License](https://github.com/estuary/flow/blob/master/LICENSE-BSL). It's possible to self-host Flow
using a cloud provider of your choice.

:::caution Beta
Setup for self-hosting is not covered in this documentation, and full support is not guaranteed at this time.
We recommend using the [hosted version of Flow](../concepts/web-app.md) for the best experience.
If you'd still like to self-host, refer to the [GitHub repository](https://github.com/estuary/flow) or
the [Estuary Slack](https://join.slack.com/t/estuary-dev/shared_invite/zt-86nal6yr-VPbv~YfZE9Q~6Zl~gmZdFQ).
:::