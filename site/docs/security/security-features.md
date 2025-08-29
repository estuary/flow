---
sidebar_position: 1
---

# Security Features

Estuary provides a number of security features to protect your data. This document covers some of the broadest capabilities built into the Estuary platform.

You can also find guides on setting up specific features, such as:

* [Allowlisting Estuary's IP addresses](/reference/allow-ip-addresses)
* Authentication methods for connectors:
   * [IAM authentication](/guides/iam-auth/aws)
   * [PrivateLink](/private-byoc/privatelink)
   * [SSH tunneling](/guides/connect-network/#configure-connections-with-ssh-tunneling)

### Data encryption in motion and at rest

Data is encrypted at every step of the way. Connector secrets are [automatically encrypted](/concepts/flowctl/#protecting-secrets), whether you set them up in the UI or via the `flowctl` CLI.

### Immutable infrastructure

Systems are rebuilt with every new update to ensure they remain secure and up-to-date.

### Zero-trust network model

All communications are secured using TLS and mutual TLS (mTLS) for internal communications.

### Role-based access control (RBAC)

Role-based access control ensures secure resource sharing, with centralized verification of authorizations.
