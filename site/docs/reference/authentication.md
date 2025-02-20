---
sidebar_position: 1
---

# Authorizing users and authenticating with Flow

Read, write, and admin capabilities over Flow catalogs and the [collections](../concepts/collections.md) that comprise them
are granted to Flow users through **capabilities**.

Capabilities are granted in terms of **prefixes** within the Flow [namespace](../concepts/catalogs.md#namespace).
By default, each organization has a unique top-level prefix.
For example, if you worked for Acme Co, your assigned organization prefix would be `acmeCo/`.
You may further divide your namespace however you'd like; for example `acmeCo/anvils` and `acmeCo/roadrunners`.
When you name a collection, you can customize the prefix, and capabilities can be configured at any prefix level.
This allows you to flexibly control access to your Flow data.

The available capabilities are:

* **`read`**: Allows the subject to read data from collections of the given prefix.

* **`write`**: Allows the subject to read and write data from collections of the given prefix.

* **`admin`**: Allows the subject to read and write data from collections of the given prefix,
and to manage storage mappings, catalog specifications, and capability grants within the prefix.
The admin capability also inherits capabilities granted to the prefix, as discussed below.

## Subjects, objects, and inherited capabilities

The entity to which you grant a capability is called the **subject**, and the entity over which access is granted is called the **object**.
The subject can be either a user or a prefix, and the object is always a prefix. This allows subjects to inherit nested capabilities,
so long as they are granted `admin`.

For example, user X of Acme Co has admin access to the `acmeCo/` prefix, and user Y has write access.
A third party has granted `acmeCo/` read access to shared data at `outside-org/acmeCo-share/`.
User X automatically inherits read access to `outside-org/acmeCo-share/`, but user Y does not.

## Default authorization settings

When you first sign up to use Flow, your organization is provisioned a prefix, and your username is granted admin access to the prefix.
Your prefix is granted write access to itself and read access to its logs, which are stored under a unique sub-prefix of the global `ops/` prefix.

Using the same example, say user X signs up on behalf of their company, AcmeCo. User X is automatically granted `admin` access to the `acmeCo/` prefix.
`acmeCo/`, in turn, has write access to `acmeCo/` and read access to `ops/acmeCo/`.

As more users and prefixes are added, admins can [provision capabilities](#provisioning-capabilities) using the CLI.

## Authenticating Flow in the web app

You must sign in to begin a new session using the [Flow web application](https://dashboard.estuary.dev).
For the duration of the session, you'll be able to perform actions depending on the capabilities granted to the user profile.

You can view the capabilities currently provisioned in your organization on the **Admin** tab.

## Authenticating Flow using the CLI

You can use the [flowctl](../concepts/flowctl.md) CLI to work with your organization's catalogs and drafts in your local development environment.

To authenticate a local development session using the CLI, do the following:

1. Ensure that you have an Estuary account and have signed into the Flow web app before.

2. In the terminal of your local development environment, run:
   ``` console
   flowctl auth login
   ```
   In a browser window, the web app opens to the CLI-API tab.

3. Copy the access token.

4. Return to the terminal, paste the access token, and press Enter.

The token will expire after a predetermined duration. Repeat this process to re-authenticate.

## Provisioning capabilities

As an admin, you can provision capabilities using the CLI with the subcommands of `flowctl auth roles`.

For example:

* `flowctl auth roles list` returns a list of all currently provisioned capabilities

* `flowctl auth roles grant --object-role=acmeCo/ --capability=admin --subject-user-id=userZ` grants user Z admin access to `acmeCo`

* `flowctl auth roles revoke --object-role=outside-org/acmeCo-share/ --capability=read --subject-role=acmeCo/` would be used by an admin of `outside-org`
to revoke `acmeCo/`'s read access to `outside-org/acmeCo-share/`.

You can find detailed help for all subcommands using the `--help` or `-h` flag.

