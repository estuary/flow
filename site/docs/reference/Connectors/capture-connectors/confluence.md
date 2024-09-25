
# Confluence

This connector captures data from Confluence into Flow collections via the Confluence [Cloud REST API](https://developer.atlassian.com/cloud/confluence/rest/v1/intro/#about).

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-confluence:dev`](https://ghcr.io/estuary/source-confluence:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

When you [configure the connector](#endpoint), you specify your email, api and domain name

From your selection, the following data resources are captured:

### resources

 - [Audit](https://developer.atlassian.com/cloud/confluence/rest/api-group-audit/#api-wiki-rest-api-audit-get)
 - [Blog Posts](https://developer.atlassian.com/cloud/confluence/rest/api-group-content/#api-wiki-rest-api-content-get)
 - [Group](https://developer.atlassian.com/cloud/confluence/rest/api-group-group/#api-wiki-rest-api-group-get)
 - [Pages](https://developer.atlassian.com/cloud/confluence/rest/api-group-content/#api-wiki-rest-api-content-get)
 - [Space](https://developer.atlassian.com/cloud/confluence/rest/api-group-space/#api-wiki-rest-api-space-get)

Each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

 - Atlassian API Token
 - Your Confluence domain name
 - Your Confluence login email

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the GitHub source connector.

1. Create an API Token
 - For detailed instructions on creating an Atlassian API Token, please refer to the [official documentation](https://support.atlassian.com/atlassian-account/docs/manage-api-tokens-for-your-atlassian-account/).
2. Set up the Confluence connector in Estuary Flow
 - Log into Estuary Flow and click "Captures".
 - Select "Create Capture" search for and click on "Confluence"
 - Enter a Capture Name
 - In the "API Token" field, enter your Atlassian API Token
 - In the "Domain Name" field, enter your Confluence Domain name
 - In the "Email" field, enter your Confluence login email
 - Click "Save and Publish"


### Sample

This sample specification reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-confluence:dev
          config:
            credentials:
              api_token: PAT Credentials
            domain_name: estuary1.atlassian.net
            email: dave@estuary.dev
      bindings:
        - resource:
            stream: audit
            syncMode: full_refresh
          target: ${PREFIX}/audit
       {...}
```
