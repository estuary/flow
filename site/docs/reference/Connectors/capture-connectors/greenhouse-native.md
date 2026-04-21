# Greenhouse

This connector captures data from [Greenhouse](https://www.greenhouse.com/) into Estuary collections.

## Supported data resources

The following data resources are supported through the Greenhouse Harvest API:

* [applications](https://harvestdocs.greenhouse.io/reference/get_v3-applications)
* [application_stages](https://harvestdocs.greenhouse.io/reference/get_v3-application-stages)
* [applied_candidate_tags](https://harvestdocs.greenhouse.io/reference/get_v3-applied-candidate-tags)
* [approval_flows](https://harvestdocs.greenhouse.io/reference/get_v3-approval-flows)
* [approver_groups](https://harvestdocs.greenhouse.io/reference/get_v3-approver-groups)
* [approvers](https://harvestdocs.greenhouse.io/reference/get_v3-approvers)
* [attachments](https://harvestdocs.greenhouse.io/reference/get_v3-attachments)
* [candidates](https://harvestdocs.greenhouse.io/reference/get_v3-candidates)
* [candidate_attribute_types](https://harvestdocs.greenhouse.io/reference/get_v3-candidate-attribute-types)
* [candidate_educations](https://harvestdocs.greenhouse.io/reference/get_v3-candidate-educations)
* [candidate_employments](https://harvestdocs.greenhouse.io/reference/get_v3-candidate-employments)
* [candidate_tags](https://harvestdocs.greenhouse.io/reference/get_v3-candidate-tags)
* [close_reasons](https://harvestdocs.greenhouse.io/reference/get_v3-close-reasons)
* [custom_fields](https://harvestdocs.greenhouse.io/reference/get_v3-custom-fields)
* [custom_field_options](https://harvestdocs.greenhouse.io/reference/get_v3-custom-field-options)
* [demographic_answers](https://harvestdocs.greenhouse.io/reference/get_v3-demographic-answers)
* [demographic_questions](https://harvestdocs.greenhouse.io/reference/get_v3-demographic-questions)
* [demographic_question_sets](https://harvestdocs.greenhouse.io/reference/get_v3-demographic-question-sets)
* [departments](https://harvestdocs.greenhouse.io/reference/get_v3-departments)
* [eeoc](https://harvestdocs.greenhouse.io/reference/get_v3-eeoc)
* [email_templates](https://harvestdocs.greenhouse.io/reference/get_v3-email-templates)
* [interviews](https://harvestdocs.greenhouse.io/reference/get_v3-interviews)
* [interviewers](https://harvestdocs.greenhouse.io/reference/get_v3-interviewers)
* [interviewer_tags](https://harvestdocs.greenhouse.io/reference/get_v3-interviewer-tags)
* [job_hiring_managers](https://harvestdocs.greenhouse.io/reference/get_v3-job-hiring-managers)
* [job_interview_stages](https://harvestdocs.greenhouse.io/reference/get_v3-job-interview-stages)
* [job_notes](https://harvestdocs.greenhouse.io/reference/get_v3-job-notes)
* [job_posts](https://harvestdocs.greenhouse.io/reference/get_v3-job-posts)
* [jobs](https://harvestdocs.greenhouse.io/reference/get_v3-jobs)
* [offers](https://harvestdocs.greenhouse.io/reference/get_v3-offers)
* [offices](https://harvestdocs.greenhouse.io/reference/get_v3-offices)
* [openings](https://harvestdocs.greenhouse.io/reference/get_v3-openings)
* [prospect_pools](https://harvestdocs.greenhouse.io/reference/get_v3-prospect-pools)
* [prospect_pool_stages](https://harvestdocs.greenhouse.io/reference/get_v3-prospect-pool-stages)
* [referrers](https://harvestdocs.greenhouse.io/reference/get_v3-referrers)
* [rejection_details](https://harvestdocs.greenhouse.io/reference/get_v3-rejection-details)
* [rejection_reasons](https://harvestdocs.greenhouse.io/reference/get_v3-rejection-reasons)
* [scorecard_questions](https://harvestdocs.greenhouse.io/reference/get_v3-scorecard-questions)
* [scorecards](https://harvestdocs.greenhouse.io/reference/get_v3-scorecards)
* [sources](https://harvestdocs.greenhouse.io/reference/get_v3-sources)
* [user_emails](https://harvestdocs.greenhouse.io/reference/get_v3-user-emails)
* [user_job_permissions](https://harvestdocs.greenhouse.io/reference/get_v3-user-job-permissions)
* [user_roles](https://harvestdocs.greenhouse.io/reference/get_v3-user-roles)
* [users](https://harvestdocs.greenhouse.io/reference/get_v3-users)

By default, each resource is mapped to an Estuary collection through a separate binding.

## Prerequisites

* A set of Greenhouse Harvest V3 (OAuth) API credentials. See [Greenhouse's documentation](https://harvestdocs.greenhouse.io/docs/authentication#custom-integrations-oauth-20-client-credentials-step-by-step) for instructions on how to generate these credentials.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Greenhouse source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/client_id`** | Client ID | The Client ID from your Greenhouse API credentials. | string | Required |
| **`/credentials/client_secret`** | Client Secret | The Client secret from your Greenhouse API credentials. | string | Required |
| **`/credentials/credentials_title`** | Authentication Method | Set to `Client Credentials`. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present date. | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string |          |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-greenhouse-native:v1
        config:
          credentials:
            client_id: <secret>
            client_secret: <secret>
            access_token: <secret>
          start_date: 2026-03-30T12:00:00Z
    bindings:
      - resource:
          name: application_stages
        target: ${PREFIX}/application_stages
```
