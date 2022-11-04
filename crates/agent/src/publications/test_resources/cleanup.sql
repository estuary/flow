with specs_delete as (
    delete from live_specs
),
drafts_delete as (
    delete from drafts
),
draft_specs_delete as (
    delete from draft_specs
),
publications_delete as (
    delete from publications
),
role_grants_delete as (
    delete from role_grants
),
user_grants_delete as (
    delete from user_grants
),
tags_delete as (
    delete from connector_tags
),
connectors_delete as (
    delete from connectors
),
applied_directives_delete as (
    delete from applied_directives
),
grants_delete as (
    delete from user_grants
),
users_delete as (
    delete from auth.users
)
select 1;