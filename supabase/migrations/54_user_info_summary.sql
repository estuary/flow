begin;

create function user_info_summary()
returns json as $$
    with all_grants(role_prefix, capability) as (
        select role_prefix, capability from auth_roles()
    )
    select json_build_object(
        'hasDemoAccess', exists(select 1 from all_grants where role_prefix = 'demo/' and capability >= 'read'),
        'hasSupportAccess', exists(select 1 from all_grants where role_prefix = 'estuary_support/' and capability >= 'admin'),
        'hasAnyAccess', exists(select 1 from all_grants)
    )

$$
language sql security invoker;

comment on function user_info_summary is
'Returns a JSON object with a few computed attributes for the UI.
These would otherwise require the UI to fetch the complete list of authorized grants,
which can be quite slow for users with many grants. Returns a response like:
{
    hasDemoAccess: boolean, //true if the user has `read` on `demo/` tenant,
    hasSupportAccess: boolean, // true if user has `admin` on `estuary_support/`
    hasAnyAccess: boolean, // true if user has any authorization grants at all
}';

commit;
