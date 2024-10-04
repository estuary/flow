begin;

-- It's necessary to drop the existing view in a separate statement instead of replacing it
-- because the existing view is owned by 'authenticated' and the new one must be owned by
-- 'postgres' so that it bypasses RLS policies.
drop view publication_specs_ext;

create view publication_specs_ext as
select p.live_spec_id,
    p.pub_id,
    p.detail,
    p.published_at,
    p.spec,
    p.spec_type,
    p.user_id,
    ls.catalog_name,
    ls.last_pub_id,
    u.email AS user_email,
    u.full_name AS user_full_name,
    u.avatar_url AS user_avatar_url,
    ls.data_plane_id  -- Added column
from live_specs ls
join publication_specs p on ls.id = p.live_spec_id
cross join lateral view_user_profile(p.user_id) u(user_id, email, full_name, avatar_url)
where
    exists (
        select 1
        from auth_roles('read'::grant_capability) r(role_prefix, capability)
        where ls.catalog_name ^@ r.role_prefix
    );

-- The view performs its own authz checks
grant select on publication_specs_ext to authenticated;

commit;
