begin;

-- Phase 3: Retire grant directives.
-- The invite links system (Phase 1a-b) has fully replaced grant directives.
-- Drop the transitional dual-write triggers and clean up old directive rows.

-- Drop triggers first, then their backing functions.
drop trigger if exists mirror_grant_directive on public.directives;
drop trigger if exists sync_directive_removal on public.directives;

drop function if exists internal.mirror_grant_directive_to_invite_links();
drop function if exists internal.sync_directive_removal_to_invite_links();

-- Delete applied_directives for grant directives (FK will not cascade from directives).
delete from applied_directives
where directive_id in (
    select id from directives where spec->>'type' = 'grant'
);

-- Delete the grant directives themselves.
delete from directives where spec->>'type' = 'grant';

commit;
