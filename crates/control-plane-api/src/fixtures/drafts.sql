-- Two non-admin users for the draft API tests, to be loaded alongside
-- `alice.sql`, which supplies the `aliceCo/` admin and its live specs.
--
-- Drafts are private to their owner, but the specs and diagnostics they contain
-- are filtered by catalog capability. These two users separate those two axes
-- from the tenant admin that `alice.sql` provides:
--
--   * bob owns drafts and reads `bobCo/`, but holds no authority to edit any
--     catalog name. He is the caller who may create and discard a draft yet
--     cannot stage a change into it.
--   * editor holds the `editor` bundle on `editorCo/` without the legacy
--     `admin` capability. The bundle carries the individual `CatalogRead` bit
--     used to count and page a draft's readable specs, which is what separates
--     catalog visibility from the ownership that governs the draft itself.
do $$
declare
  bob_uid uuid := '22222222-2222-2222-2222-222222222222';
  editor_uid uuid := '33333333-3333-3333-3333-333333333333';
begin

  insert into auth.users (id, email) values
    (bob_uid, 'bob@example.test'),
    (editor_uid, 'editor@example.test')
  ;

  insert into public.user_grants (user_id, object_role, capability) values
    (bob_uid, 'bobCo/', 'read')
  ;

  -- `none` rather than `read`: the legacy capability is deliberately withheld
  -- so that any read this user is permitted must come from the bundle.
  insert into public.user_grants (user_id, object_role, capability, bundles) values
    (editor_uid, 'editorCo/', 'none', '{editor}')
  ;

end
$$;
