-- Shared helper functions for pgTAP tests
-- This file is loaded before each test to provide common utilities

-- Note that seed.sql installs fixtures into auth.users (alice, bob, carol, dave)
-- having UUIDs like 1111*, 2222*, 3333*, etc.
create function set_authenticated_context(test_user_id uuid)
returns void as $$
begin

  set role postgres;
  execute 'set session request.jwt.claim.sub to "' || test_user_id::text || '"';
  set role authenticated;

end
$$ language plpgsql;
