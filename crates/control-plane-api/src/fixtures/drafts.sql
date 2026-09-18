-- A viewer without edit grants, alongside alice.sql's tenant admin.
insert into auth.users (id, email) values
  ('22222222-2222-2222-2222-222222222222', 'bob@example.test');

insert into public.user_grants (user_id, object_role, capability) values
  ('22222222-2222-2222-2222-222222222222', 'bobCo/', 'read');
