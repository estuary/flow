-- Storage mappings of several tenants. `data_planes` is the only part of a
-- mapping's spec which authorization reads: it stands in for residency when a
-- task isn't in the Snapshot, naming the planes the task could be created in.
--
-- `aliceCo/` admits plane one, and `bobCo/` admits only plane two -- so a
-- request from plane one under `bobCo/` is covered by a mapping but not
-- admitted by it. `carolCo/` deliberately has no mapping at all.
--
-- `aliceCo/private/` nests under `aliceCo/` and admits only plane two: the
-- longest covering mapping decides alone, so under it plane one is denied
-- despite the parent mapping, and plane two is admitted despite it.
insert into public.storage_mappings (catalog_prefix, spec) values (
  'aliceCo/',
  '{"stores":[{"provider":"S3","bucket":"alice-bucket"}],"data_planes":["ops/dp/public/aws-us-west-2-c1"]}'
), (
  'aliceCo/private/',
  '{"stores":[{"provider":"S3","bucket":"alice-private-bucket"}],"data_planes":["ops/dp/public/gcp-us-central1-c2"]}'
), (
  'bobCo/',
  '{"stores":[{"provider":"S3","bucket":"bob-bucket"}],"data_planes":["ops/dp/public/gcp-us-central1-c2"]}'
);
