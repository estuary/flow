-- Holy extraneous indexes, Batman! We've somehow accumulated a whole load of
-- indexes that we don't need. This migration drops the extra indexes, and add a
-- new index on `catalog_stats_hourly.ts`. Also, the existing primary key index
-- needs rebuilt, since it's become quite bloated. This is not the first time that
-- the index has become bloated and needed rebuilt, so a cron job is added to do
-- that weekly.
--
-- The new index is a quick and dirty fix for the immediate problem of
-- `delete_old_hourly_stats` timing out. A better solution would be to partition
-- `catalog_stats_hourly` by time (so deleting expired stats is just dropping an
-- old partition and creating a new one), or to use something like timescaledb.
-- But either of those would take a bit too much time to be immediately useful.
--
-- Note that _dropping_ indexes is done in a transaction, and does _not_ use
-- `concurrently` because it's unsupported for partitioned tables, and dropping
-- should be relatively quick. But indexes are built _outside_ of the
-- transaction so that it can be done concurrently. This is important in order
-- to allow our `stats-view` materialization to continue while the index is
-- built.
--
-- Start by rebuilding the primary key index, so that it will be ready to use
-- once we drop the `_ccnew` index.
reindex index concurrently public.catalog_stats_hourly_pkey;

begin;

drop index if exists public.catalog_stats_catalog_index_spgist;
drop index if exists public.catalog_stats_catalog_index;

drop index if exists public.catalog_stats_daily_catalog_name_idx3;

drop index if exists public.catalog_stats_hourly_catalog_name_idx3;
drop index if exists public.catalog_stats_hourly_catalog_name_idx3_ccnew;
drop index if exists public.catalog_stats_hourly_catalog_name_idx_ccnew;
drop index if exists public.catalog_stats_hourly_pkey_ccnew;

drop index if exists public.catalog_stats_monthly_catalog_name_idx3;

select cron.schedule('reindex-catalog-stats-hourly', '6 6 * * 2', 'reindex index concurrently public.catalog_stats_hourly_pkey;');

commit;

create index concurrently catalog_stats_hourly_ts_idx on public.catalog_stats_hourly(ts);
comment on index public.catalog_stats_hourly_ts_idx is
  'Used by the delete_old_hourly_stats function to enable faster deletions';
