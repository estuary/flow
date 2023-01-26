-- This script should be run after the publications created in tiered_stats_1.sql are finished.
-- ops-catalog/generate-migration.sh can then be run to (re-)generate the entire ops catalog,
-- including per-tenant specs.

alter table catalog_stats owner to stats_loader;
