-- Run this after the publication to delete individual tenant reporting tasks has completed, and
-- prior to deploying the new ops-catalog service.

alter table catalog_stats owner to stats_loader;