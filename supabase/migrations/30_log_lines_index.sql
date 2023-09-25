
-- The previous BRIN index on internal.log_lines is ineffective for queries that don't 
-- supply a where condition on the `logged_at` timestamp. Currently, that represents
-- all queries against that table. This drops the old BRIN index in favor of a regular
-- btree index, which is effective when queries only provide the `token`.

begin;
create index idx_logs_token on internal.log_lines (token);
drop index internal.idx_logs_token_logged_at;
commit;

