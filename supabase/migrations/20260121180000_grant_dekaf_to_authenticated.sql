begin;

-- These got dropped from the most recent migration rollup
grant dekaf to authenticator;
alter role dekaf nologin bypassrls;

commit;
