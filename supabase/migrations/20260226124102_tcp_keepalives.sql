-- The default value for tcp_keepalives_idle is 2 hours, which is effectively
-- the lower bound on how long it takes postgres to notice and close a connection
-- after a client unexpectedly disconnects. That's far too long, especially for our
-- materialization connectors. This migration tunes the tcp keepalive settings to
-- allow detecting dead connections much faster.
alter database postgres set tcp_keepalives_idle = 60;
alter database postgres set tcp_keepalives_interval = 10;
alter database postgres set tcp_keepalives_count = 5;
