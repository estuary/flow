-- Produces 64bit unique, non-sequential identifiers. These are smaller than
-- full UUIDs, but avoid the issues of sequential ids.
--
-- Adapted from: https://rob.conery.io/2014/05/29/a-better-id-generator-for-postgresql/
-- Which itself was inspired by http://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram

CREATE SEQUENCE unique_id_sequence;

CREATE OR REPLACE FUNCTION id_generator(OUT result bigint) AS $$
DECLARE
    our_epoch bigint := 1314220021721;
    seq_id bigint;
    now_millis bigint;
    -- the id of this DB shard, must be set for each
    -- schema shard you have - you could pass this as a parameter too
    shard_id int := 1;
BEGIN
    SELECT nextval('unique_id_sequence') % 1024 INTO seq_id;

    SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO now_millis;
    result := (now_millis - our_epoch) << 23;
    result := result | (shard_id << 10);
    result := result | (seq_id);
END;
$$ LANGUAGE PLPGSQL;
