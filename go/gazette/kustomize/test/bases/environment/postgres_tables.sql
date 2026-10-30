-- Table gazette_checkpoints is expected by consumer.SQLStore for its use in
-- persisting consumer transaction checkpoints. It's created just once per
-- database, and is shared by all consumer applications and shards which use it.
CREATE TABLE gazette_checkpoints
(
    shard_fqn  TEXT PRIMARY KEY NOT NULL,
    fence      INTEGER          NOT NULL,
    checkpoint BYTEA            NOT NULL
);

-- Table rides captures a bike-share datum with its Gazette UUID.
CREATE TABLE rides
(
    uuid                    UUID        NOT NULL,
    trip_duration           INTERVAL    NOT NULL,
    bike_id                 INTEGER     NOT NULL,
    user_type               VARCHAR(16) NOT NULL,
    birth_year              SMALLINT    NOT NULL,
    gender                  SMALLINT    NOT NULL,

    start_time              TIMESTAMP   NOT NULL,
    start_station_id        INTEGER     NOT NULL,
    start_station_name      VARCHAR(64) NOT NULL,
    start_station_latitude  REAL        NOT NULL,
    start_station_longitude REAL        NOT NULL,

    end_time                TIMESTAMP   NOT NULL,
    end_station_id          INTEGER     NOT NULL,
    end_station_name        VARCHAR(64) NOT NULL,
    end_station_latitude    REAL        NOT NULL,
    end_station_longitude   REAL        NOT NULL,

    PRIMARY KEY (bike_id, start_time)
);
