package bike_share

// CreateTableStmt bootstraps an embedded SQLite database with the "rides" table.
const CreateTableStmt = `
CREATE TABLE IF NOT EXISTS rides
(
    uuid                    BLOB        NOT NULL,
    trip_duration           INTEGER     NOT NULL,
    bike_id                 INTEGER     NOT NULL,
    user_type               VARCHAR(16) NOT NULL,
    birth_year              SMALLINT    NOT NULL,
    gender                  SMALLINT    NOT NULL,

    start_time              TIMESTAMP   NOT NULL,
    start_station_id        INTEGER     NOT NULL,
    start_station_name      VARCHAR(64) NOT NULL,
    start_station_latitude  REAL        NOT NULL,
    start_station_longitude REAL        NOT NULL,

    end_time               TIMESTAMP   NOT NULL,
    end_station_id         INTEGER     NOT NULL,
    end_station_name       VARCHAR(64) NOT NULL,
    end_station_latitude   REAL        NOT NULL,
    end_station_longitude  REAL        NOT NULL,

    PRIMARY KEY (bike_id, start_time)
);
`

// InsertStmt inserts a CSV bike-share input record into the "rides" table.
const InsertStmt = `
INSERT INTO rides
(
	uuid,
	trip_duration,
	start_time,
	end_time,
	start_station_id,
	start_station_name,
	start_station_latitude,
	start_station_longitude,
	end_station_id,
	end_station_name,
	end_station_latitude,
	end_station_longitude,
	bike_id,
	user_type,
	birth_year,
	gender
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16);
`

// WindowStmt windows a bike ID ($1) to the $2 most-recent rides.
const WindowStmt = `
WITH keep AS (
	SELECT uuid FROM rides WHERE bike_id = $1 ORDER BY start_time DESC limit $2
)
DELETE FROM rides WHERE bike_id = $1 AND uuid NOT IN (SELECT uuid FROM keep);
`

// QueryCycleStmt returns a path taken by a bike ID ($1) of at least length $2,
// such that the path starts and ends at the bike's final station but does not
// visit it in between, and where the bike is not relocated between rides.
const QueryCycleStmt = `
WITH 
	-- Path is a recursive common table expression which iteratively builds
	-- the path this bike took, ending at it's terminus station.
	RECURSIVE path(ind, ts, id, name) AS (
        SELECT * FROM terminus
        UNION ALL
        -- Iterate backwards to extend our path.
        SELECT p.ind, p.start_time, p.start_id, p.start_name
        FROM ordered_rides AS p INNER JOIN path AS n
        -- Examine the previous ride of the bike.
        ON p.ind = n.ind + 1
        -- Filter cases where the bike was relocated between rides.
        AND p.end_id = n.id
        -- Stop iterating upon reaching our terminus station.
        AND (p.ind = 1 OR p.end_id NOT IN (SELECT id FROM terminus))
    ),
    -- ordered_rides filters and indexes to rides of this bike ordered on time.
    ordered_rides AS (
        SELECT ROW_NUMBER() OVER (ORDER BY start_time DESC) AS ind,
               start_time,
               end_time,
               start_station_id   AS start_id,
               end_station_id     AS end_id,
               start_station_name AS start_name,
               end_station_name   AS end_name
        FROM rides
        WHERE bike_id = $1
    ),
    -- Terminus is the final ride & station of this bike.
    terminus AS (
        SELECT ind - 1 AS ind, end_time, end_id AS id, end_name
        FROM ordered_rides WHERE ind = 1
    )
SELECT $1, ts, name FROM (
	SELECT ind, ts, name, COUNT(*) OVER () - 1 AS path_length,
      	SUM(CASE WHEN id IN (SELECT id FROM terminus) THEN 1 ELSE 0 END) OVER () = 2 AS is_cycle
      	FROM path) AS sub
WHERE is_cycle AND path_length > $2
ORDER BY ind DESC;
`

// QueryHistoryStmt retrieves the current window of rides of the given bike ($1).
// It powers the ServeBikeHistory API.
const QueryHistoryStmt = `
SELECT uuid, start_time, end_time, start_station_name, end_station_name
FROM rides WHERE bike_id = $1
`
