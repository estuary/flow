# Citi Bike System Data

This example uses Estuary to capture and process Citi Bike [system data][].
The dataset is available in the S3 [tripdata][] bucket as compressed CSV files
of each ride taken within the system, by month.

[system data]: https://www.citibikenyc.com/system-data
[tripdata]: https://s3.amazonaws.com/tripdata/index.html

The source data represents individual customer rides taken within the Citi Bike system.
From this granular data we'll build:

-   A query-able view which provides the last station a given bike was seen at.
-   An understanding of when a bike is relocated from one station to another.
-   A query-able view of station statistics, including the bikes which are currently at the station.
-   Alerts when a bike is "idle" at a station for a prolonged period, indicating it may be broken and need repair.

## Modeling Rides

A ride document has the following shape:

```json
{
    "bike_id": 26396,
    "duration_seconds": 1193,
    "user_type": "Customer",
    "gender": 0,
    "birth_year": 1969,
    "begin": {
        "station": {
            "geo": {
                "latitude": 40.711863,
                "longitude": -73.944024
            },
            "id": 3081,
            "name": "Graham Ave & Grand St"
        },
        "timestamp": "2020-09-01 00:00:12.2020"
    },
    "end": {
        "station": {
            "geo": {
                "latitude": 40.68402,
                "longitude": -73.94977
            },
            "id": 3048,
            "name": "Putnam Ave & Nostrand Ave"
        },
        "timestamp": "2020-09-01 00:20:05.5470"
    }
}
```

## Ride Schema

See [ride.schema.yaml](ride.schema.yaml) for the JSON schema of ride documents.

A few things about it to point out:

It defines the _shape_ that documents can take.

-   A "ride" document must have a _bike_id_, _begin_, and _end_.
-   A "location" must have a _latitude_, _longitude_, and so on.
-   The `$ref` keyword makes it easy to re-use common structures.

_Validations_ constrain the types and values that documents can take.
A "longitude" must be a number and fall within the expected range, and "gender"
must be a value within the expected enumeration. Some properties are `required`,
while others are optional. Estuary enforces that all documents of a collection
must validate against its schema before they can be added.

Estuary is also able to _translate_ many schema constraints (e.g. "/begin/station/id
must exist and be an integer") into other kinds of schema -- like TypeScript types
and SQL constraints -- which promotes end-to-end type safety and a better
development experience.

_Annotations_ attach information to locations within the document.
`title` and `description` keywords give color to locations of the document.
They're machine-accessible documentation -- which makes it possible to re-use
these annotations in transformed versions of the schema.

## Capturing Rides

To work with ride events, first we need to define a collection into which we'll
ingest them. Simple enough, but a wrinkle is that the source dataset is
CSV files, using header names which don't match our schema:

```console
$ wget https://s3.amazonaws.com/tripdata/202009-citibike-tripdata.csv.zip
$ unzip -p 202009-citibike-tripdata.csv.zip | head -5
"tripduration","starttime","stoptime","start station id","start station name","start station latitude","start station longitude","end station id","end station name","end station latitude","end station longitude","bikeid","usertype","birth year","gender"
4225,"2020-09-01 00:00:01.0430","2020-09-01 01:10:26.6350",3508,"St Nicholas Ave & Manhattan Ave",40.809725,-73.953149,116,"W 17 St & 8 Ave",40.74177603,-74.00149746,44317,"Customer",1979,1
1868,"2020-09-01 00:00:04.8320","2020-09-01 00:31:13.7650",3621,"27 Ave & 9 St",40.7739825,-73.9309134,3094,"Graham Ave & Withers St",40.7169811,-73.94485918,37793,"Customer",1991,1
1097,"2020-09-01 00:00:06.8990","2020-09-01 00:18:24.2260",3492,"E 118 St & Park Ave",40.8005385,-73.9419949,3959,"Edgecombe Ave & W 145 St",40.823498,-73.94386,41438,"Subscriber",1984,1
1473,"2020-09-01 00:00:07.7440","2020-09-01 00:24:41.1800",3946,"St Nicholas Ave & W 137 St",40.818477,-73.947568,4002,"W 144 St & Adam Clayton Powell Blvd",40.820877,-73.939249,35860,"Customer",1990,2
```

_Projections_ let us account for this, by defining a mapping between
document locations (as [JSON Pointers][]) and corresponding fields
in a flattened, table-based representation such as a CSV file or SQL table.
They're used whenever Estuary is capturing from or materializing into
table-like systems.

[json pointers]: https://docs.opis.io/json-schema/1.x/pointers.html

[rides.flow.yaml](rides.flow.yaml) defines the collection into which rides are ingested,
and its declared projections map CSV headers in the source data to document locations.

```console
# Start a local development instance, and leave it running:
$ flowctl-go develop --port 8080

# Begin loading rides into the development instance:
$ examples/citi-bike/load-rides.sh
```

## Last-Seen Station of a Bike

[last-seen.flow.yaml](last-seen.flow.yaml) is a derivation that derives,
for each bike, the station it last arrived at. It's materialized into
`citi_last_seen` in a `materialize-sqlite` instance.

```console
$ docker ps | grep materialize-sqlite # find the docker container name or id
$ docker exec -it <container-name> sqlite3 /tmp/sqlite.db 'select bike_id, "last/station/name", "last/timestamp" from last_seen limit 10
```

The materialization updates continuously as bikes move around the system.

## Bike Relocations

Citi Bike will sometimes redistribute bikes between stations, when a station gets
too full or empty. These relocations show up as "holes" in the ride data,
where a bike mysteriously ends a ride at one station and starts its next ride at
a different station.

[rides-and-relocations.flow.yaml](rides-and-relocations.flow.yaml) enriches
ride documents by tracking prior bike locations in a register, and then
supplementing rides with detected relocations.

Use `gazctl` to observe relocation events, as they're derived:

```console
$ gazctl journals read --block -l estuary.dev/collection=examples/citi-bike/rides-and-relocations \
 | jq -c '. | select(.relocation)'
```

## Station Status

Suppose we're building a station status API. We're bringing together some basic statistics
about each station, like the number of bikes which have arrived, departed, and been relocated
in or out. We also need to know which bikes are currently at each station.

To accomplish this, we'll build a collection keyed on station IDs into which we'll derive
documents that update our station status. However, we need to tell Estuary how to _reduce_
these updates into a full view of a station's status, by adding `reduce` annotations
into our schema. [station.schema.yaml](station.schema.yaml) is the complete schema for
our station status collection.

Estuary uses reduce annotations to build general "combiners" (in the map/reduce sense) over
documents of a given schema. Those combiners are employed automatically by Estuary.

Now we define our derivation. Since Estuary is handling reductions for us, our remaining
responsibility is to implement the "mapper" function which will transform source
events into status status updates. See [stations.flow.yaml](stations.flow.yaml).

Stations are materialized into table `citi_stations` in the `example.db`.

```sql
-- Current bikes at each station.
select id, name, stable from stations order by name asc limit 10;
-- Station arrivals and departures.
select id, name, "arrival/ride", "departure/ride", "arrival/move", "departure/move"
from stations order by name asc limit 10;
```

## Idle Bikes

We're next tasked with identifying when bikes have sat idle at a station for an extended
period of time. This is a potential signal that something is wrong with the bike,
and customers are avoiding it.

Event-driven systems usually aren't terribly good at detecting when things _haven't_
happened. At this point, an engineer will often reach for a task scheduler like Airflow,
and set up a job that takes periodic snapshots of bike locations, and compares them to
find ones which haven't changed.

Estuary offers a simpler approach, which is to join the rides collection with itself,
using a _read delay_. [idle-bikes.flow.yaml](idle-bikes.flow.yaml) demonstrates this
workflow.

After the read delay has elapsed, we'll start to see events in the "idle-bikes" collection:

```console
$ gazctl journals read --block -l estuary.dev/collection=examples/citi-bike/idle-bikes

```
