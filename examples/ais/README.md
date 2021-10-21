# Example: AIS vessel data analysis with H3 framework

BOEM and NOAA published important AIS [vessel data online](https://marinecadastre.gov/ais/).

Uber's [H3 spatial index](https://eng.uber.com/h3/), with [js support](https://github.com/uber/h3-js), provides a spatial framework to process these data in real-time.

This example builds Flow collections and derivations on top of H3 to analyze the AIS vessel data. To be more specific, it finds the pairs of vessels that are close to each other in real time, which enables an alerting system to avoid potential collisions.

The example is for a proof of concept that Flow and H3 are a good fit for real-time spatial data processing.

## Flow components
The Example build involves the following components:
- The capture `ais-data-capture` reads AIS data into the Flow system.
- The collection `vessels` hosts raw AIS data from the capture.
- The collection `vessel_movements` is derived from the collection `vessel`.  It records the interactions between the vessels and the hexagon regions defined by H3 framework. E.g. `vessel A enter hexagon region B`, etc.
- The collection `close_vessel_pairs` is derived from the collection `vessel_movements`. It analyzes the vessels in each hexagon region, and reports the paris of vessels that are close to each other.
- The materializations `output` saves the detected results into an Sqlite table.

# How to run the demo

- Download some sample dataset from https://marinecadastre.gov/ais/
- Upload the downloaded dataset to AWS S3, under certain `bucket` and `path-prefix`.
- Modify the `airbyteSource/config` and `resource/stream` for the capture in `vessel.flow.yaml`, and provide all the AWS credentials and S3 config as needed.
- run
```
   flowctl develop --source=flow.yaml
```
- check the results in the sqlite db with table name `close_vessel_pairs`.
