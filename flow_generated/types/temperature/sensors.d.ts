// Generated from collection schema examples/temp-sensors/schemas.yaml#/$defs/sensor.
// Referenced from examples/temp-sensors/flow.yaml#/collections/temperature~1sensors.
export type Document = /* A sensor that produces temperature readings */ {
    id: /* The unique id of this sensor */ number;
    location?: /* GeoJSON Point The precise geographic location of the sensor */ {
        bbox?: number[];
        coordinates: number[];
        type: 'Point';
    };
    locationName?: /* Human readable name of the sensor location */ string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
