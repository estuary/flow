// Generated from collection schema examples/temp-sensors/schemas.yaml#/$defs/average.
// Referenced from examples/temp-sensors/flow.yaml#/collections/temperature~1averages.
export type Document = /* Average temperature information for a particular sensor */ {
    lastReading?: /* Timestamp of the most recent reading for this named location */ string;
    maxTempC?: number;
    minTempC?: number;
    numReadings?: number;
    sensor: /* A sensor that produces temperature readings */ {
        id: /* The unique id of this sensor */ number;
        location?: /* GeoJSON Point The precise geographic location of the sensor */ {
            bbox?: number[];
            coordinates: number[];
            type: 'Point';
        };
        locationName?: /* Human readable name of the sensor location */ string;
    };
    totalC?: number;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/temp-sensors/flow.yaml?ptr=/collections/temperature~1averages/derivation/register/schema.
// Referenced from examples/temp-sensors/flow.yaml#/collections/temperature~1averages/derivation.
export type Register = unknown;

// Generated from transform fromReadings as a re-export of collection temperature/readings.
// Referenced from examples/temp-sensors/flow.yaml#/collections/temperature~1averages/derivation/transform/fromReadings."
import { SourceDocument as FromReadingsSource } from './readings';
export { SourceDocument as FromReadingsSource } from './readings';

// Generated from transform fromSensors as a re-export of collection temperature/sensors.
// Referenced from examples/temp-sensors/flow.yaml#/collections/temperature~1averages/derivation/transform/fromSensors."
import { SourceDocument as FromSensorsSource } from './sensors';
export { SourceDocument as FromSensorsSource } from './sensors';

// Generated from derivation examples/temp-sensors/flow.yaml#/collections/temperature~1averages/derivation.
// Required to be implemented by examples/temp-sensors/flow.ts.
export interface IDerivation {
    fromReadingsPublish(source: FromReadingsSource, register: Register, previous: Register): OutputDocument[];
    fromSensorsPublish(source: FromSensorsSource, register: Register, previous: Register): OutputDocument[];
}
