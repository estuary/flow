// Generated from collection schema examples/temp-sensors/schemas.yaml#/$defs/reading.
// Referenced from examples/temp-sensors/flow.yaml#/collections/temperature~1readings.
export type Document = /* A reading of a temperature from a sensor */ {
    sensorId: /* The id of the sensor that produced the reading */ number;
    tempC: /* The temperature in degrees celsius */ number;
    timestamp: /* An RFC-3339 formatted string holding the time of the reading */ string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
