
// Generated for published documents of derived collection temperature/averages.
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
            type: "Point";
        };
        locationName?: /* Human readable name of the sensor location */ string;
    };
    totalC?: number;
};


// Generated for read documents of sourced collection temperature/sensors.
export type SourceFromSensors = /* A sensor that produces temperature readings */ {
    id: /* The unique id of this sensor */ number;
    location?: /* GeoJSON Point The precise geographic location of the sensor */ {
        bbox?: number[];
        coordinates: number[];
        type: "Point";
    };
    locationName?: /* Human readable name of the sensor location */ string;
};


// Generated for read documents of sourced collection temperature/readings.
export type SourceFromReadings = /* A reading of a temperature from a sensor */ {
    sensorId: /* The id of the sensor that produced the reading */ number;
    tempC: /* The temperature in degrees celsius */ number;
    timestamp: /* An RFC-3339 formatted string holding the time of the reading */ string;
};


export abstract class IDerivation {
    // Construct a new Derivation instance from a Request.Open message.
    constructor(_open: { state: unknown }) { }

    // flush awaits any remaining documents to be published and returns them.
    // deno-lint-ignore require-await
    async flush(): Promise<Document[]> {
        return [];
    }

    // reset is called only when running catalog tests, and must reset any internal state.
    async reset() { }

    // startCommit is notified of a runtime commit in progress, and returns an optional
    // connector state update to be committed.
    startCommit(_startCommit: { runtimeCheckpoint: unknown }): { state?: { updated: unknown, mergePatch: boolean } } {
        return {};
    }

    abstract fromSensors(read: { doc: SourceFromSensors }): Document[];
    abstract fromReadings(read: { doc: SourceFromReadings }): Document[];
}
