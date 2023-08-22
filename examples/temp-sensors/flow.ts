import { IDerivation, Document, SourceFromReadings, SourceFromSensors } from 'flow/temperature/averages.ts';

// Implementation for derivation examples/temp-sensors/flow.yaml#/collections/temperature~1averages/derivation.
export class Derivation extends IDerivation {
    fromReadings(read: {doc: SourceFromReadings}): Document[] {
        const source = read.doc;
        // This will execute on every reading so by setting numReadings to 1 for a single document, we'll sum the number of documents correctly by using
        // reduction annotations. Reduction annotations will handle ensuring temps and other fields are correctly minimized, maximized, or summed.
        return [
            {
                sensor: { id: source.sensorId },
                numReadings: 1,
                totalC: source.tempC,
                minTempC: source.tempC,
                maxTempC: source.tempC,
                lastReading: source.timestamp,
            },
        ];
    }
    fromSensors(read: { doc: SourceFromSensors}): Document[] {
        return [{ sensor: read.doc}];
    }
}
