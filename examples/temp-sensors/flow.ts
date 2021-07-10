import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/temp-sensors/flow.yaml#/collections/temperature~1averages/derivation.
export class TemperatureAverages implements interfaces.TemperatureAverages {
    fromReadingsPublish(
        source: collections.TemperatureReadings,
        _register: registers.TemperatureAverages,
        _previous: registers.TemperatureAverages,
    ): collections.TemperatureAverages[] {
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
    fromSensorsPublish(
        source: collections.TemperatureSensors,
        _register: registers.TemperatureAverages,
        _previous: registers.TemperatureAverages,
    ): collections.TemperatureAverages[] {
        return [{ sensor: source }];
    }
}
