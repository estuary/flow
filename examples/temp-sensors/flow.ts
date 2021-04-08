import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/temp-sensors/flow.yaml#/collections/temperature~1averageByLocation/derivation.
export class TemperatureAverageByLocation implements interfaces.TemperatureAverageByLocation {
    avgTempLocationSensorsUpdate(source: collections.TemperatureSensors): registers.TemperatureAverageByLocation[] {
        // Update the register when a new location arrives.
        return [{ locationName: source.locationName }];
    }
    avgTempLocationSensorsPublish(
        source: collections.TemperatureSensors,
        register: registers.TemperatureAverageByLocation,
        _previous: registers.TemperatureAverageByLocation,
    ): collections.TemperatureAverageByLocation[] {
        // If we have a reading for a new location, update the collection.  Else, don't update it
        // but keep it around in the register for when a reading arrives.
        if (register.numReadings && register.totalC) {
            const avg = Math.round((register.totalC / register.numReadings) * 100) / 100.0;
            return [
                {
                    sensorId: source.id,
                    numReadings: register.numReadings,
                    avgC: avg,
                    totalC: register.totalC,
                    minTempC: register.minTempC,
                    maxTempC: register.maxTempC,
                    lastReading: register.lastReading,
                    locationName: source.locationName,
                },
            ];
        } else {
            return [];
        }
    }
    avgTempLocationTempsUpdate(source: collections.TemperatureAverageTemps): registers.TemperatureAverageByLocation[] {
        // Update the register with stats when a new reading comes in.  This can be used later
        // if a location arrives in for this sensor to ensure a fully reactive join.
        return [
            {
                numReadings: source.numReadings,
                totalC: source.totalC,
                minTempC: source.minTempC,
                maxTempC: source.maxTempC,
                lastReading: source.lastReading,
            },
        ];
    }
    avgTempLocationTempsPublish(
        source: collections.TemperatureAverageTemps,
        register: registers.TemperatureAverageByLocation,
        _previous: registers.TemperatureAverageByLocation,
    ): collections.TemperatureAverageByLocation[] {
        const avg = Math.round((source.totalC / source.numReadings) * 100) / 100.0;
        // Always update the collection when a new reading comes in.
        return [
            {
                sensorId: source.sensorId,
                numReadings: source.numReadings,
                avgC: avg,
                totalC: source.totalC,
                minTempC: source.minTempC,
                maxTempC: source.maxTempC,
                lastReading: source.lastReading,
                locationName: register.locationName,
            },
        ];
    }
}

// Implementation for derivation examples/temp-sensors/flow.yaml#/collections/temperature~1averageTemps/derivation.
export class TemperatureAverageTemps implements interfaces.TemperatureAverageTemps {
    averageTempsPublish(
        source: collections.TemperatureReadings,
        _register: registers.TemperatureAverageTemps,
        _previous: registers.TemperatureAverageTemps,
    ): collections.TemperatureAverageTemps[] {
        // This will execute on every reading so by setting numReadings to 1 for a single document, we'll sum the number of documents correctly by using
        // reduction annotations. Reduction annotations will handle ensuring temps and other fields are correctly minimized, maximized, or summed.
        return [
            {
                sensorId: source.sensorId,
                numReadings: 1,
                totalC: source.tempC,
                minTempC: source.tempC,
                maxTempC: source.tempC,
                lastReading: source.timestamp,
            },
        ];
    }
}
