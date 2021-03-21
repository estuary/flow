import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/temp-sensors/flow.yaml#/collections/temperature~1average-by-location/derivation.
export class TemperatureAverageByLocation implements interfaces.TemperatureAverageByLocation {

    // Updates the register value for each reading document.
    readingsUpdate(
        source: collections.TemperatureReadings,
    ): registers.TemperatureAverageByLocation[] {
      // When updating the register, these values will be 'reduced' into the current value of the
      // register according to annotations on the register schema.
      return [{
        // Will increment the number of readings by 1, since that field uses a `sum` strategy
        numReadings: 1,
        // Will add this reading's value to `totalC` since that field also uses a `sum` strategy
        totalC: source.tempC,
        // Since this field has a `maximize` strategy, `lastReading` will only be modified if
        // `source.timestamp` is more recent than the current value.
        lastReading: source.timestamp,

        // The minimize and maximize reduction strategies will take care of determining the final
        // values by comparing the values in the register against these ones.
        minTempC: source.tempC,
        maxTempC: source.tempC,
      }]
    }

    // For each reading, publish an update to the average temperature for the location.
    readingsPublish(
        source: collections.TemperatureReadings,
        register: registers.TemperatureAverageByLocation,
        _previous: registers.TemperatureAverageByLocation,
    ): collections.TemperatureAverageByLocation[] {
      return [publishAvgTemp(source.sensorId, register)]
    }

    // sensorsUpdate updates the register state for a given sensor
    sensorsUpdate(
        source: collections.TemperatureSensors,
    ): registers.TemperatureAverageByLocation[] {
      // Updates the register with the location name, but does not add to the number of readings
      return [{ locationName: source.locationName }]
    }

    // sensorPublish may publish a document when the sensor is updated. This will allow updates to
    // sensor locationNames to be plumbed through.
    sensorsPublish(
        source: collections.TemperatureSensors,
        register: registers.TemperatureAverageByLocation,
        previous: registers.TemperatureAverageByLocation,
    ): collections.TemperatureAverageByLocation[] {
      // Only publish a document if the locationName has changed AND there's been prior readings from
      // this sensor. This is just an optimization, since we know that locationName is the only field
      // we're populating from the sensor record. You could remove this conditional and always
      // publish a new document, and you'd get exactly the same end result (just with an additional
      // document in your cloud storage bucket).
      if (register.locationName === previous.locationName || !register.lastReading) {
        return []
      } else {
        return [publishAvgTemp(source.id, register)]
      }
    }
}

// This function is called from both `readingsPublish` and `sensorsPublish` to produce the joined
// and aggregated result document.
function publishAvgTemp(sensorId: number, register: registers.TemperatureAverageByLocation):
  collections.TemperatureAverageByLocation {

  // Calculate the average, rounding to 2 decimal places
  // The ! at the ends of expressions here are a non-null assertions, because the register schema
  // does not require these properties to be present. We know these to be non-null because this
  // function is only called if we've seen at least one reading. An alternative would be to provide
  // default values for the `initial` register value.
  var avg = Math.round(register.totalC! / register.numReadings! * 100) / 100.0
  return {
    sensorId: sensorId,
    locationName: register.locationName || null,
    averageTempC: avg,
    minTempC: register.minTempC!,
    maxTempC: register.maxTempC!,
    lastReading: register.lastReading!,
  }
}
