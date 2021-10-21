import { collections, interfaces, registers } from 'flow/modules';
import { geoToH3, kRing } from 'h3-js';
import { getDistance } from 'geolib';
import * as moment from 'moment';

// Implementation for derivation vessels.flow.yaml#/collections/examples~1ais~1close_vessel_pairs/derivation.
export class ExamplesAisCloseVesselPairs implements interfaces.ExamplesAisCloseVesselPairs {
    closeVesselDetectionUpdate(
        source: collections.ExamplesAisVesselMovements,
    ): registers.ExamplesAisCloseVesselPairs[] {
        switch (source.action) {
            case 'closeTo':
                return [{ add: [source.vessel] }];
            case 'distantFrom':
                return [{ remove: [source.vessel] }];
            default: {
                return [];
            }
        }
    }
    closeVesselDetectionPublish(
        source: collections.ExamplesAisVesselMovements,
        register: registers.ExamplesAisCloseVesselPairs,
        _previous: registers.ExamplesAisCloseVesselPairs,
    ): collections.ExamplesAisCloseVesselPairs[] {
        const outputs: collections.ExamplesAisCloseVesselPairs[] = [];
        if (source.action != 'enter') {
            return [];
        }

        const a = source.vessel;

        for (const b of register.add) {
            if (a.mmsi == b.mmsi) continue;
            const distance = getDistance({ latitude: a.lat, longitude: a.lon }, { latitude: b.lat, longitude: b.lon });

            const ma = moment(a.timestamp, 'YYYY-MM-DDTHH:mm:ss');
            const mb = moment(b.timestamp, 'YYYY-MM-DDTHH:mm:ss');
            const time_difference = Math.abs(moment.duration(mb.diff(ma)).asMinutes());

            // TODO: set threshold check on distance / time_difference

            outputs.push({
                vessel_a: a,
                vessel_b: b,
                spatial_distance_in_meters: Math.round(distance),
                time_difference_in_minutes: Math.round(time_difference),
            });
        }
        return outputs;
    }
}

// Implementation for derivation vessels.flow.yaml#/collections/examples~1ais~1vessel_movements/derivation.
export class ExamplesAisVesselMovements implements interfaces.ExamplesAisVesselMovements {
    vesselMovementUpdate(source: collections.ExamplesAisVessels): registers.ExamplesAisVesselMovements[] {
        // H3 resolution table https://h3geo.org/docs/core-library/restable/
        return [
            {
                h3_index: geoToH3(source.lat, source.lon, 8),
            },
        ];
    }
    vesselMovementPublish(
        source: collections.ExamplesAisVessels,
        register: registers.ExamplesAisVesselMovements,
        previous: registers.ExamplesAisVesselMovements,
    ): collections.ExamplesAisVesselMovements[] {
        const outputs: collections.ExamplesAisVesselMovements[] = [];

        const cur_h3_index = register.h3_index;
        const prev_h3_index = previous.h3_index;

        // only consider 1-hop neighbors.
        const cur_close_to = kRing(cur_h3_index, 1).concat([cur_h3_index]);
        const prev_close_to = prev_h3_index == '' ? [] : kRing(prev_h3_index, 1).concat([prev_h3_index]);
        const distant_from = prev_close_to.filter((item) => cur_close_to.indexOf(item) < 0);

        for (const h3_index of distant_from) {
            outputs.push({
                h3_index: h3_index,
                vessel: source,
                action: 'distantFrom',
            });
        }

        // It is inefficient to update all Hexagons that are close to the vessel.
        // This is needed for updating vessel information in the index.
        // We are trading efficiency for simplicity for demo purposes.
        // The efficiency can be improved by separating the vessel information from the index.
        for (const h3_index of cur_close_to) {
            outputs.push({
                h3_index: h3_index,
                vessel: source,
                action: 'closeTo',
            });
        }

        if (cur_h3_index != prev_h3_index) {
            outputs.push({
                h3_index: cur_h3_index,
                vessel: source,
                action: 'enter',
            });

            if (prev_h3_index != '') {
                outputs.push({
                    h3_index: prev_h3_index,
                    vessel: source,
                    action: 'leave',
                });
            }
        }

        return outputs;
    }
}
