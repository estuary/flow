import { collections, interfaces, registers } from 'flow/modules';

import * as moment from 'moment';

// Implementation for derivation examples/net-trace/services.flow.yaml#/collections/examples~1net-trace~1services/derivation.
export class ExamplesNetTraceServices implements interfaces.ExamplesNetTraceServices {
    fromPairsPublish(
        source: collections.ExamplesNetTracePairs,
        _register: registers.ExamplesNetTraceServices,
        _previous: registers.ExamplesNetTraceServices,
    ): collections.ExamplesNetTraceServices[] {
        // Use moment.js to deal with oddball timestamp format, and truncate to current date.
        const date = moment(source.timestamp, 'DD/MM/YYYYhh:mm:ss').format('YYYY-MM-DD');
        const out = [];

        if (source.src.port < 1024) {
            source.src.ip = source.src.ip.split('.').slice(0, -1).join('.');
            out.push({
                date: date,
                service: source.src,
                stats: source.fwd,
            });
        }
        if (source.dst.port < 1024) {
            source.dst.ip = source.dst.ip.split('.').slice(0, -1).join('.');
            out.push({
                date: date,
                service: source.dst,
                stats: source.bwd,
            });
        }
        return out;
    }
}
