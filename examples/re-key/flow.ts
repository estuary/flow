import {
    IDerivation,
    Document,
    Register,
    FromAnonymousEventsSource,
    FromIdMappingsSource,
} from 'flow/examples/re-key/stable_events';

// Implementation for derivation examples/re-key/flow.yaml#/collections/examples~1re-key~1stable_events/derivation.
export class Derivation implements IDerivation {
    fromAnonymousEventsUpdate(source: FromAnonymousEventsSource): Register[] {
        // Reduce this event into |register.events|. If stable_id is already known,
        // then register.events is null and this is a no-op.
        return [{ events: [source] }];
    }
    fromAnonymousEventsPublish(source: FromAnonymousEventsSource, register: Register, _previous: Register): Document[] {
        // If the stable ID for this event is known, enrich the source event and publish.
        // Otherwise, we've updated this source event into |register.events| and will
        // publish once its stable ID becomes known.
        if (register.stable_id) {
            return [{ stable_id: register.stable_id, ...source }];
        }
        return [];
    }
    fromIdMappingsUpdate(source: FromIdMappingsSource): Register[] {
        // Update the register with the associated stable ID of this anonymous ID.
        // Set events to null, so that future "append" reductions are no-ops.
        return [{ events: null, stable_id: source.stable_id }];
    }
    fromIdMappingsPublish(_source: FromIdMappingsSource, register: Register, previous: Register): Document[] {
        // Publish previous register.events, enriched with the just-learned stable ID.
        const out = [];
        if (register.stable_id && previous.events) {
            for (const event of previous.events) {
                out.push({ stable_id: register.stable_id, ...event });
            }
        }
        return out;
    }
}
