import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/re-key/flow.yaml#/collections/examples~1re-key~1stable_events/derivation.
export class ExamplesReKeyStableEvents implements interfaces.ExamplesReKeyStableEvents {
    fromAnonymousEventsUpdate(source: collections.ExamplesReKeyAnonymousEvents): registers.ExamplesReKeyStableEvents[] {
        // Reduce this event into |register.events|. If stable_id is already known,
        // then register.events is null and this is a no-op.
        return [{ events: [source] }];
    }
    fromAnonymousEventsPublish(
        source: collections.ExamplesReKeyAnonymousEvents,
        register: registers.ExamplesReKeyStableEvents,
        _previous: registers.ExamplesReKeyStableEvents,
    ): collections.ExamplesReKeyStableEvents[] {
        // If the stable ID for this event is known, enrich the source event and publish.
        // Otherwise, we've updated this source event into |register.events| and will
        // publish once its stable ID becomes known.
        if (register.stable_id) {
            return [{ stable_id: register.stable_id, ...source }];
        }
        return [];
    }
    fromIdMappingsUpdate(source: collections.ExamplesReKeyMappings): registers.ExamplesReKeyStableEvents[] {
        // Update the register with the associated stable ID of this anonymous ID.
        // Set events to null, so that future "append" reductions are no-ops.
        return [{ events: null, stable_id: source.stable_id }];
    }
    fromIdMappingsPublish(
        _source: collections.ExamplesReKeyMappings,
        register: registers.ExamplesReKeyStableEvents,
        previous: registers.ExamplesReKeyStableEvents,
    ): collections.ExamplesReKeyStableEvents[] {
        // Publish previous register.events, enriched with the just-learned stable ID.
        let out = [];
        if (register.stable_id && previous.events) {
            for (var event of previous.events) {
                out.push({ stable_id: register.stable_id, ...event });
            }
        }
        return out;
    }
}
