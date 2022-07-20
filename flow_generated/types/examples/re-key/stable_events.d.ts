// Generated from collection schema examples/re-key/schema.yaml#/$defs/stable_event.
// Referenced from examples/re-key/flow.yaml#/collections/examples~1re-key~1stable_events.
export type Document = /* An event enriched with a stable ID */ {
    anonymous_id: string;
    event_id: string;
    stable_id: string;
};

// Generated from derivation register schema examples/re-key/schema.yaml#/$defs/join_register.
// Referenced from examples/re-key/flow.yaml#/collections/examples~1re-key~1stable_events/derivation.
export type Register = /* Register that's keyed on anonymous ID, which:
  1) Stores anonymous events prior to a stable ID being known, and thereafter
  2) Stores a mapped stable ID for this anonymous ID.
 */ {
    events: /* An interesting event, keyed on an anonymous ID */
    | {
              anonymous_id: string;
              event_id: string;
          }[]
        | null;
    stable_id?: string;
};

// Generated from transform fromAnonymousEvents as a re-export of collection examples/re-key/anonymous_events.
// Referenced from examples/re-key/flow.yaml#/collections/examples~1re-key~1stable_events/derivation/transform/fromAnonymousEvents."
import { Document as FromAnonymousEventsSource } from './anonymous_events';
export { Document as FromAnonymousEventsSource } from './anonymous_events';

// Generated from transform fromIdMappings as a re-export of collection examples/re-key/mappings.
// Referenced from examples/re-key/flow.yaml#/collections/examples~1re-key~1stable_events/derivation/transform/fromIdMappings."
import { Document as FromIdMappingsSource } from './mappings';
export { Document as FromIdMappingsSource } from './mappings';

// Generated from derivation examples/re-key/flow.yaml#/collections/examples~1re-key~1stable_events/derivation.
// Required to be implemented by examples/re-key/flow.ts.
export interface IDerivation {
    fromAnonymousEventsUpdate(source: FromAnonymousEventsSource): Register[];
    fromAnonymousEventsPublish(source: FromAnonymousEventsSource, register: Register, previous: Register): Document[];
    fromIdMappingsUpdate(source: FromIdMappingsSource): Register[];
    fromIdMappingsPublish(source: FromIdMappingsSource, register: Register, previous: Register): Document[];
}
