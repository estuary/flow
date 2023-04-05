
// Generated for published documents of derived collection examples/citi-bike/stations.
export type Document = /* A Citi Bike Station */ {
    arrival?: /* Statistics on Bike arrivals to the station */ {
        move?: /* Bikes moved to the station */ number;
        ride?: /* Bikes ridden to the station */ number;
    };
    departure?: /* Statistics on Bike departures from the station */ {
        move?: /* Bikes moved from the station */ number;
        ride?: /* Bikes ridden from the station */ number;
    };
    geo?: /* Location of this station Geographic Location as Latitude & Longitude */ {
        latitude: number;
        longitude: number;
    };
    id: /* Unique identifier for this station */ number;
    name: /* Human-friendly name of this station */ string;
    stable?: /* Set of Bike IDs which are currently at this station */ {
        [k: string]: number[];
    };
};


// Generated for read documents of sourced collection examples/citi-bike/rides-and-relocations.
export type SourceRidesAndMoves = /* Ride within the Citi Bike system */ {
    begin: /* Starting point of the trip Station and time at which a trip began or ended */ {
        station: /* A Citi Bike Station */ {
            geo?: /* Location of this station Geographic Location as Latitude & Longitude */ {
                latitude: number;
                longitude: number;
            };
            id: /* Unique identifier for this station */ number;
            name: /* Human-friendly name of this station */ string;
        };
        timestamp: /* Timestamp as YYYY-MM-DD HH:MM:SS.F in UTC */ string;
    };
    bike_id: /* Unique identifier for this bike */ number;
    birth_year?: /* Birth year of the rider */ number | null;
    duration_seconds?: /* Duration of the trip, in seconds */ number;
    end: /* Ending point of the trip Station and time at which a trip began or ended */ {
        station: /* A Citi Bike Station */ {
            geo?: /* Location of this station Geographic Location as Latitude & Longitude */ {
                latitude: number;
                longitude: number;
            };
            id: /* Unique identifier for this station */ number;
            name: /* Human-friendly name of this station */ string;
        };
        timestamp: /* Timestamp as YYYY-MM-DD HH:MM:SS.F in UTC */ string;
    };
    gender?: /* Gender of the rider (Zero=unknown; 1=male; 2=female) */ 0 | 1 | 2;
    relocation?: true;
    user_type?: /* Subscriber, or pay-as-you-go Customer */ null | "Customer" | "Subscriber";
};


export abstract class IDerivation {
    // Construct a new Derivation instance from a Request.Open message.
    constructor(_open: { state: unknown }) { }

    // flush awaits any remaining documents to be published and returns them.
    // deno-lint-ignore require-await
    async flush(): Promise<Document[]> {
        return [];
    }

    // startCommit is notified of a runtime commit in progress, and returns an optional
    // connector state update to be committed.
    startCommit(_startCommit: { runtimeCheckpoint: unknown }): { state?: { updated: unknown, mergePatch: boolean } } {
        return {};
    }

    abstract ridesAndMoves(source: { doc: SourceRidesAndMoves }): Document[];
}
