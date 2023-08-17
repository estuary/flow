
// Generated for published documents of derived collection examples/citi-bike/rides.
export type Document = /* Ride within the Citi Bike system */ {
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
    user_type?: /* Subscriber, or pay-as-you-go Customer */ null | "Customer" | "Subscriber";
};


// Generated for read documents of sourced collection examples/citi-bike/csv-rides.
export type SourceFromCsvRides = {
    "Bike ID": string;
    "Birth Year": string | null;
    "End Station ID": string;
    "End Station Latitude": string;
    "End Station Longitude": string;
    "End Station Name": string;
    Gender: string;
    "Start Station ID": string;
    "Start Station Latitude": string;
    "Start Station Longitude": string;
    "Start Station Name": string;
    "Start Time": string;
    "Stop Time": string;
    "Trip Duration": string;
    "User Type": string | null;
};


export abstract class IDerivation {
    // Construct a new Derivation instance from a Request.Open message.
    constructor(_open: { state: unknown }) { }

    // flush awaits any remaining documents to be published and returns them.
    // deno-lint-ignore require-await
    async flush(): Promise<Document[]> {
        return [];
    }

    // reset is called only when running catalog tests, and must reset any internal state.
    async reset() { }

    // startCommit is notified of a runtime commit in progress, and returns an optional
    // connector state update to be committed.
    startCommit(_startCommit: { runtimeCheckpoint: unknown }): { state?: { updated: unknown, mergePatch: boolean } } {
        return {};
    }

    abstract fromCsvRides(read: { doc: SourceFromCsvRides }): Document[];
}
