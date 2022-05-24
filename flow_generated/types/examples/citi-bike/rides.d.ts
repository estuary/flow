// Generated from collection schema examples/citi-bike/ride.schema.yaml.
// Referenced from examples/citi-bike/rides.flow.yaml#/collections/examples~1citi-bike~1rides.
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
    user_type?: /* Subscriber, or pay-as-you-go Customer */ null | 'Customer' | 'Subscriber';
};
