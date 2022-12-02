// Generated from collection schema examples/citi-bike/csv-ride.schema.yaml.
// Referenced from examples/citi-bike/csv-rides.flow.yaml#/collections/examples~1citi-bike~1csv-rides.
export type Document = {
    'Bike ID': string;
    'Birth Year': string | null;
    'End Station ID': string;
    'End Station Latitude': string;
    'End Station Longitude': string;
    'End Station Name': string;
    Gender: string;
    'Start Station ID': string;
    'Start Station Latitude': string;
    'Start Station Longitude': string;
    'Start Station Name': string;
    'Start Time': string;
    'Stop Time': string;
    'Trip Duration': string;
    'User Type': string | null;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
