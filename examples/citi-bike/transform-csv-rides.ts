import { IDerivation, Document, SourceFromCsvRides } from 'flow/examples/citi-bike/rides.ts';

// Implementation for derivation examples/citi-bike/rides.
export class Derivation extends IDerivation {
    fromCsvRides(read: { doc: SourceFromCsvRides }): Document[] {
        const source = read.doc;

        let birth_year: number | null = null;
        if (source['Birth Year']) {
            birth_year = parseInt(source['Birth Year']);
        }
        let gender: 0 | 1 | 2 = 0;
        if (source['Gender']) {
            switch (parseInt(source['Gender'])) {
                case 1:
                    gender = 1;
                    break;
                case 2:
                    gender = 2;
                    break;
                default:
                    gender = 0;
            }
        }
        let user_type: null | 'Customer' | 'Subscriber' = null;
        switch (source['User Type']) {
            case 'Customer':
                user_type = 'Customer';
                break;
            case 'Subscriber':
                user_type = 'Subscriber';
                break;
            default:
                user_type = null;
        }
        return [
            {
                bike_id: parseInt(source['Bike ID']),
                birth_year: birth_year,
                duration_seconds: parseFloat(source['Trip Duration']),
                gender: gender,
                user_type: user_type,
                begin: {
                    station: {
                        geo: {
                            latitude: parseFloat(source['Start Station Latitude']),
                            longitude: parseFloat(source['Start Station Longitude']),
                        },
                        id: parseInt(source['Start Station ID']),
                        name: source['Start Station Name'],
                    },
                    timestamp: source['Start Time'],
                },
                end: {
                    station: {
                        geo: {
                            latitude: parseFloat(source['End Station Latitude']),
                            longitude: parseFloat(source['End Station Longitude']),
                        },
                        id: parseInt(source['End Station ID']),
                        name: source['End Station Name'],
                    },
                    timestamp: source['Stop Time'],
                },
            },
        ];
    }
}
