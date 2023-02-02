import { IDerivation, Document, Register, NewFactTableSource } from 'flow/yourprefix/wikipedia/user-fact-table';

// Implementation for derivation estuary/public/wikipedia/flow.yaml#/collections/yourprefix~1wikipedia~1user-fact-table/derivation.
export class Derivation implements IDerivation {
    newFactTablePublish(
        source: NewFactTableSource,
        _register: Register,
        _previous: Register,
    ): Document[] {
        let user_id = 0;
        if (typeof source.log_params == "object" && !Array.isArray(source.log_params) && source.log_params.userid != undefined) {
            user_id = source.log_params.userid;
        }

        const [yyyy, mm, dd] = source.meta.dt.split('-');
        const dd2 = dd.substring(0, 2);
        let date = yyyy + '-' + mm + '-' + dd2;

        return [
            {
                userid: user_id,
                count: 1,
                last_updated: date,
            },
        ]
    }
}