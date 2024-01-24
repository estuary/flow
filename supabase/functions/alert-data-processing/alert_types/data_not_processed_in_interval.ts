import { AlertRecord, EmailConfig } from "../index.ts";
import { isFinite } from "https://cdn.skypack.dev/lodash";
import { commonTemplate } from "../template.ts";

interface DataProcessingArguments {
    bytes_processed: number;
    emails: string[];
    evaluation_interval: string;
    spec_type: string;
}

type DataNotProcessedRecord = AlertRecord<"data_not_processed_in_interval", DataProcessingArguments>;

const getTaskDetailsPageURL = (catalogName: string, specType: string) =>
    `https://dashboard.estuary.dev/${specType}s/details/overview?catalogName=${catalogName}`;

const formatAlertEmail = ({
    arguments: { emails, evaluation_interval, spec_type },
    catalog_name,
}: DataNotProcessedRecord): EmailConfig => {
    let formattedEvaluationInterval = evaluation_interval;

    // A postgresql interval in hour increments has the following format: 'HH:00:00'.
    if (evaluation_interval.includes(":")) {
        const timeOffset = evaluation_interval.split(":");
        const hours = Number(timeOffset[0]);

        // Ideally, an hour-based interval less than ten would be represented by a single digit. To accomplish this,
        // the hour segment of the evaluation interval is selected (i.e., timeOffset[0]) and attempted to be converted to a number.
        // This conditional is a failsafe, in the event the aforementioned conversion fails which would result in the display
        // of two digits for the hour (e.g., 02 hours instead of 2 hours).
        formattedEvaluationInterval = isFinite(hours) ? `${hours} hours` : `${timeOffset[0]} hours`;
    }

    const subject = `Estuary Flow: Alert for ${spec_type} ${catalog_name}`;

    const detailsPageURL = getTaskDetailsPageURL(catalog_name, spec_type);

    const content = commonTemplate(`
        <mj-text font-size="17px" line-height="1.3">
            You are receiving this alert because your task, ${spec_type} <a class="identifier">${catalog_name}</a> hasn't seen new data in ${formattedEvaluationInterval}.  You can locate your task <a href="${detailsPageURL}" target="_blank" rel="noopener">here</a> to make changes or update its alerting settings.
        </mj-text>
    `);

    return {
        content,
        emails,
        subject,
    };
};

const formatConfirmationEmail = ({
    arguments: { emails, spec_type },
    catalog_name,
}: DataNotProcessedRecord): EmailConfig => {
    const subject = `Estuary Flow: Alert resolved for ${spec_type} ${catalog_name}`;

    const detailsPageURL = getTaskDetailsPageURL(catalog_name, spec_type);

    const content = commonTemplate(`
        <mj-text font-size="17px" line-height="1.3">
            You are receiving this notice because a previous alert for your task, ${spec_type} <a class="identifier">${catalog_name}</a>, has now resolved.  You can locate your task <a href="${detailsPageURL}" target="_blank" rel="noopener">here</a> to make changes or update its alerting settings.
        </mj-text>
    `);

    return {
        content,
        emails,
        subject,
    };
};

export const dataNotProcessedInIntervalEmail = (request: DataNotProcessedRecord): EmailConfig[] => {
    if (request.resolved_at) {
        return [formatConfirmationEmail(request)];
    } else {
        return [formatAlertEmail(request)];
    }
};
