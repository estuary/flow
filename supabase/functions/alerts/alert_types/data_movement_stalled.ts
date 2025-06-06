import { isFinite } from "npm:lodash";

import { AlertRecord, EmailConfig } from "../index.ts";
import { commonTemplate } from "../template.ts";
import { Recipient } from "../template.ts";

interface DataMovementStalledArguments {
    bytes_processed: number;
    recipients: Recipient[];
    evaluation_interval: string;
    spec_type: string;
}

type DataMovementStalledRecord = AlertRecord<"data_movement_stalled", DataMovementStalledArguments>;

const getTaskDetailsPageURL = (catalogName: string, specType: string) =>
    `https://dashboard.estuary.dev/${specType}s/details/overview?catalogName=${catalogName}`;

const formatAlertEmail = ({
    arguments: { recipients, evaluation_interval, spec_type },
    catalog_name,
}: DataMovementStalledRecord): EmailConfig[] => {
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

    return recipients.map((recipient) => ({
        content: commonTemplate(
            `
                <mj-text>
                    You are receiving this alert because your task, ${spec_type} <a class="identifier">${catalog_name}</a> hasn't seen new data in ${formattedEvaluationInterval}.  You can locate your task <a href="${detailsPageURL}" target="_blank" rel="noopener">here</a> to make changes or update its alerting settings.
                </mj-text>
            `,
            recipient,
        ),
        subject,
        emails: [recipient.email],
    }));
};

const formatConfirmationEmail = ({
    arguments: { recipients, spec_type },
    catalog_name,
}: DataMovementStalledRecord): EmailConfig[] => {
    const subject = `Estuary Flow: Alert resolved for ${spec_type} ${catalog_name}`;

    const detailsPageURL = getTaskDetailsPageURL(catalog_name, spec_type);

    return recipients.map((recipient) => ({
        content: commonTemplate(
            `
        <mj-text>
            You are receiving this notice because a previous alert for your task, ${spec_type} <a class="identifier">${catalog_name}</a>, has now resolved.  You can locate your task <a href="${detailsPageURL}" target="_blank" rel="noopener">here</a> to make changes or update its alerting settings.
        </mj-text>
    `,
            recipient,
        ),
        subject,
        emails: [recipient.email],
    }));
};

export const dataMovementStalledEmail = (request: DataMovementStalledRecord): EmailConfig[] => {
    if (request.resolved_at) {
        return formatConfirmationEmail(request);
    } else {
        return formatAlertEmail(request);
    }
};
