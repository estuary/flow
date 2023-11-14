import { serve } from "https://deno.land/std@0.184.0/http/server.ts";
import { isEmpty, isFinite } from "https://cdn.skypack.dev/lodash";

import { corsHeaders } from "../_shared/cors.ts";
import { returnPostgresError } from "../_shared/helpers.ts";
import { supabaseClient } from "../_shared/supabaseClient.ts";

interface DataProcessingArguments {
    bytes_processed: number;
    emails: string[];
    evaluation_interval: string;
    spec_type: string;
}

interface AlertRecord {
    alert_type: string;
    catalog_name: string;
    fired_at: string;
    resolved_at: string | null;
    arguments: DataProcessingArguments;
}

interface EmailConfig {
    emails: string[];
    subject: string;
    content: string;
}

export const handleSuccess = <T>(response: any) => {
    return response.error
        ? {
            data: null,
            error: response.error,
        }
        : {
            data: response.data as T,
        };
};

export const handleFailure = (error: any) => {
    return {
        data: null,
        error,
    };
};

const dataProcessingAlertType = "data_not_processed_in_interval";

const TABLES = { ALERT_HISTORY: "alert_history" };

const RESEND_API_KEY = "re_Qu4ZevKs_DmDfQxdNmMvoyuSeGtfYz2VS";

const emailNotifications = async (
    pendingNotifications: EmailConfig[],
): Promise<void> => {
    // TODO: Replace hardcoded sender and recipient address with the destructured `emails` property.
    const notificationPromises = pendingNotifications.map(
        ({ emails, content, subject }) =>
            fetch("https://api.resend.com/emails", {
                method: "POST",
                headers: {
                    ...corsHeaders,
                    "Content-Type": "application/json",
                    "Authorization": `Bearer ${RESEND_API_KEY}`,
                },
                body: JSON.stringify({
                    from: "Estuary <onboarding@resend.dev>",
                    to: emails,
                    subject,
                    html: `
                      <div style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;">
                        ${content}

                        <p style="margin-bottom: 0;">Thanks,</p>
                        <p style="margin-top: 0;">Estuary Team</p>
                      </div>
                    `,
                }),
            }),
    );

    await Promise.all(notificationPromises);
};

serve(async (_request: Request): Promise<Response> => {
    const startTimestamp = new Date();
    const minuteOffset = startTimestamp.getUTCMinutes() - 5;

    startTimestamp.setUTCMilliseconds(0);
    startTimestamp.setUTCSeconds(0);
    startTimestamp.setUTCMinutes(minuteOffset);

    const { data: alerts, error: alertsError } = await supabaseClient
        .from<AlertRecord>(TABLES.ALERT_HISTORY)
        .select("*")
        .eq("alert_type", dataProcessingAlertType)
        .is("resolved_at", null)
        .gt("fired_at", startTimestamp.toUTCString());

    if (alertsError !== null) {
        returnPostgresError(alertsError);
    }

    const { data: confirmations, error: confirmationsError } = await supabaseClient
        .from<AlertRecord>(TABLES.ALERT_HISTORY)
        .select("*")
        .eq("alert_type", dataProcessingAlertType)
        .gt("resolved_at", startTimestamp.toUTCString());

    if (confirmationsError !== null) {
        returnPostgresError(confirmationsError);
    }

    if (isEmpty(alerts) && isEmpty(confirmations)) {
        // Terminate the function without error if there aren't any active notifications in the system.
        return new Response(null, {
            headers: { ...corsHeaders, "Content-Type": "application/json" },
            status: 200,
        });
    }

    const pendingAlertEmails: EmailConfig[] = alerts
        ? alerts.map(
            ({
                arguments: { emails, evaluation_interval, spec_type },
                catalog_name,
            }) => {
                let formattedEvaluationInterval = evaluation_interval;

                // A postgresql interval in hour increments has the following format: 'HH:00:00'.
                if (evaluation_interval.includes(":")) {
                    const timeOffset = evaluation_interval.split(":");
                    const hours = Number(timeOffset[0]);

                    formattedEvaluationInterval = isFinite(hours) ? `${hours} hours` : `${timeOffset[0]} hours`;
                }

                const subject = `Estuary Flow: Alert for ${spec_type} ${catalog_name}`;

                const content =
                    `<p>You are receiving this alert because your task, ${spec_type} ${catalog_name} hasn't seen new data in ${formattedEvaluationInterval}.  You can locate your task <a href="https://dashboard.estuary.dev/captures/details/overview?catalogName=${catalog_name}" target="_blank" rel="noopener">here</a> to make changes or update its alerting settings.</p>`;

                return {
                    content,
                    emails,
                    subject,
                };
            },
        )
        : [];

    const pendingConfirmationEmails: EmailConfig[] = confirmations
        ? confirmations.map(
            ({ arguments: { emails, spec_type }, catalog_name }) => {
                const subject = `Estuary Flow: Alert for ${spec_type} ${catalog_name}`;

                const content =
                    `<p>You are receiving this alert because your task, ${spec_type} ${catalog_name} has resumed processing data.  You can locate your task <a href="https://dashboard.estuary.dev/captures/details/overview?catalogName=${catalog_name}" target="_blank" rel="noopener">here</a> to make changes or update its alerting settings.</p>`;

                return {
                    content,
                    emails,
                    subject,
                };
            },
        )
        : [];

    const pendingEmails = [...pendingAlertEmails, ...pendingConfirmationEmails];

    if (pendingEmails.length === 0) {
        return new Response(null, {
            headers: { ...corsHeaders, "Content-Type": "application/json" },
            status: 200,
        });
    }

    await emailNotifications(pendingEmails);

    return new Response(null, {
        status: 200,
        headers: {
            "Content-Type": "application/json",
        },
    });
});
