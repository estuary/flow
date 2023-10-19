import { serve } from "https://deno.land/std@0.184.0/http/server.ts";
import { isFinite } from "https://cdn.skypack.dev/lodash";

import { corsHeaders } from "../_shared/cors.ts";
import { returnPostgresError } from "../_shared/helpers.ts";
import { supabaseClient } from "../_shared/supabaseClient.ts";

interface DataProcessingNotification {
    live_spec_id: string;
    acknowledged: boolean;
    evaluation_interval: string;
}

interface DataProcessingNotificationExt {
    live_spec_id: string;
    acknowledged: boolean;
    evaluation_interval: string;
    notification_title: string;
    notification_message: string;
    confirmation_title: string;
    confirmation_message: string;
    classification: string | null;
    verified_email: string;
    catalog_name: string;
    spec_type: string;
    bytes_processed: number;
}

interface EmailConfig {
    live_spec_id: string;
    emails: string[];
    subject: string;
    html: string;
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

const TABLES = {
    DATA_PROCESSING_NOTIFICATIONS: "data_processing_notifications",
    DATA_PROCESSING_NOTIFICATIONS_EXT: "data_processing_notifications_ext",
};

const RESEND_API_KEY = "re_Qu4ZevKs_DmDfQxdNmMvoyuSeGtfYz2VS";

const emailNotifications = async (
    pendingNotifications: EmailConfig[],
): Promise<string[]> => {
    const notificationsDelivered: string[] = [];

    // TODO: Replace hardcoded sender and recipient address with the destructured `emails` property.
    const notificationPromises = pendingNotifications.map(
        ({ live_spec_id, emails, subject, html }) =>
            fetch("https://api.resend.com/emails", {
                method: "POST",
                headers: {
                    ...corsHeaders,
                    "Content-Type": "application/json",
                    "Authorization": `Bearer ${RESEND_API_KEY}`,
                },
                body: JSON.stringify({
                    from: "Estuary <onboarding@resend.dev>",
                    to: ["tucker.kiahna@gmail.com"],
                    subject,
                    html,
                }),
            }).then(
                (response) => {
                    if (response.ok) {
                        notificationsDelivered.push(live_spec_id);
                    }
                },
                () => {},
            ),
    );

    await Promise.all(notificationPromises);

    return notificationsDelivered;
};

const updateAcknowledgementFlag = async (
    alertEmailsDelivered: string[],
    confirmationEmailsDelivered: string[],
) => {
    const alertUpdates = alertEmailsDelivered.map((liveSpecId) =>
        supabaseClient
            .from<DataProcessingNotification>(
                TABLES.DATA_PROCESSING_NOTIFICATIONS,
            )
            .update({ acknowledged: true })
            .match({ live_spec_id: liveSpecId })
            .then(handleSuccess, handleFailure)
    );

    const confirmationUpdates = confirmationEmailsDelivered.map((liveSpecId) =>
        supabaseClient
            .from<DataProcessingNotification>(
                TABLES.DATA_PROCESSING_NOTIFICATIONS,
            )
            .update({ acknowledged: false })
            .match({ live_spec_id: liveSpecId })
            .then(handleSuccess, handleFailure)
    );

    await Promise.all([...alertUpdates, ...confirmationUpdates]);
};

serve(async (_request: Request): Promise<Response> => {
    const { data: notifications, error: notificationError } = await supabaseClient
        .from<DataProcessingNotificationExt>(
            TABLES.DATA_PROCESSING_NOTIFICATIONS_EXT,
        )
        .select("*");

    if (notificationError !== null) {
        returnPostgresError(notificationError);
    }

    if (!notifications || notifications.length === 0) {
        // Terminate the function without error if there aren't any active notification subscriptions in the system.
        return new Response(null, {
            headers: { ...corsHeaders, "Content-Type": "application/json" },
            status: 200,
        });
    }

    const pendingAlertEmails: EmailConfig[] = notifications
        .filter(
            ({ bytes_processed, acknowledged }) => !acknowledged && bytes_processed === 0,
        )
        .map(
            ({
                catalog_name,
                evaluation_interval,
                live_spec_id,
                notification_message,
                notification_title,
                spec_type,
                verified_email,
            }) => {
                const timeOffset = evaluation_interval.split(":");
                const hours = Number(timeOffset[0]);

                const subject = notification_title
                    .replaceAll("{spec_type}", spec_type)
                    .replaceAll("{catalog_name}", catalog_name);

                const html = notification_message
                    .replaceAll("{spec_type}", spec_type)
                    .replaceAll("{catalog_name}", catalog_name)
                    .replaceAll(
                        "{evaluation_interval}",
                        isFinite(hours) ? hours.toString() : timeOffset[0],
                    );

                return {
                    live_spec_id,
                    emails: [verified_email],
                    subject,
                    html,
                };
            },
        );

    const pendingConfirmationEmails: EmailConfig[] = notifications
        .filter(
            ({ bytes_processed, acknowledged }) => acknowledged && bytes_processed > 0,
        )
        .map(
            ({
                catalog_name,
                confirmation_message,
                confirmation_title,
                live_spec_id,
                spec_type,
                verified_email,
            }) => {
                const subject = confirmation_title
                    .replaceAll("{spec_type}", spec_type)
                    .replaceAll("{catalog_name}", catalog_name);

                const html = confirmation_message
                    .replaceAll("{spec_type}", spec_type)
                    .replaceAll("{catalog_name}", catalog_name);

                return {
                    live_spec_id,
                    emails: [verified_email],
                    subject,
                    html,
                };
            },
        );

    if (
        pendingAlertEmails.length === 0 &&
        pendingConfirmationEmails.length === 0
    ) {
        return new Response(null, {
            headers: { ...corsHeaders, "Content-Type": "application/json" },
            status: 200,
        });
    }

    const alertEmailsDelivered = await emailNotifications(pendingAlertEmails);
    const confirmationEmailsDelivered = await emailNotifications(
        pendingConfirmationEmails,
    );

    await updateAcknowledgementFlag(
        alertEmailsDelivered,
        confirmationEmailsDelivered,
    );

    return new Response(null, {
        status: 200,
        headers: {
            "Content-Type": "application/json",
        },
    });
});
