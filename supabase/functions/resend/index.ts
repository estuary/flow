import { serve } from "https://deno.land/std@0.184.0/http/server.ts";
import { isFinite } from "https://cdn.skypack.dev/lodash";

import { corsHeaders } from "../_shared/cors.ts";
import { returnPostgresError } from "../_shared/helpers.ts";
import { supabaseClient } from "../_shared/supabaseClient.ts";

interface NotificationQuery {
    notification_id: string;
    evaluation_interval: string;
    acknowledged: boolean;
    notification_title: string;
    notification_message: string;
    classification: string | null;
    preference_id: string;
    verified_email: string;
    live_spec_id: string;
    catalog_name: string;
    spec_type: string;
}

interface CatalogStatsQuery {
    catalog_name: string;
    grain: string;
    ts: string;
    bytes_written_by_me: number;
    bytes_written_to_me: number;
    bytes_read_by_me: number;
    bytes_read_from_me: number;
}

interface EmailConfig {
    notification_id: string;
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

const getDataProcessedInInterval = (
    startStat: CatalogStatsQuery,
    endStat: CatalogStatsQuery,
    specType: string,
): boolean => {
    if (specType === "capture") {
        const dataProcessed = endStat.bytes_written_by_me - startStat.bytes_written_by_me;

        return dataProcessed > 0;
    } else if (specType === "materialization") {
        const dataProcessed = endStat.bytes_read_by_me - startStat.bytes_read_by_me;

        return dataProcessed > 0;
    } else {
        const dataWritten = endStat.bytes_written_to_me - startStat.bytes_written_to_me;
        const dataRead = endStat.bytes_read_from_me - startStat.bytes_read_from_me;

        return dataWritten > 0 || dataRead > 0;
    }
};

const getAlertEmailConfigurations = (
    notifications: NotificationQuery[],
    catalogStats: CatalogStatsQuery[],
): EmailConfig[] => {
    return notifications
        .filter(({ evaluation_interval, catalog_name, spec_type }) => {
            const taskStats = catalogStats.filter(
                (stat) => stat.catalog_name === catalog_name,
            );

            const timeOffset = evaluation_interval.split(":");
            const hourOffset = Number(timeOffset[0]);

            if (isFinite(hourOffset)) {
                const endStat = taskStats[0];

                const intervalStart = new Date(endStat.ts);
                const intervalHours = intervalStart.getUTCHours() - hourOffset;

                intervalStart.setUTCHours(intervalHours);

                const startStat = taskStats.find((stat) => {
                    const statDate = new Date(stat.ts);

                    return (
                        statDate.toUTCString() === intervalStart.toUTCString()
                    );
                });

                if (startStat && endStat) {
                    const dataProcessed = getDataProcessedInInterval(
                        startStat,
                        endStat,
                        spec_type,
                    );

                    return !Boolean(dataProcessed);
                }
            }

            return false;
        })
        .map(
            ({
                notification_title,
                notification_message,
                catalog_name,
                notification_id,
                evaluation_interval,
                spec_type,
                verified_email,
            }) => {
                const subject = notification_title
                    .replaceAll("{spec_type}", spec_type)
                    .replaceAll("{catalog_name}", catalog_name);

                const html = notification_message
                    .replaceAll("{spec_type}", spec_type)
                    .replaceAll("{catalog_name}", catalog_name)
                    .replaceAll("{notification_interval}", evaluation_interval);

                return {
                    notification_id,
                    emails: [verified_email],
                    subject,
                    html,
                };
            },
        );
};

const RESEND_API_KEY = "re_Qu4ZevKs_DmDfQxdNmMvoyuSeGtfYz2VS";

serve(async (_request: Request): Promise<Response> => {
    const { data: notifications, error: notificationError } = await supabaseClient
        .from<NotificationQuery>("notifications_ext")
        .select("*")
        .eq("classification", "data-not-processed-in-interval");

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

    // Determine a date to use to narrow the catalog stats query results. The largest notification interval
    // supported at this time is 24 hours.
    const startDate = new Date();
    const yesterday = startDate.getUTCDate() - 1;

    startDate.setUTCMilliseconds(0);
    startDate.setUTCSeconds(0);
    startDate.setUTCMinutes(0);
    startDate.setUTCHours(0);
    startDate.setUTCDate(yesterday);

    const catalogNames = notifications
        .filter(({ acknowledged }) => !acknowledged)
        .map(({ catalog_name }) => catalog_name);

    const { data: catalogStats, error: catalogStatsError } = await supabaseClient
        .from<CatalogStatsQuery>("catalog_stats")
        .select(
            `catalog_name,
             grain,
             ts,
             bytes_written_by_me,
             bytes_written_to_me,
             bytes_read_by_me,
             bytes_read_from_me`,
        )
        .in("catalog_name", catalogNames)
        .eq("grain", "hourly")
        .gte("ts", startDate.toUTCString())
        .order("ts", { ascending: false });

    if (catalogStatsError !== null) {
        returnPostgresError(catalogStatsError);
    }

    if (!catalogStats || catalogStats.length === 0) {
        return new Response(
            JSON.stringify({
                error: {
                    code: "catalog_stats_missing",
                    message: `Catalog stats not found.`,
                    description: `Failed to fetch the catalog stats of the requested entities.`,
                },
            }),
            {
                headers: { ...corsHeaders, "Content-Type": "application/json" },
                status: 500,
            },
        );
    }

    const alertEmailConfigs: EmailConfig[] = getAlertEmailConfigurations(
        notifications,
        catalogStats,
    );

    if (alertEmailConfigs.length === 0) {
        return new Response(null, {
            headers: { ...corsHeaders, "Content-Type": "application/json" },
            status: 200,
        });
    }

    const alertEmailSent: string[] = [];

    const alertPromises = alertEmailConfigs.map(
        ({ notification_id, emails, subject, html }) =>
            fetch("https://api.resend.com/emails", {
                method: "POST",
                headers: {
                    ...corsHeaders,
                    "Content-Type": "application/json",
                    "Authorization": `Bearer ${RESEND_API_KEY}`,
                },
                body: JSON.stringify({
                    from: "Resend Test <onboarding@resend.dev>",
                    to: ["tucker.kiahna@gmail.com"],
                    subject,
                    html,
                }),
            }).then(
                (response) => {
                    if (response.ok) {
                        alertEmailSent.push(notification_id);
                    }
                },
                () => {},
            ),
    );

    await Promise.all(alertPromises);

    if (alertEmailSent.length === 0) {
        return new Response(null, {
            headers: { ...corsHeaders, "Content-Type": "application/json" },
            status: 200,
        });
    }

    const acknowledgementPromises = alertEmailSent.map((notificationId) =>
        supabaseClient
            .from("notifications")
            .update({ acknowledged: true })
            .match({ id: notificationId })
            .then(handleSuccess, handleFailure)
    );

    await Promise.all(acknowledgementPromises);

    return new Response(null, {
        status: 200,
        headers: {
            "Content-Type": "application/json",
        },
    });
});
