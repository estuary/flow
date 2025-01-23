import { serve } from "https://deno.land/std@0.184.0/http/server.ts";

import { corsHeaders } from "../_shared/cors.ts";
import { dataMovementStalledEmail } from "./alert_types/data_movement_stalled.ts";
import { freeTrialStalledEmail } from "./alert_types/free_trial_stalled.ts";
import { freeTrialEndingEmail } from "./alert_types/free_trial_ending.ts";
import { missingPaymentMethodEmail } from "./alert_types/missing_payment_method.ts";
import { freeTrialEmail } from "./alert_types/free_trial.ts";

export interface AlertRecord<T extends keyof typeof emailTemplates, A> {
    alert_type: T;
    catalog_name: string;
    fired_at: string;
    resolved_at: string | null;
    arguments: A;
    resolved_arguments: A | null;
}

export interface EmailConfig {
    emails: string[];
    subject: string;
    content: string;
}

const emailTemplates = {
    "free_trial": freeTrialEmail,
    "free_trial_ending": freeTrialEndingEmail,
    "free_trial_stalled": freeTrialStalledEmail,
    "missing_payment_method": missingPaymentMethodEmail,
    "data_movement_stalled": dataMovementStalledEmail,
};

// This is a temporary type guard for the POST request that provides shallow validation
// of the object.
// deno-lint-ignore no-explicit-any
function validateAlertRecordKeys<T extends keyof typeof emailTemplates>(request: any): request is Parameters<typeof emailTemplates[T]>[0] {
    const validAlertType = Object.hasOwn(request, "alert_type") &&
        typeof request.alert_type === "string" &&
        Object.keys(emailTemplates).includes(request.alert_type);

    const validCatalogName = Object.hasOwn(request, "catalog_name") &&
        typeof request.catalog_name === "string";

    const validFiredAtTimestamp = Object.hasOwn(request, "fired_at") &&
        typeof request.fired_at === "string";

    const validResolvedAtTimestamp = Object.hasOwn(request, "resolved_at") &&
        (typeof request.resolved_at === "string" ||
            request.resolved_at === null);

    const argumentsExist = Object.hasOwn(request, "arguments");

    return (
        validAlertType &&
        validCatalogName &&
        validFiredAtTimestamp &&
        validResolvedAtTimestamp &&
        argumentsExist
    );
}

const emailNotifications = (
    pendingNotifications: EmailConfig[],
    token: string,
    senderAddress: string,
): Promise<Response[]> => {
    const notificationPromises = pendingNotifications.flatMap(({ content, emails, subject }) =>
        emails.map((email) => {
            return fetch("https://api.resend.com/emails", {
                method: "POST",
                headers: {
                    ...corsHeaders,
                    "Content-Type": "application/json",
                    "Authorization": `Bearer ${token}`,
                },
                body: JSON.stringify({
                    from: senderAddress,
                    to: email,
                    subject,
                    html: content,
                }),
            });
        })
    );

    return Promise.all(notificationPromises);
};

// TODO(jshearer): This should be renamed to "fire all alerts"
serve(async (rawRequest: Request): Promise<Response> => {
    const request = await rawRequest.json();

    if (!validateAlertRecordKeys(request)) {
        return new Response(
            JSON.stringify({
                error: {
                    code: "malformed_request",
                    message: `Malformed Request: One or more parmeters are missing or invalid.`,
                    description: `You must provide 'alert_type', 'catalog_name', 'fired_at', 'resolved_at', and 'arguments'. 'alert_type' must be one of [${
                        Object.keys(emailTemplates).join(", ")
                    }]`,
                },
            }),
            {
                headers: { ...corsHeaders, "Content-Type": "application/json" },
                status: 400,
            },
        );
    }

    const resendToken = Deno.env.get("RESEND_API_KEY");
    const senderAddress = Deno.env.get("RESEND_EMAIL_ADDRESS");
    const sharedSecret = Deno.env.get("ALERT_EMAIL_FUNCTION_SECRET");

    const authHeader = rawRequest.headers.get("authorization");

    const missingCredentials = !resendToken || !senderAddress || !sharedSecret || !authHeader;

    if (missingCredentials || !authHeader.includes(sharedSecret)) {
        return new Response(
            JSON.stringify({
                error: {
                    code: "invalid_resend_credentials",
                    message: `Unauthorized: access is denied due to invalid credentials.`,
                    description: `The server could not verify that you are authorized to access the desired resource with the credentials provided.`,
                },
            }),
            {
                headers: {
                    ...corsHeaders,
                    "Content-Type": "application/json",
                },
                status: 401,
            },
        );
    }

    let pendingEmails: EmailConfig[] = [];

    // This is an annoying hack to work around TypeScript's lack of support for
    // correlated union types [1]. The problem is that even though we know
    // that `request.alert_type` is valid, and we can get the generator out
    // of `emailTemplates`, TypeScript still reads the type of that generator
    // as the union of all possible generator types. Since different email
    // templates have different arguments, that looks like (never)=>EmailConfig[].
    // [1]: https://github.com/microsoft/TypeScript/issues/30581
    switch (request.alert_type) {
        case "free_trial":
            pendingEmails = emailTemplates[request.alert_type](request);
            break;
        case "free_trial_ending":
            pendingEmails = emailTemplates[request.alert_type](request);
            break;
        case "free_trial_stalled":
            pendingEmails = emailTemplates[request.alert_type](request);
            break;
        case "missing_payment_method":
            pendingEmails = emailTemplates[request.alert_type](request);
            break;
        case "data_movement_stalled":
            pendingEmails = emailTemplates[request.alert_type](request);
            break;
        default: {
            // This checks that we have an exhaustive match. If this line has a
            // type error, make sure you have a case above for every key in `emailTemplates`.
            const exhaustiveCheck: never = request;
            throw new Error(`Unhandled alert type: ${exhaustiveCheck}`);
        }
    }

    const responses = await emailNotifications(
        pendingEmails,
        resendToken,
        senderAddress,
    );

    const errors = responses.filter((response) => response.status >= 400);

    if (errors.length > 0) {
        console.log("finished sending emails", {
            catalogName: request.catalog_name,
            attempted: responses.length,
            errors,
        });

        errors.forEach(async (error) => {
            console.error(await error.text());
        });

        return new Response(
            JSON.stringify({
                error: {
                    code: "email_send_failure",
                    message: `Sending email failed.`,
                    description: `Failed to send ${errors.length} emails.`,
                },
            }),
            {
                headers: { ...corsHeaders, "Content-Type": "application/json" },
                status: 500,
            },
        );
    } else {
        console.info(`${responses.length} emails sent.`);
    }

    return new Response(null, {
        status: 200,
        headers: {
            "Content-Type": "application/json",
        },
    });
});
