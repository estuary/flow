import { AlertRecord, EmailConfig } from "../index.ts";
import { Recipient } from "../template.ts";
import { commonTemplate } from "../template.ts";

interface FreeTrialEnding {
    tenant: string;
    // This feels like it should apply to all alert types, and doesn't belong here..
    recipients: Recipient[];
    trial_start: string;
    trial_end: string;
    has_credit_card: boolean;
}

type FreeTrialEndingRecord = AlertRecord<"free_trial_ending", FreeTrialEnding>;

const freeTrialEnding = (req: FreeTrialEndingRecord): EmailConfig[] => {
    return req.arguments.recipients.map((recipient) => ({
        emails: [recipient.email],
        bcc: !req.arguments.has_credit_card ? ["dave@estuary.dev", "elif@estuary.dev"] : undefined,
        subject: "Estuary Flow: Paid Tier",
        content: req.arguments.has_credit_card
            ? commonTemplate(
                `
                <mj-text>Just so you know, your free trial for <a class="identifier">${req.arguments.tenant}</a> will be ending on <strong>${req.arguments.trial_end}</strong>. Since you have already added a payment method, no action is required.</mj-text>
                <mj-button href="https://dashboard.estuary.dev/admin/billing">📈 View your stats</mj-button>
            `,
                recipient,
            )
            : commonTemplate(
                `
                <mj-text>Your free trial for <a class="identifier">${req.arguments.tenant}</a> is ending on <strong>${req.arguments.trial_end}</strong>, at which point your account will begin accruing usage. Please enter a payment method in order to continue using the platform after your trial ends. We'd be sad to see you go!</mj-text>
                <mj-button href="https://dashboard.estuary.dev/admin/billing">Add payment information</mj-button>
            `,
                recipient,
            ),
    }));
};

export const freeTrialEndingEmail = (request: FreeTrialEndingRecord): EmailConfig[] => {
    if (request.resolved_at) {
        return [];
    } else {
        return freeTrialEnding(request);
    }
};
