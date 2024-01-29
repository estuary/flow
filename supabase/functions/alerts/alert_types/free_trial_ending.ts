import { AlertRecord, EmailConfig } from "../index.ts";
import { commonTemplate } from "../template.ts";

interface FreeTrialEnding {
    // This feels like it should apply to all alert types, and doesn't belong here..
    recipients: {
        email: string;
        full_name: string | null;
    }[];
    trial_start: string;
    trial_end: string;
    tenant: string;
    has_credit_card: boolean;
}

type FreeTrialEndingRecord = AlertRecord<"free_trial_ending", FreeTrialEnding>;

const freeTrialEnding = (req: FreeTrialEndingRecord): EmailConfig[] => {
    return req.arguments.recipients.map((recipient) => ({
        emails: [recipient.email],
        subject: "Estuary Flow: Paid Tier",
        content: req.arguments.has_credit_card
            ? commonTemplate(`
                <mj-text font-size="20px" color="#512d0b"><strong>Dear ${recipient.full_name},</strong></mj-text>
                <mj-text font-size="17px" line-height="1.3">Just so you know, your free trial for <a class="identifier">${req.arguments.tenant}</a> will be ending on <strong>${req.arguments.trial_end}</strong>. Since you have already added a payment method, no action is required.</mj-text>
                <mj-button background-color="#5072EB" color="white" href="https://dashboard.estuary.dev/admin/billing" padding="25px 0 0 0" font-weight="400" font-size="17px">📈 View your stats</mj-button>
            `)
            : commonTemplate(`
                <mj-text font-size="20px" color="#512d0b"><strong>Dear ${recipient.full_name},</strong></mj-text>
                <mj-text font-size="17px" line-height="1.3">Your free trial for <a class="identifier">${req.arguments.tenant}</a> is ending on <strong>${req.arguments.trial_end}</strong>, at which point your account will begin accruing usage. Please enter a payment method in order to continue using the platform after your trial ends. We'd be sad to see you go!</mj-text>
                <mj-button background-color="#5072EB" color="white" href="https://dashboard.estuary.dev/admin/billing" padding="25px 0 0 0" font-weight="400" font-size="17px">💳 Enter a credit card</mj-button>
            `),
    }));
};

export const freeTrialEndingEmail = (request: FreeTrialEndingRecord): EmailConfig[] => {
    if (request.resolved_at) {
        // Do we want to send a "cc confirmed" email when this alert stops firing?
        return [];
    } else {
        return freeTrialEnding(request);
    }
};
