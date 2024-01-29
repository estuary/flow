import { AlertRecord, EmailConfig } from "../index.ts";
import { commonTemplate } from "../template.ts";

interface FreeTrialEnded {
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

type FreeTrialEndedRecord = AlertRecord<"free_trial_ended", FreeTrialEnded>;

const freeTrialEnded = (req: FreeTrialEndedRecord): EmailConfig[] => {
    return req.arguments.recipients.map((recipient) => ({
        emails: [recipient.email],
        subject: "Estuary Flow: Paid Tier",
        content: req.arguments.has_credit_card
            ? commonTemplate(`
                <mj-text font-size="20px" color="#512d0b"><strong>Dear ${recipient.full_name},</strong></mj-text>
                <mj-text font-size="17px" line-height="1.3">Your free trial for <a class="identifier">${req.arguments.tenant}</a> has ended. Since you have already added a payment method, no action is required.</mj-text>
                <mj-button background-color="#5072EB" color="white" href="https://dashboard.estuary.dev/admin/billing" padding="25px 0 0 0" font-weight="400" font-size="17px">📈 See your bill</mj-button>
            `)
            : commonTemplate(`
                <mj-text font-size="20px" color="#512d0b"><strong>Dear ${recipient.full_name},</strong></mj-text>
                <mj-text font-size="17px" line-height="1.3">Your free trial for <a class="identifier">${req.arguments.tenant}</a> ended on <strong>${req.arguments.trial_end}</strong>, and your account is now accruing usage. Please enter a payment method within 5 days to continue using the platform.</mj-text>
                <mj-button background-color="#5072EB" color="white" href="https://dashboard.estuary.dev/admin/billing" padding="25px 0 0 0" font-weight="400" font-size="17px">💳 Enter a credit card</mj-button>
            `),
    }));
};

export const freeTrialEndedEmail = (request: FreeTrialEndedRecord): EmailConfig[] => {
    if (request.resolved_at) {
        // Do we want to send a "cc confirmed" email when this alert stops firing?
        return [];
    } else {
        return freeTrialEnded(request);
    }
};
