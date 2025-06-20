import { AlertRecord, EmailConfig } from "../index.ts";
import { Recipient } from "../template.ts";
import { commonTemplate } from "../template.ts";

interface FreeTrialArguments {
    tenant: string;
    // This feels like it should apply to all alert types, and doesn't belong here..
    recipients: Recipient[];
    trial_start: string;
    trial_end: string;
    has_credit_card: boolean;
}

type FreeTrialRecord = AlertRecord<"free_trial", FreeTrialArguments>;

const freeTrialStarted = (req: FreeTrialRecord): EmailConfig[] => {
    const args = req.arguments;

    return args.recipients.map((recipient) => ({
        emails: [recipient.email],
        subject: "Estuary Free Trial",
        content: args.has_credit_card
            // The only scenario in which your trial would start and you have a cc on file is we manually gave you back your trial
            ? commonTemplate(
                `
                <mj-text>Your Estuary account <a class="identifier">${args.tenant}</a> has started its 30-day free trial. This trial will end on <strong>${args.trial_end}</strong>. Billing will begin accruing then.</mj-text>
            `,
                recipient,
            )
            : commonTemplate(
                `
                <mj-text>We hope you're enjoying Estuary Flow. Our free tier includes 10 GB/month and 2 connectors. Your account <a class="identifier">${args.tenant}</a> has now exceeded that, so it has been transitioned to a 30 day free trial ending on <strong>${args.trial_end}</strong>.</mj-text>
                <mj-text>Please add payment information in the next 30 days to continue using the platform. If you have any questions, feel free to reach out to <a href="mailto:support@estuary.dev">support@estuary.dev</a></mj-text>
                <mj-button href="https://dashboard.estuary.dev/admin/billing">Add payment information</mj-button>
            `,
                recipient,
            ),
    }));
};

const freeTrialEnded = (req: FreeTrialRecord): EmailConfig[] => {
    const args = req.resolved_arguments;
    if (!args) {
        throw new Error("Resolved arguments are required for this alert type.");
    }

    return args.recipients.map((recipient) => ({
        emails: [recipient.email],
        subject: args.has_credit_card ? "Estuary Flow: Paid Tier" : "Estuary Paid Tier: Enter Payment Info to Continue Access",
        content: args.has_credit_card
            ? commonTemplate(
                `
                <mj-text>We hope you are enjoying Estuary Flow. Your free trial for account <a class="identifier">${args.tenant}</a> is officially over.</mj-text>
                <mj-text>Since you have already added a payment method, no action is required. If you have any questions, feel free to reach out to <a href="mailto:support@estuary.dev">support@estuary.dev</a> anytime!</mj-text>
                <mj-button href="https://dashboard.estuary.dev">🌊 View your data flows</mj-button>
                `,
                recipient,
            )
            : commonTemplate(
                `
                <mj-text>We hope you are enjoying Estuary Flow. Your free trial for account <a class="identifier">${args.tenant}</a> is officially over.</mj-text>
                <mj-text>Please add payment information immediately to continue using the platform. If you have any questions, feel free to reach out to <a href="mailto:support@estuary.dev">support@estuary.dev</a></mj-text>
                <mj-button href="https://dashboard.estuary.dev/admin/billing">Add payment information</mj-button>
            `,
                recipient,
            ),
    }));
};

export const freeTrialEmail = (request: FreeTrialRecord): EmailConfig[] => {
    if (request.resolved_at) {
        return freeTrialEnded(request);
    } else {
        return freeTrialStarted(request);
    }
};
