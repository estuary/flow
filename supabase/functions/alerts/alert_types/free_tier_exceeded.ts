import { AlertRecord, EmailConfig } from "../index.ts";
import { commonTemplate } from "../template.ts";

interface FreeTierExceededArguments {
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

type FreeTierExceededRecord = AlertRecord<"free_tier_exceeded", FreeTierExceededArguments>;

const freeTierExceeded = (req: FreeTierExceededRecord): EmailConfig[] => {
    return req.arguments.recipients.map((recipient) => ({
        emails: [recipient.email],
        subject: "Estuary Flow: Paid Tier",
        content: req.arguments.has_credit_card
            // The only scenario in which your trial would start and you have a cc on file is we manually gave you back your trial
            ? commonTemplate(`
                <mj-text font-size="20px" color="#512d0b"><strong>Dear ${recipient.full_name},</strong></mj-text>
                <mj-text font-size="17px" line-height="1.3">Your Estuary account <a class="identifier">${req.arguments.tenant}</a> has started its 30-day free trial. This trial will end on <strong>${req.arguments.trial_end}</strong>. Billing will begin accruing then.</mj-text>
            `)
            : commonTemplate(`
                <mj-text font-size="20px" color="#512d0b"><strong>Dear ${recipient.full_name},</strong></mj-text>
                <mj-text font-size="17px" line-height="1.3">Your Estuary account <a class="identifier">${req.arguments.tenant}</a> has gone over its free tier limit, and is advancing to a 30 day free trial that ends on <strong>${req.arguments.trial_end}</strong>. Please enter a credit card before then to continue using the platform.</mj-text>
                <mj-button background-color="#5072EB" color="white" href="https://dashboard.estuary.dev/admin/billing" padding="25px 0 0 0" font-weight="400" font-size="17px">💳 Enter a credit card</mj-button>
            `),
    }));
};

export const freeTierExceededEmail = (request: FreeTierExceededRecord): EmailConfig[] => {
    if (request.resolved_at) {
        // Do we want to send a "cc confirmed" email when this alert stops firing?
        return [];
    } else {
        return freeTierExceeded(request);
    }
};
