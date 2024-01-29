import { AlertRecord, EmailConfig } from "../index.ts";
import { commonTemplate } from "../template.ts";

interface FreeTrialGracePeriodOver {
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

type FreeTrialGracePeriodOverRecord = AlertRecord<"free_trial_grace_period_over", FreeTrialGracePeriodOver>;

const freeTrialGracePeriodOver = (req: FreeTrialGracePeriodOverRecord): EmailConfig[] => {
    return req.arguments.recipients.map((recipient) => ({
        // emails: [recipient.email],
        // TODO(jshearer): Remove joseph@estuary.dev after testing
        emails: ["dave@estuary.dev", "joseph@estuary.dev"],
        subject: `Free Tier Grace Period for ${req.arguments.tenant}: ${req.arguments.has_credit_card ? "CC Entered 💳✅" : "No CC 💳❌"}`,
        content: commonTemplate(`
                <mj-text font-size="20px" color="#512d0b"><strong>Name:</strong> ${recipient.full_name}</mj-text>
                <mj-text font-size="20px" color="#512d0b"><strong>Email:</strong> ${recipient.email}</mj-text>
                <mj-text font-size="20px" color="#512d0b"><strong>Tenant:</strong> ${req.arguments.tenant}</mj-text>
                <mj-text font-size="20px" color="#512d0b"><strong>Trial Start:</strong> ${req.arguments.trial_start}, <strong>Trial End:</strong> ${req.arguments.trial_end}</mj-text>
                <mj-text font-size="20px" color="#512d0b"><strong>Credit Card</strong>: ${req.arguments.has_credit_card ? "✅" : "❌"} </mj-text>
            `),
    }));
};

export const freeTrialGracePeriodOverEmail = (request: FreeTrialGracePeriodOverRecord): EmailConfig[] => {
    if (request.resolved_at) {
        // Do we want to send a "cc confirmed" email when this alert stops firing?
        return [];
    } else {
        return freeTrialGracePeriodOver(request);
    }
};
