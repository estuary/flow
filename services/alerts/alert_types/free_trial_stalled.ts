import { AlertRecord, EmailConfig } from "../index.ts";
import { Recipient } from "../template.ts";
import { commonTemplate } from "../template.ts";

interface FreeTrialStalledArguments {
    tenant: string;
    // This feels like it should apply to all alert types, and doesn't belong here..
    recipients: Recipient[];
    trial_start: string;
    trial_end: string;
}

type FreeTrialStalledRecord = AlertRecord<"free_trial_stalled", FreeTrialStalledArguments>;

// This alert only fires if they don't have a CC entered and they're >=5 days after the end of their trial
// So this alert resolving implicitly means they entered a CC.
const freeTrialStalled = (req: FreeTrialStalledRecord, started: boolean): EmailConfig[] => {
    return req.arguments.recipients.map((recipient) => ({
        emails: ["dave@estuary.dev"],
        subject: `Free Tier Grace Period for ${req.arguments.tenant}: ${started ? "No CC 💳❌" : "CC Entered 💳✅"}`,
        content: commonTemplate(
            `
                <mj-text font-size="20px" color="#512d0b"><strong>Name:</strong> ${recipient.full_name}</mj-text>
                <mj-text font-size="20px" color="#512d0b"><strong>Email:</strong> ${recipient.email}</mj-text>
                <mj-text font-size="20px" color="#512d0b"><strong>Tenant:</strong> ${req.arguments.tenant}</mj-text>
                <mj-text font-size="20px" color="#512d0b"><strong>Trial Start:</strong> ${req.arguments.trial_start}, <strong>Trial End:</strong> ${req.arguments.trial_end}</mj-text>
                <mj-text font-size="20px" color="#512d0b"><strong>Credit Card</strong>: ${started ? "❌" : "✅"} </mj-text>
            `,
            null,
        ),
    }));
};

export const freeTrialStalledEmail = (request: FreeTrialStalledRecord): EmailConfig[] => {
    return freeTrialStalled(request, request.resolved_at === null);
};
