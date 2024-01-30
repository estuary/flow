import { AlertRecord, EmailConfig } from "../index.ts";
import { Recipient } from "../template.ts";
import { commonTemplate } from "../template.ts";

interface DelinquentTenantArguments {
    // This feels like it should apply to all alert types, and doesn't belong here..
    recipients: Recipient[];
    trial_start: string;
    trial_end: string;
    tenant: string;
}

type DelinquentTenantRecord = AlertRecord<"delinquent_tenant", DelinquentTenantArguments>;

const delinquentTenant = (req: DelinquentTenantRecord, started: boolean): EmailConfig[] => {
    return req.arguments.recipients.map((recipient) => ({
        // emails: [recipient.email],
        // TODO(jshearer): Remove joseph@estuary.dev after testing
        emails: ["dave@estuary.dev", "joseph@estuary.dev"],
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

export const delinquentTenantEmail = (request: DelinquentTenantRecord): EmailConfig[] => {
    return delinquentTenant(request, request.resolved_at !== null);
};
