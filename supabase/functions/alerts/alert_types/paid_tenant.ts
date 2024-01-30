import { AlertRecord, EmailConfig } from "../index.ts";
import { commonTemplate, Recipient } from "../template.ts";

interface PaidTenantArguments {
    // This feels like it should apply to all alert types, and doesn't belong here..
    recipients: Recipient[];
    trial_start: string;
    trial_end: string;
    tenant: string;
    in_trial: boolean;
    straight_from_free_tier: boolean;
}

type PaidTenantRecord = AlertRecord<"paid_tenant", PaidTenantArguments>;

const paidTenant = (req: PaidTenantRecord): EmailConfig[] => {
    return req.arguments.recipients.map((recipient) => ({
        emails: [recipient.email],
        subject: `Estuary: Thanks for Adding a Payment Method🎉`,
        content: commonTemplate(
            `
            <mj-text font-size="17px">We hope you are enjoying Estuary Flow. We have received your payment method for account <a class="identifier">${req.arguments.tenant}</a> and ${
                req.arguments.in_trial
                    ? `after your free trial ends on <strong>${req.arguments.trial_end}</strong> it will be transitioned to the paid tier.`
                    : `it will be transitioned to the paid tier.`
            }</mj-text>
            <mj-button href="https://dashboard.estuary.dev/admin/billing">📈 See your bill</mj-button>

            <mj-divider border-width="1px" border-style="dashed" border-color="lightgrey" padding-top="40px" padding-bottom="10px" />
            <mj-text align="center" font-weight="bold" font-size="22px">Frequently Asked Questions</mj-text>
            <mj-divider border-width="1px" border-style="dashed" border-color="lightgrey" padding-bottom="20px" />
            <mj-text font-weight="bold" font-size="19px">Where is my data stored?</mj-text>
            <mj-text font-size="17px">By default, all collection data is stored in an Estuary-owned cloud storage bucket with a 30 day retention plicy. Now that you have a paid account, you can update this to store data in your own cloud storage bucket. We support <strong>GCS</strong>, <strong>S3</strong>, and <strong>Azure Blob storage</strong>.</mj-text>
        `,
            recipient,
        ),
    }));
};

export const paidTenantEmail = (request: PaidTenantRecord): EmailConfig[] => {
    if (request.resolved_at) {
        return [];
    } else {
        return paidTenant(request);
    }
};
