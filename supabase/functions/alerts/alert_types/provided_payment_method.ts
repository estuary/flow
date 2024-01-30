import { AlertRecord, EmailConfig } from "../index.ts";
import { commonTemplate } from "../template.ts";

interface ProvidedPaymentMethod {
    // This feels like it should apply to all alert types, and doesn't belong here..
    recipients: {
        email: string;
        full_name: string | null;
    }[];
    trial_start: string;
    trial_end: string;
    tenant: string;
    in_trial: boolean;
    straight_from_free_tier: boolean;
}

type ProvidedPaymentMethodRecord = AlertRecord<"provided_payment_method", ProvidedPaymentMethod>;

const ProvidedPaymentMethod = (req: ProvidedPaymentMethodRecord): EmailConfig[] => {
    return req.arguments.recipients.map((recipient) => ({
        emails: [recipient.email],
        subject: `Estuary Flow: Payment Method Entered 🎉`,
        content: commonTemplate(`
            <mj-text font-size="20px" color="#512d0b"><strong>Dear ${recipient.full_name},</strong></mj-text>
            <mj-text font-size="17px" line-height="1.3">Thank you for entering your payment information. ${
            req.arguments.in_trial
                ? `After your free trial ends on <strong>${req.arguments.trial_end}</strong>, your account will begin accruing usage. Your dataflows will not be interrupted.`
                : req.arguments.straight_from_free_tier
                ? `Your account will now begin accruing usage. Your dataflows will not be interrupted.`
                : `Your account is now active.`
        }
            <mj-button background-color="#5072EB" color="white" href="https://dashboard.estuary.dev/admin/billing" padding="25px 0 0 0" font-weight="400" font-size="17px">📈 See your bill</mj-button>

            <mj-divider border-width="1px" border-style="dashed" border-color="lightgrey" />
            <mj-text align="center" font-weight="bold" font-size="24pt">Frequently Asked Questions</mj-text>
            <mj-divider border-width="1px" border-style="dashed" border-color="lightgrey" />
            <mj-text align="left" font-weight="bold" font-size="18pt">Where is my data stored?</mj-text>
            <mj-text align="left" font-size="16pt">By default, all collection data is stored in an Estuary-owned cloud storage bucket with a 30 day retention plicy. Now that you have a paid account, you can update this to store data in your own cloud storage bucket. We support <strong>GCS</strong>, <strong>S3</strong>, and <strong>Azure Blob storage</strong>.</mj-text>
        `),
    }));
};

export const ProvidedPaymentMethodEmail = (request: ProvidedPaymentMethodRecord): EmailConfig[] => {
    if (request.resolved_at) {
        // Do we want to send a "cc confirmed" email when this alert stops firing?
        return [];
    } else {
        return ProvidedPaymentMethod(request);
    }
};
