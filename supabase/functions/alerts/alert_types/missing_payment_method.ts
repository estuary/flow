import { AlertRecord, EmailConfig } from "../index.ts";
import { commonTemplate, Recipient } from "../template.ts";

interface MissingPaymentMethodArguments {
    // This feels like it should apply to all alert types, and doesn't belong here..
    recipients: Recipient[];
    trial_start: string;
    trial_end: string;
    tenant: string;
    plan_state: "free_tier" | "free_trial" | "paid";
}

const faq = [
    {
        question: "Where is my data stored?",
        answer: "By default, all collection data is stored in an Estuary-owned cloud storage bucket" +
            "with a 30 day retention plicy. Now that you have a paid account, you can update this " +
            "to store data in your own cloud storage bucket. We support GCS, S3, and Azure Blob storage.",
    },
    {
        question: "How can I access Estuary Support?",
        answer: "Reach out to support@estuary.dev or join our slack.",
    },
    {
        question: "Is it possible to schedule data flows?",
        answer: "Estuary moves most data in real-time by default, without the need for scheduling, " +
            "but you can add “update delays” to data warehouses to enable more downtime on your " +
            "warehouse for cost savings. This can be enabled under “advanced settings” and default " +
            "settings are 30 minutes for a warehouse.",
    },
];

type MissingPaymentRecord = AlertRecord<"missing_payment_method", MissingPaymentMethodArguments>;

const paymentMethodProvided = (req: MissingPaymentRecord): EmailConfig[] => {
    return req.arguments.recipients.map((recipient) => ({
        emails: [recipient.email],
        subject: `Estuary: Thanks for Adding a Payment Method🎉`,
        content: commonTemplate(
            `
            <mj-text font-size="17px">We hope you are enjoying Estuary Flow. We have received your payment method for your account <a class="identifier">${req.arguments.tenant}</a>. ${
                req.arguments.plan_state === "free_trial"
                    ? `After your free trial ends on <strong>${req.arguments.trial_end}</strong>, you will automatically be switched the paid tier.`
                    : `you are now on the paid tier.`
            }</mj-text>
            <mj-button href="https://dashboard.estuary.dev/admin/billing">📈 See your bill</mj-button>

            <mj-divider border-width="1px" border-style="dashed" border-color="lightgrey" padding-top="40px" padding-bottom="10px" />
            <mj-text align="center" font-weight="bold" font-size="22px">Frequently Asked Questions</mj-text>
            <mj-divider border-width="1px" border-style="dashed" border-color="lightgrey" padding-bottom="20px" />
            ${
                faq.map(({ question, answer }) => `
                    <mj-text font-weight="bold" font-size="19px">${question}</mj-text>
                    <mj-text font-size="17px">${answer}</mj-text>
                `).join("\n")
            }
        `,
            recipient,
        ),
    }));
};

export const missingPaymentMethodEmail = (request: MissingPaymentRecord): EmailConfig[] => {
    // We should only send an email on the trailing edge of this alert, i.e
    // "payment method is no longer missing"
    if (request.resolved_at) {
        return paymentMethodProvided(request);
    } else {
        // Maaaaaybe we want to send an email here on tenant creation that says something like
        // "Welcome to Estuary! You're on the free tier, but you'll need to add a payment method to continue using the platform after your trial ends."
        return [];
    }
};
