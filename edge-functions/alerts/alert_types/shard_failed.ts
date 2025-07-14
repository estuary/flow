import { AlertRecord, EmailConfig } from "../index.ts";
import { commonTemplate, getTaskDetailsPageURL } from "../template.ts";
import { Recipient } from "../template.ts";
import { ControllerAlertArguments } from "./controller_alerts.ts";

type ShardFailedRecord = AlertRecord<"shard_failed", ControllerAlertArguments>;

const formatAlertEmail = ({
  arguments: { recipients, spec_type },
  catalog_name,
}: ShardFailedRecord): EmailConfig[] => {
  const subject = `Estuary Flow: Shard failure detected for ${spec_type} ${catalog_name}`;
  const detailsPageURL = getTaskDetailsPageURL(catalog_name, spec_type);

  return recipients.map((recipient) => ({
    content: commonTemplate(
      `
                <mj-text>
                    Your Estuary ${spec_type} <a class="identifier">${catalog_name}</a> has a failure that is impacting your data pipeline. To troubleshoot please:
                </mj-text>
                <mj-text>
                    <ul>
                        <li><a href="${detailsPageURL}" target="_blank" rel="noopener">Visit the task status and logs</a> for more information about the error</li>
                        <li>If you need help please reach out to our team via Slack (#support and #ask-ai) or reply to this email.</li>
                    </ul>
                </mj-text>
                <mj-text>
                    We are here to help ensure your data pipelines run smoothly.
                </mj-text>
            `,
      recipient,
    ),
    subject,
    emails: [recipient.email],
  }));
};

const formatConfirmationEmail = ({
  arguments: { recipients, spec_type },
  catalog_name,
}: ShardFailedRecord): EmailConfig[] => {
  const subject = `Estuary Flow: Shard failure resolved for ${spec_type} ${catalog_name}`;
  const detailsPageURL = getTaskDetailsPageURL(catalog_name, spec_type);

  return recipients.map((recipient) => ({
    content: commonTemplate(
      `
                <mj-text>
                    Good news! The shard failure for your ${spec_type} <a class="identifier">${catalog_name}</a> has been resolved.
                </mj-text>
                <mj-text>
                    You can <a href="${detailsPageURL}" target="_blank" rel="noopener">view your task</a> to confirm everything is working as expected, or update your alerting settings.
                </mj-text>
                <mj-text>
                    If you continue to experience issues, please don't hesitate to reach out to our support team.
                </mj-text>
            `,
      recipient,
    ),
    subject,
    emails: [recipient.email],
  }));
};

export const shardFailedEmail = (request: ShardFailedRecord): EmailConfig[] => {
  if (request.resolved_at) {
    return formatConfirmationEmail(request);
  } else {
    return formatAlertEmail(request);
  }
};
