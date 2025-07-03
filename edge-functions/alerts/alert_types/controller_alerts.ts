import { Recipient } from "../template.ts";

export interface ControllerAlertArguments {
  recipients: Recipient[];
  spec_type: string; // "capture", "materialization", "collection", "test"
  first_ts: string;
  last_ts?: string;
  error: string;
  count: number;
  resolved_at?: string;
}
