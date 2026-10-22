# Billing integrations

Operator CLI for publishing control-plane bills to Stripe and approving collection.
`lib.rs` defines the commands and shared calendar-month parser.

- `publish.rs` classifies bills and creates or refreshes draft invoices. `--dry-run`
  performs read-only classification against Stripe. Manual bills always use
  `send_invoice`. `--recreate-existing` (alias `--recreate-finalized`) replaces
  eligible usage invoices; it does not reissue manual bills.
  A bill's invoice and a tenant's customer are retrieved by the IDs recorded in `stripe_invoice_id` (of `internal.billing_historicals` and `internal.manual_bills`) and `tenants.stripe_customer_id`. Without a recorded ID, execution creates the customer or invoice and records its ID. A manual bill keeps one invoice across the months it spans and across edits to its dates; each publish refreshes its draft.
- `send.rs` checks current Stripe state before approval and again before
  finalization. Usage drafts without a payment method can switch to Net 30 with
  operator approval. Failed switches are excluded from finalization; incorrectly
  configured manual drafts require republishing. A changed collection decision
  after approval requires another send run. Per-invoice failures do not prevent
  other invoices from completing, but cause the command to exit unsuccessfully.
  Errors use structured logging so invoice context is retained in redirected output.
  Bills are selected from the database with the same query as publishing, and each bill's invoice is retrieved by its recorded ID. Bills without one are skipped. After editing a manual bill's dates, republish its draft before sending it.
- `stripe_utils.rs` wraps Stripe invoices for metadata access and operator tables.

Both commands require `--connection-string` and accept `--month YYYY-MM` or the legacy `YYYY-MM-01` form.
Run the focused checks with `mise exec -- cargo nextest run -p billing-integrations`.
