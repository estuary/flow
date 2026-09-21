# Billing integrations

Operator CLI for publishing control-plane bills to Stripe and approving collection.
`lib.rs` defines the commands and shared calendar-month parser.

- `publish.rs` classifies bills and creates or refreshes draft invoices. `--dry-run`
  performs read-only classification against Stripe. Manual bills always use
  `send_invoice`. `--recreate-existing` (alias `--recreate-finalized`) replaces
  eligible usage invoices; it does not reissue manual bills.
- `send.rs` checks current Stripe state before approval and again before
  finalization. Usage drafts without a payment method can switch to Net 30 with
  operator approval. Failed switches are excluded from finalization; incorrectly
  configured manual drafts require republishing. A changed collection decision
  after approval requires another send run.
- `stripe_utils.rs` wraps Stripe invoices for metadata access and operator tables.

Both commands accept `--month YYYY-MM` or the legacy `YYYY-MM-01` form.
Run the focused checks with `mise exec -- cargo nextest run -p billing-integrations`.
