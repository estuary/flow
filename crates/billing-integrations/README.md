# Billing integrations

Operator CLI for publishing control-plane bills to Stripe and approving collection.
`lib.rs` defines the commands and shared calendar-month parser.

- `publish.rs` classifies bills and creates or refreshes draft invoices. `--dry-run`
  performs read-only classification against Stripe. Manual bills always use
  `send_invoice`. `--recreate-existing` (alias `--recreate-finalized`) replaces
  eligible usage invoices; it does not reissue manual bills.
  A bill's invoice and a tenant's customer are retrieved by the IDs recorded in
  `stripe_invoice_id` (of `internal.billing_historicals` / `internal.manual_bills`)
  and `tenants.stripe_customer_id`. A missing customer ID creates and records a
  tenant customer; a missing invoice ID creates an invoice on execution. The shared
  `billing-types` provider handles customer creation and retrieval; the publisher
  retains invoice lifecycle operations. A manual bill thus keeps one invoice across
  the months it spans and edits to its dates, which refresh its draft.
- `send.rs` selects bills using the same database month and tenant filters as
  publishing, then retrieves their recorded Stripe invoice IDs. Bills without an
  invoice ID are skipped. DB tenant, bill type, and dates govern selection and
  operator context; Stripe supplies current invoice and payment state. It checks
  that state before approval and again before finalization. Usage drafts without a payment method can switch to Net 30 with
  operator approval. Failed switches are excluded from finalization; incorrectly
  configured manual drafts require republishing. A changed collection decision
  after approval requires another send run. Per-invoice failures do not prevent
  other invoices from completing, but cause the command to exit unsuccessfully.
  Errors use structured logging so invoice context is retained in redirected output.
  Republish a manual draft after editing its bill dates before sending it.
- `stripe_utils.rs` pairs Stripe state with its database bill context for operator
  tables and collection decisions.

Both commands require `--connection-string` and accept `--month YYYY-MM` or
`YYYY-MM-01`. Final bills belong to the selected month; manual bills are selected
when their date range overlaps it. `send-invoices` processes Draft/Open invoices
and keeps `--fix-auto-advance` for open invoices.
Run the focused checks with `mise exec -- cargo nextest run -p billing-integrations`.
