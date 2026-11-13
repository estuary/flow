# Billing integrations

Operator CLI for calculating monthly invoices, publishing bills to Stripe, and
approving collection.
`lib.rs` defines the commands and shared calendar-month parser.

- `generate.rs` uses `billing-types` to calculate invoice line items,
  credits, and subtotals from Bigtable usage and PostgreSQL billing terms.
  `generate-invoices` prints the calculations as JSON for inspection without
  saving them.
  `freeze-invoices` saves the month's calculated invoices in one PostgreSQL
  transaction, reading usage, calculating invoices, and inserting them in serial
  batches. It retains invoices already frozen and their Stripe invoice IDs.
  It replaces the operator's `internal.freeze_billing_month(...)` call.
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
  Bills are selected from the database with the same query as publishing, and each bill's invoice is retrieved by its recorded ID. Usage bills without one are skipped; a manual bill without one is skipped with a warning. After editing a manual bill's dates, republish its draft before sending it.
- `stripe_utils.rs` wraps Stripe invoices for metadata access and operator tables,
  and checks invoice customer IDs.

Recorded invoices must belong to the tenant's recorded Stripe customer before
publishing, cleanup, or sending. A missing or mismatched customer ID prevents
writes to that invoice. Cleanup reports the failure as a warning.

All commands require `--connection-string` and accept `--month YYYY-MM` or the legacy `YYYY-MM-01` form.

Calculate a tenant's invoice with `generate-invoices --month 2026-08 --tenants acmeCo/`
and a control-plane `--connection-string`. Use `--all-tenants` for every tenant.
Provide `--bigtable-project` and `--bigtable-instance`, or `BIGTABLE_PROJECT` and
`BIGTABLE_INSTANCE`, with Google application default credentials that can read
the statistics. Application profile and emulator options match the catalog
stats API. Invoice JSON goes to stdout; diagnostics go to stderr.

Freeze a month with `freeze-invoices --month 2026-08` and the same database and
Bigtable settings, once the month has ended. This recalculates invoices from current inputs, so amounts
can differ from an earlier `generate-invoices` run if usage or billing terms
changed. The command selects all tenants and prints how many invoices were newly
frozen.

Run the focused checks with `mise exec -- cargo nextest run -p billing-integrations`.
