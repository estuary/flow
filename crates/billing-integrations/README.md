# Billing integrations

Operator CLI for calculating monthly invoices, publishing bills to Stripe, and
approving collection.
`lib.rs` defines the commands and shared calendar-month parser.

- `generate.rs` uses the shared `billing` crate to calculate invoice line items,
  credits, and subtotals from Bigtable usage and PostgreSQL billing terms.
  `generate-invoices` prints the calculations as JSON for inspection without
  saving them. `--compare-sql` includes the PostgreSQL calculation alongside each
  Rust result. The two calculations read their inputs separately, so changes
  during the comparison can affect the results.
- `publish.rs` classifies bills and creates or refreshes draft invoices. `--dry-run`
  performs read-only classification against Stripe. Manual bills always use
  `send_invoice`. `--recreate-existing` (alias `--recreate-finalized`) replaces
  eligible usage invoices; it does not reissue manual bills.
- `send.rs` checks current Stripe state before approval and again before
  finalization. Usage drafts without a payment method can switch to Net 30 with
  operator approval. Failed switches are excluded from finalization; incorrectly
  configured manual drafts require republishing. A changed collection decision
  after approval requires another send run. Per-invoice failures do not prevent
  other invoices from completing, but cause the command to exit unsuccessfully.
  Errors use structured logging so invoice context is retained in redirected output.
- `stripe_utils.rs` wraps Stripe invoices for metadata access and operator tables.

All commands accept `--month YYYY-MM` or the legacy `YYYY-MM-01` form.

Calculate a tenant's invoice with `generate-invoices --month 2026-08 --tenants acmeCo/`
and a control-plane `--connection-string`. Use `--all-tenants` for every tenant.
Provide `--bigtable-project` and `--bigtable-instance`, or `BIGTABLE_PROJECT` and
`BIGTABLE_INSTANCE`, with Google application default credentials that can read
the statistics. Application profile and emulator options match the catalog
stats API. Invoice JSON goes to stdout; diagnostics go to stderr.

Run the focused checks with `mise exec -- cargo nextest run -p billing-integrations`.
