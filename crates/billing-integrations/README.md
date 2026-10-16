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
  after approval requires another send run. Per-invoice failures do not prevent
  other invoices from completing, but cause the command to exit unsuccessfully.
  Errors use structured logging so invoice context is retained in redirected output.
- `stripe_utils.rs` wraps Stripe invoices for metadata access and operator tables.

Publishing and sending accept `--month YYYY-MM` or the legacy `YYYY-MM-01` form.
Run the focused checks with `mise exec -- cargo nextest run -p billing-integrations`.

`backfill-stripe-ids` is a temporary cutover command. It lists Stripe customers
and invoices and matches their metadata to persisted tenants and bills. Preview
first, then add `--apply` to fill NULL IDs in one transaction:

```sh
billing-integrations backfill-stripe-ids --connection-string "$DATABASE_URL" --stripe-api-key "$STRIPE_API_KEY"
```

Existing IDs remain authoritative. Mapping conflicts return a nonzero exit and
prevent all writes. Unmatched objects and remaining NULL counts are reported for
operator verification; a successful command does not establish that every
historical invoice was mapped, particularly after manual bill dates were edited.

Apply the schema first. Pause customer creation and publishing during final
backfill and verification, deploy the ID-only agent and API, then resume. The
old Rust setup-intent path creates customers without recording their IDs, even
when the UI already uses GraphQL. Use the updated publisher for the next run.
