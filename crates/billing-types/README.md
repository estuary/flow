# Billing types

Billing code shared by `agent`, `billing-integrations`, and `control-plane-api`. `lib.rs` and `stripe_helpers.rs` define Stripe metadata keys, invoice and payment provider types, and Stripe search helpers. `db.rs` records a tenant's Stripe customer ID.

## Invoice calculation

The calculation in `calculate.rs` separates numeric pricing and credit allocation
from invoice formatting. `BillingTerms` and the calculation inputs stay within
this crate; `Invoice` carries the invoice JSON.

`invoices::generate` reads billing terms and adjustments from PostgreSQL and daily
tenant usage from Bigtable. It returns the calculated invoices without writing
them to storage. PostgreSQL inputs are read in one snapshot, and the transaction
is released before reading Bigtable.

Calculation takes a year and `chrono::Month` to select a UTC calendar month.
Charges use cumulative integer bytes and seconds rounded up to billable GB and
hours, with amounts calculated in cents.
Each day is allocated the change in cumulative tier price, so credits apply to
usage in the applicable free or trial period. Usage fields also show fractional
GB and hours for display. Months with no usage still include recurring fees and
adjustments.

Run focused checks through `mise exec -- cargo nextest run -p billing-types`. Integration
tests require this checkout's `local:supabase` and `local:bigtable` services.
