begin;

create schema stripe;
grant usage on schema stripe to postgres;

-- Included here to match the shape of production database so that sqlx can infer queries properly.
create table stripe.customers (
    id text PRIMARY KEY,
    address json,
    "address/city" text,
    "address/country" text,
    "address/line1" text,
    "address/line2" text,
    "address/postal_code" text,
    "address/state" text,
    balance bigint,
    created bigint,
    currency text,
    default_source text,
    delinquent boolean,
    description text,
    email text,
    invoice_prefix text,
    invoice_settings json,
    "invoice_settings/custom_fields" json,
    "invoice_settings/default_payment_method" text,
    metadata json,
    name text,
    phone text,
    flow_document json NOT NULL
);

commit;