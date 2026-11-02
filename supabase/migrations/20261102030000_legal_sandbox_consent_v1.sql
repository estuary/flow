begin;

insert into internal.legal_terms (type, version, text) values (
    'sandbox',
    1,
    $legal_terms$# Sandbox Agreement

Placeholder sandbox agreement. Final terms are pending.
$legal_terms$
);

commit;
