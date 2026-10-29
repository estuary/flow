-- no-transaction
-- PostgreSQL requires the enum value to be committed before it can be used.
begin;
alter type internal.legal_terms_type add value 'sandbox_consent';
commit;

-- Placeholder pending legal review.
insert into internal.legal_terms (type, version, text) values (
    'sandbox_consent', 1,
    'Lorem ipsum dolor sit amet, consectetur adipiscing elit.'
);
