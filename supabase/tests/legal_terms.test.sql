create function tests.test_legal_terms_and_consent()
returns setof text as $$
declare
    terms internal.legal_terms;
    consent internal.tenant_consent;
    tenant public.tenants;
    consent_user uuid := 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
begin
    insert into internal.legal_terms (type, text)
        values ('msa', 'Test MSA') returning * into terms;
    insert into auth.users (id, email)
        values (consent_user, 'consent@example.test');
    insert into public.tenants (tenant)
        values ('acmeCo/') returning * into tenant;
    insert into internal.tenant_consent (user_id, user_email, terms_id, tenant_name, tenant_id)
        values (consent_user, 'consent@example.test', terms.id, tenant.tenant, tenant.id)
        returning * into consent;

    return query select throws_ok(
        format('insert into internal.tenant_consent (user_email, terms_id, tenant_name, tenant_id)
                values (%L, %L, %L, %L)',
               consent.user_email, terms.id, tenant.tenant, tenant.id),
        '23502', 'null value in column "user_id" of relation "tenant_consent" violates not-null constraint',
        'consent requires the original user ID');
    return query select throws_ok(
        format('insert into internal.tenant_consent (user_id, user_email, terms_id, tenant_name)
                values (%L, %L, %L, %L)',
               consent_user, consent.user_email, terms.id, tenant.tenant),
        '23502', 'null value in column "tenant_id" of relation "tenant_consent" violates not-null constraint',
        'consent requires the original tenant ID');

    return query select throws_ok(
        'update internal.legal_terms set text = ''Changed''',
        'P0001', 'legal terms are immutable; insert a new version',
        'legal terms reject updates');
    return query select throws_ok(
        'delete from internal.legal_terms',
        'P0001', 'legal terms are immutable; insert a new version',
        'legal terms reject deletes');
    -- CASCADE ensures the trigger, rather than the consent FK, rejects truncation.
    return query select throws_ok(
        'truncate internal.legal_terms cascade',
        'P0001', 'legal terms are immutable; insert a new version',
        'legal terms reject truncation');
    return query select throws_ok(
        'update internal.tenant_consent set user_email = ''changed@example.test''',
        'P0001', 'tenant consent is immutable; insert a new record',
        'consent rejects updates');
    return query select throws_ok(
        'delete from internal.tenant_consent',
        'P0001', 'tenant consent is immutable; insert a new record',
        'consent rejects deletes');
    return query select throws_ok(
        'truncate internal.tenant_consent',
        'P0001', 'tenant consent is immutable; insert a new record',
        'consent rejects truncation');

    delete from auth.users where id = consent_user;
    return query select ok(
        (select c = consent from internal.tenant_consent c where id = consent.id),
        'user deletion preserves the complete consent record');
    delete from public.tenants where id = tenant.id;
    return query select ok(
        (select c = consent from internal.tenant_consent c where id = consent.id),
        'tenant deletion preserves the complete consent record');
    return query select ok(
        (select t = terms from internal.legal_terms t where id = terms.id),
        'accepted legal terms remain unchanged');
end;
$$ language plpgsql;
