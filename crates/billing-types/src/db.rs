/// Record `customer_id` as the Stripe customer of `tenant`, which callers do
/// after creating the customer. A different customer already recorded for the
/// tenant is an error rather than
/// being overwritten: the tenant would otherwise be billed to two customers.
/// A missing tenant is an error.
pub async fn record_customer_id(
    pool: &sqlx::PgPool,
    tenant: &str,
    customer_id: &stripe::CustomerId,
) -> anyhow::Result<()> {
    let recorded = sqlx::query_scalar!(
        r#"
        update public.tenants
        set stripe_customer_id = coalesce(stripe_customer_id, $2)
        where tenant = $1
        returning stripe_customer_id as "stripe_customer_id!"
        "#,
        tenant,
        customer_id.as_str(),
    )
    .fetch_optional(pool)
    .await?;

    match recorded {
        None => anyhow::bail!("tenant '{tenant}' does not exist"),
        Some(recorded) if recorded != customer_id.as_str() => anyhow::bail!(
            "tenant '{tenant}' is billed to Stripe customer {recorded}, not {customer_id}"
        ),
        _ => Ok(()),
    }
}
