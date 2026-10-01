#[derive(Debug, Default)]
pub struct LegalTermsQuery;

#[derive(Debug, Copy, Clone, PartialEq, Eq, async_graphql::Enum, sqlx::Type)]
#[sqlx(type_name = "internal.legal_terms_type", rename_all = "snake_case")]
pub enum LegalTermsType {
    /// Master Services Agreement.
    Msa,
    PrivacyPolicy,
}

#[derive(async_graphql::SimpleObject)]
pub struct LegalTerms {
    pub text: String,
    pub version: i32,
}

#[async_graphql::Object]
impl LegalTermsQuery {
    /// Returns the legal terms of the given type with the highest version.
    async fn legal_terms(
        &self,
        ctx: &async_graphql::Context<'_>,
        r#type: LegalTermsType,
    ) -> async_graphql::Result<Option<LegalTerms>> {
        let env = ctx.data::<crate::Envelope>()?;
        let row = sqlx::query_as::<_, (String, i32)>(
            "SELECT text, version FROM internal.legal_terms
             WHERE type = $1 ORDER BY version DESC LIMIT 1",
        )
        .bind(r#type)
        .fetch_optional(&env.pg_pool)
        .await?;

        Ok(row.map(|(text, version)| LegalTerms { text, version }))
    }
}

#[cfg(test)]
mod test {
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn legal_terms_unauthenticated(pool: sqlx::PgPool) {
        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let msa = serde_json::json!({ "query": "{ legalTerms(type: MSA) { text version } }" });

        let seeded: serde_json::Value = server.graphql(&msa, None).await;
        assert!(seeded.get("errors").is_none(), "{seeded}");
        let terms = &seeded["data"]["legalTerms"];
        assert_eq!(terms["version"], 3);
        let text = terms["text"].as_str().unwrap();
        assert!(text.starts_with("# MASTER SERVICES AGREEMENT\n"));
    }
}
