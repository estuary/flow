#[derive(Debug, Default)]
pub struct LegalTermsQuery;

#[derive(Debug, Copy, Clone, PartialEq, Eq, async_graphql::Enum, sqlx::Type)]
#[sqlx(type_name = "internal.legal_terms_type", rename_all = "snake_case")]
pub enum LegalTermsType {
    /// Master Services Agreement.
    Msa,
}

#[derive(async_graphql::SimpleObject)]
pub struct LegalTerms {
    pub id: models::Id,
    pub text: String,
}

#[async_graphql::Object]
impl LegalTermsQuery {
    /// Returns the latest legal terms of the given type.
    async fn legal_terms(
        &self,
        ctx: &async_graphql::Context<'_>,
        r#type: LegalTermsType,
    ) -> async_graphql::Result<Option<LegalTerms>> {
        let env = ctx.data::<crate::Envelope>()?;
        let row = sqlx::query_as::<_, (models::Id, String)>(
            "SELECT id, text FROM internal.legal_terms
             WHERE type = $1 ORDER BY version DESC LIMIT 1",
        )
        .bind(r#type)
        .fetch_optional(&env.pg_pool)
        .await?;

        Ok(row.map(|(id, text)| LegalTerms { id, text }))
    }
}

#[cfg(test)]
mod test {
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn legal_terms_requires_type(pool: sqlx::PgPool) {
        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let query = serde_json::json!({ "query": "{ legalTerms { id text } }" });

        let response: serde_json::Value = server.graphql(&query, None).await;
        insta::assert_json_snapshot!(response["errors"][0]["message"], @r#""Field \"legalTerms\" argument \"type\" of type \"QueryRoot\" is required but not provided""#);
        assert!(response["data"].is_null());
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn legal_terms_unauthenticated(pool: sqlx::PgPool) {
        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let msa = serde_json::json!({ "query": "{ legalTerms(type: MSA) { id text } }" });

        let seeded: serde_json::Value = server.graphql(&msa, None).await;
        assert!(seeded.get("errors").is_none(), "{seeded}");
        let terms = &seeded["data"]["legalTerms"];
        let expected_id: models::Id = sqlx::query_scalar(
            "SELECT id FROM internal.legal_terms WHERE type = 'msa' AND version = 4",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(terms["id"], expected_id.to_string());
        let text = terms["text"].as_str().unwrap();
        assert!(text.starts_with("# MASTER SERVICES AGREEMENT\n"));
    }
}
