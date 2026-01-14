use sqlx::Executor;

#[sqlx::test(migrations = "../../supabase/migrations")]
async fn test_example(pool: sqlx::PgPool) -> Result<(), sqlx::Error> {
    let result = pool.execute("SELECT internal.id_generator()").await?;
    assert_eq!(result.rows_affected(), 1);
    Ok(())
}
