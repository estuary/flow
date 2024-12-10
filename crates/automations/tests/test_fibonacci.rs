use std::time::Duration;

mod fibonacci;

// Percentage of the time that task polls should randomly fail.
// Value should be in range [0, 1) where 0 never fails.
const FAILURE_RATE: f32 = 0.00;
// Fibonacci sequence index to calculate.
// Larger numbers require exponentially more work.
const SEQUENCE: i64 = 10;
// Expected value at `SEQUENCE` offset.
const EXPECT_VALUE: i64 = 55;
// Number of concurrent polls that may run.
const CONCURRENCY: u32 = 50;
// When idle, the interval between polls for ready-to-run tasks.
// Note that `automations` will also poll after task completions.
const DEQUEUE_INTERVAL: Duration = Duration::from_secs(5);
// The timeout before a task poll is considered to have failed,
// and is eligible for retry.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);
// Amount of time each poll sleeps before responding.
const SLEEP_FOR: Duration = Duration::from_secs(0);
// Database under test.
const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

#[tokio::test]
async fn test_fibonacci_bench() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let pool = sqlx::postgres::PgPool::connect(&FIXED_DATABASE_URL)
        .await
        .expect("connect");

    // This cleanup is not required for correctness, but makes it nicer to
    // visually review the internal.tasks table.
    sqlx::query!("DELETE FROM internal.tasks WHERE task_type = 32767;")
        .execute(&pool)
        .await
        .unwrap();

    let root_id = automations::next_task_id();

    sqlx::query!(
        "SELECT internal.create_task($1, 32767::SMALLINT, NULL::public.flowid)",
        root_id as models::Id
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query!(
        r#"SELECT internal.send_to_task($1, '00:00:00:00:00:00:00:00'::flowid, $2::JSON)"#,
        root_id as models::Id,
        sqlx::types::Json(fibonacci::Message { value: SEQUENCE })
            as sqlx::types::Json<fibonacci::Message>
    )
    .execute(&pool)
    .await
    .unwrap();

    let monitor = async {
        let mut ticker = tokio::time::interval(Duration::from_millis(500));

        loop {
            let _instant = ticker.tick().await;

            let record = sqlx::query!(
                r#"SELECT inner_state as "state: sqlx::types::Json<fibonacci::State>" FROM internal.tasks WHERE task_id = $1"#,
                root_id as models::Id
            )
            .fetch_one(&pool)
            .await
            .unwrap();

            if let Some(sqlx::types::Json(fibonacci::State::Waiting {
                partial,
                pending: 0,
            })) = record.state
            {
                tracing::info!(value = partial, "completed Fibonacci sequence");
                assert_eq!(partial, EXPECT_VALUE);
                break;
            }
        }
    };

    () = automations::Server::new()
        .register(fibonacci::Fibonacci {
            failure_rate: FAILURE_RATE,
            sleep_for: SLEEP_FOR,
        })
        .serve(
            CONCURRENCY,
            pool.clone(),
            DEQUEUE_INTERVAL,
            HEARTBEAT_TIMEOUT,
            monitor,
        )
        .await;
}
