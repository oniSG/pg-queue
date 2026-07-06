//! Queue worker.
//!
//! Loop: claim one pending job atomically (SELECT … FOR UPDATE SKIP LOCKED),
//! simulate work, then commit success or failure. Multiple worker replicas
//! never claim the same job because SKIP LOCKED bypasses rows another txn
//! already holds.

use std::env;
use std::time::Duration;

use pg_queue::{init_pool, init_tracing, run_migrations};
use rand::Rng;
use sqlx::PgPool;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

/// How long to sleep when the queue is empty.
const POLL_INTERVAL: Duration = Duration::from_millis(1000);
/// Max attempts before a job is marked `failed` (terminal).
const MAX_ATTEMPTS: i64 = 3;
/// Delay applied to `scheduled_for` when a job goes back to `pending` for retry.
const RETRY_BACKOFF: Duration = Duration::from_secs(5);
/// Simulated work range.
const WORK_MIN: Duration = Duration::from_millis(100);
const WORK_MAX: Duration = Duration::from_millis(500);
/// Simulated failure probability, so the retry path is exercised.
const FAIL_RATE: f64 = 0.15;

#[derive(Debug)]
struct ClaimedJob {
    id: i64,
    segment_id: Uuid,
    execution_id: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    init_tracing();

    let worker_id = env::var("HOSTNAME").unwrap_or_else(|_| "worker-local".into());
    let db_url = env::var("DATABASE_URL")?;
    let pool = init_pool(&db_url).await?;
    run_migrations(&pool).await?;

    info!(worker = %worker_id, "worker starting");

    loop {
        match claim_next(&pool).await {
            Ok(Some(job)) => process(&pool, &job, &worker_id).await,
            Ok(None) => sleep(POLL_INTERVAL).await,
            Err(err) => {
                error!(?err, worker = %worker_id, "claim failed, backing off");
                sleep(RETRY_BACKOFF).await;
            }
        }
    }
}

/// Atomically move one ready job from `pending` → `running` and insert its
/// execution row. Returns `None` when nothing is ready.
async fn claim_next(pool: &PgPool) -> anyhow::Result<Option<ClaimedJob>> {
    let mut tx = pool.begin().await?;

    let picked: Option<(i64, Uuid)> = sqlx::query_as(
        "UPDATE segment_recalculate_queue AS srq
            SET status = 'running', updated_at = NOW()
          WHERE srq.id = (
                SELECT id
                  FROM segment_recalculate_queue
                 WHERE status = 'pending' AND scheduled_for <= NOW()
                 ORDER BY scheduled_for, id
                   FOR UPDATE SKIP LOCKED
                 LIMIT 1
          )
         RETURNING srq.id, srq.segment_id",
    )
    .fetch_optional(&mut *tx)
    .await?;

    let Some((job_id, segment_id)) = picked else {
        tx.rollback().await?;
        return Ok(None);
    };

    let execution_id: i64 = sqlx::query_scalar(
        "INSERT INTO queue_executions (queue_id) VALUES ($1) RETURNING id",
    )
    .bind(job_id)
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(Some(ClaimedJob {
        id: job_id,
        segment_id,
        execution_id,
    }))
}

async fn process(pool: &PgPool, job: &ClaimedJob, worker_id: &str) {
    info!(
        worker = %worker_id,
        job_id = job.id,
        execution_id = job.execution_id,
        segment_id = %job.segment_id,
        "claimed"
    );

    match do_work().await {
        Ok(duration_ms) => match finish_success(pool, job).await {
            Ok(()) => info!(worker = %worker_id, job_id = job.id, duration_ms, "succeeded"),
            Err(err) => error!(?err, worker = %worker_id, job_id = job.id, "failed to record success"),
        },
        Err(reason) => match finish_failure(pool, job, &reason).await {
            Ok(true) => warn!(worker = %worker_id, job_id = job.id, reason, "attempt failed, will retry"),
            Ok(false) => warn!(worker = %worker_id, job_id = job.id, reason, "attempt failed, giving up"),
            Err(err) => error!(?err, worker = %worker_id, job_id = job.id, "failed to record failure"),
        },
    }
}

/// Pretend to do the recalculation. Returns the elapsed ms on success, or a
/// reason string on failure.
async fn do_work() -> Result<u64, String> {
    // Scope the RNG so it drops before the await (ThreadRng is !Send).
    let (delay_ms, should_fail) = {
        let mut rng = rand::thread_rng();
        (
            rng.gen_range(WORK_MIN.as_millis() as u64..=WORK_MAX.as_millis() as u64),
            rng.gen_bool(FAIL_RATE),
        )
    };
    sleep(Duration::from_millis(delay_ms)).await;
    if should_fail {
        Err(format!("simulated failure after {delay_ms}ms"))
    } else {
        Ok(delay_ms)
    }
}

/// Mark the execution + queue row succeeded and bump `segments.recalculated_at`,
/// all in one txn.
async fn finish_success(pool: &PgPool, job: &ClaimedJob) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;

    sqlx::query(
        "UPDATE queue_executions
            SET status = 'succeeded', ended_at = NOW()
          WHERE id = $1",
    )
    .bind(job.execution_id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE segment_recalculate_queue
            SET status = 'succeeded', updated_at = NOW()
          WHERE id = $1",
    )
    .bind(job.id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE segments
            SET recalculated_at = NOW(), updated_at = NOW()
          WHERE id = $1",
    )
    .bind(job.segment_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

/// Record the failure on the execution, then either send the queue row back to
/// `pending` (with a backoff on `scheduled_for`) if we have retries left, or
/// mark it `failed` terminally. Returns whether the job will be retried.
async fn finish_failure(pool: &PgPool, job: &ClaimedJob, reason: &str) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;

    sqlx::query(
        "UPDATE queue_executions
            SET status = 'failed',
                ended_at = NOW(),
                error = jsonb_build_object('reason', $2::text)
          WHERE id = $1",
    )
    .bind(job.execution_id)
    .bind(reason)
    .execute(&mut *tx)
    .await?;

    let attempts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM queue_executions WHERE queue_id = $1")
            .bind(job.id)
            .fetch_one(&mut *tx)
            .await?;

    let will_retry = attempts < MAX_ATTEMPTS;

    if will_retry {
        sqlx::query(
            "UPDATE segment_recalculate_queue
                SET status = 'pending',
                    scheduled_for = NOW() + $2::interval,
                    updated_at = NOW()
              WHERE id = $1",
        )
        .bind(job.id)
        .bind(format!("{} seconds", RETRY_BACKOFF.as_secs()))
        .execute(&mut *tx)
        .await?;
    } else {
        sqlx::query(
            "UPDATE segment_recalculate_queue
                SET status = 'failed', updated_at = NOW()
              WHERE id = $1",
        )
        .bind(job.id)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(will_retry)
}
