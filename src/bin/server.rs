use axum::{
    Json,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use pg_queue::{QueueJobStatus, init_pool, init_tracing, run_migrations};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use std::env;
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};
use utoipa_scalar::{Scalar, Servable};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    pool: PgPool,
}

#[derive(Debug, Serialize, FromRow, utoipa::ToSchema)]
struct Segment {
    id: Uuid,
    #[schema(example = "us-west-power-users")]
    name: String,
    recalculated_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct CreateSegment {
    #[schema(example = "us-west-power-users")]
    name: String,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct UpdateSegment {
    #[schema(example = "eu-power-users")]
    name: Option<String>,
}

#[derive(Debug, Serialize, FromRow, utoipa::ToSchema)]
struct SegmentRecalculateJob {
    id: i64,
    segment_id: Uuid,
    status: QueueJobStatus,
    scheduled_for: DateTime<Utc>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct EnqueueSegmentRecalculate {
    segment_id: Uuid,
}

enum ApiError {
    NotFound,
    Db(sqlx::Error),
}

impl From<sqlx::Error> for ApiError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::RowNotFound => Self::NotFound,
            sqlx::Error::Database(ref db_err) if db_err.code().as_deref() == Some("23503") => {
                Self::NotFound
            }
            other => Self::Db(other),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
            ApiError::Db(err) => {
                tracing::error!(?err, "database error");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "pg-queue api",
        version = "0.1.0",
        description = "Segments + segment-recalculate queue backed by Postgres — testbed for pg-as-queue."
    ),
    tags(
        (name = "segments", description = "Segments CRUD"),
        (name = "segment-recalculate-queue", description = "Enqueue segment recalculation jobs"),
        (name = "health", description = "Health check")
    )
)]
struct ApiDoc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    init_tracing();

    let db_url = env::var("DATABASE_URL")?;
    let pool = init_pool(&db_url).await?;
    run_migrations(&pool).await?;

    let state = AppState { pool };

    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(health))
        .routes(routes!(list_segments, create_segment))
        .routes(routes!(get_segment, update_segment, delete_segment))
        .routes(routes!(enqueue_recalculate))
        .with_state(state)
        .split_for_parts();

    let openapi_json = api.to_pretty_json().expect("openapi serializes");

    let app = router
        .route(
            "/openapi.json",
            get(move || {
                let json = openapi_json.clone();
                async move { ([(header::CONTENT_TYPE, "application/json")], json) }
            }),
        )
        .merge(Scalar::with_url("/scalar", api));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    let addr = listener.local_addr()?;
    tracing::info!("listening on http://{addr}");
    tracing::info!("scalar ui:   http://{addr}/scalar");
    tracing::info!("openapi:     http://{addr}/openapi.json");
    axum::serve(listener, app).await?;

    Ok(())
}

#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses((status = 200, description = "OK", body = String))
)]
async fn health() -> &'static str {
    "ok"
}

#[utoipa::path(
    get,
    path = "/segments",
    tag = "segments",
    responses((status = 200, description = "List all segments", body = Vec<Segment>))
)]
async fn list_segments(State(state): State<AppState>) -> Result<Json<Vec<Segment>>, ApiError> {
    let segments = sqlx::query_as::<_, Segment>(
        "SELECT id, name, recalculated_at, created_at, updated_at
         FROM segments
         ORDER BY created_at DESC",
    )
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(segments))
}

#[utoipa::path(
    post,
    path = "/segments",
    tag = "segments",
    request_body = CreateSegment,
    responses(
        (status = 201, description = "Segment created", body = Segment),
        (status = 500, description = "Internal error")
    )
)]
async fn create_segment(
    State(state): State<AppState>,
    Json(payload): Json<CreateSegment>,
) -> Result<(StatusCode, Json<Segment>), ApiError> {
    let segment = sqlx::query_as::<_, Segment>(
        "INSERT INTO segments (name) VALUES ($1)
         RETURNING id, name, recalculated_at, created_at, updated_at",
    )
    .bind(&payload.name)
    .fetch_one(&state.pool)
    .await?;
    Ok((StatusCode::CREATED, Json(segment)))
}

#[utoipa::path(
    get,
    path = "/segments/{id}",
    tag = "segments",
    params(("id" = Uuid, Path, description = "Segment id")),
    responses(
        (status = 200, description = "OK", body = Segment),
        (status = 404, description = "Not found")
    )
)]
async fn get_segment(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Segment>, ApiError> {
    let segment = sqlx::query_as::<_, Segment>(
        "SELECT id, name, recalculated_at, created_at, updated_at
         FROM segments
         WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&state.pool)
    .await?;
    Ok(Json(segment))
}

#[utoipa::path(
    patch,
    path = "/segments/{id}",
    tag = "segments",
    params(("id" = Uuid, Path, description = "Segment id")),
    request_body = UpdateSegment,
    responses(
        (status = 200, description = "Segment updated", body = Segment),
        (status = 404, description = "Not found")
    )
)]
async fn update_segment(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateSegment>,
) -> Result<Json<Segment>, ApiError> {
    let segment = sqlx::query_as::<_, Segment>(
        "UPDATE segments
         SET name = COALESCE($2, name),
             updated_at = NOW()
         WHERE id = $1
         RETURNING id, name, recalculated_at, created_at, updated_at",
    )
    .bind(id)
    .bind(payload.name.as_deref())
    .fetch_one(&state.pool)
    .await?;
    Ok(Json(segment))
}

#[utoipa::path(
    delete,
    path = "/segments/{id}",
    tag = "segments",
    params(("id" = Uuid, Path, description = "Segment id")),
    responses(
        (status = 204, description = "Segment deleted"),
        (status = 404, description = "Not found")
    )
)]
async fn delete_segment(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let result = sqlx::query("DELETE FROM segments WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(ApiError::NotFound);
    }
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/segment-recalculate-queue",
    tag = "segment-recalculate-queue",
    request_body = EnqueueSegmentRecalculate,
    responses(
        (status = 201, description = "Job enqueued", body = SegmentRecalculateJob),
        (status = 404, description = "Segment not found")
    )
)]
async fn enqueue_recalculate(
    State(state): State<AppState>,
    Json(payload): Json<EnqueueSegmentRecalculate>,
) -> Result<(StatusCode, Json<SegmentRecalculateJob>), ApiError> {
    let job = sqlx::query_as::<_, SegmentRecalculateJob>(
        "INSERT INTO segment_recalculate_queue (segment_id)
         VALUES ($1)
         RETURNING id, segment_id, status, scheduled_for, created_at, updated_at",
    )
    .bind(payload.segment_id)
    .fetch_one(&state.pool)
    .await?;
    Ok((StatusCode::CREATED, Json(job)))
}
