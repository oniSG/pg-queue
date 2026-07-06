use axum::{
    Json,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, postgres::PgPoolOptions};
use std::env;
use tracing_subscriber::EnvFilter;
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};
use utoipa_scalar::{Scalar, Servable};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    pool: PgPool,
}

#[derive(Debug, Serialize, FromRow, utoipa::ToSchema)]
struct Todo {
    id: Uuid,
    #[schema(example = "buy milk")]
    title: String,
    done: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct CreateTodo {
    #[schema(example = "buy milk")]
    title: String,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct UpdateTodo {
    #[schema(example = "buy oat milk")]
    title: Option<String>,
    #[schema(example = true)]
    done: Option<bool>,
}

enum ApiError {
    NotFound,
    Db(sqlx::Error),
}

impl From<sqlx::Error> for ApiError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::RowNotFound => Self::NotFound,
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
        title = "pg-queue todo api",
        version = "0.1.0",
        description = "Minimal TODO API backed by Postgres — testbed for pg-as-queue."
    ),
    servers(
        (url = "http://localhost:3000", description = "local dev")
    ),
    tags(
        (name = "todos", description = "Todo CRUD"),
        (name = "health", description = "Health check")
    )
)]
struct ApiDoc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let db_url = env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&db_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    let state = AppState { pool };

    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(health))
        .routes(routes!(list_todos, create_todo))
        .routes(routes!(get_todo, update_todo, delete_todo))
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
    path = "/todos",
    tag = "todos",
    responses((status = 200, description = "List all todos", body = Vec<Todo>))
)]
async fn list_todos(State(state): State<AppState>) -> Result<Json<Vec<Todo>>, ApiError> {
    let todos = sqlx::query_as::<_, Todo>(
        "SELECT id, title, done, created_at, updated_at FROM todos ORDER BY created_at DESC",
    )
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(todos))
}

#[utoipa::path(
    post,
    path = "/todos",
    tag = "todos",
    request_body = CreateTodo,
    responses(
        (status = 201, description = "Todo created", body = Todo),
        (status = 500, description = "Internal error")
    )
)]
async fn create_todo(
    State(state): State<AppState>,
    Json(payload): Json<CreateTodo>,
) -> Result<(StatusCode, Json<Todo>), ApiError> {
    let todo = sqlx::query_as::<_, Todo>(
        "INSERT INTO todos (title) VALUES ($1)
         RETURNING id, title, done, created_at, updated_at",
    )
    .bind(&payload.title)
    .fetch_one(&state.pool)
    .await?;
    Ok((StatusCode::CREATED, Json(todo)))
}

#[utoipa::path(
    get,
    path = "/todos/{id}",
    tag = "todos",
    params(("id" = Uuid, Path, description = "Todo id")),
    responses(
        (status = 200, description = "OK", body = Todo),
        (status = 404, description = "Not found")
    )
)]
async fn get_todo(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Todo>, ApiError> {
    let todo = sqlx::query_as::<_, Todo>(
        "SELECT id, title, done, created_at, updated_at FROM todos WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&state.pool)
    .await?;
    Ok(Json(todo))
}

#[utoipa::path(
    patch,
    path = "/todos/{id}",
    tag = "todos",
    params(("id" = Uuid, Path, description = "Todo id")),
    request_body = UpdateTodo,
    responses(
        (status = 200, description = "Todo updated", body = Todo),
        (status = 404, description = "Not found")
    )
)]
async fn update_todo(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateTodo>,
) -> Result<Json<Todo>, ApiError> {
    let todo = sqlx::query_as::<_, Todo>(
        "UPDATE todos
         SET title = COALESCE($2, title),
             done  = COALESCE($3, done),
             updated_at = NOW()
         WHERE id = $1
         RETURNING id, title, done, created_at, updated_at",
    )
    .bind(id)
    .bind(payload.title.as_deref())
    .bind(payload.done)
    .fetch_one(&state.pool)
    .await?;
    Ok(Json(todo))
}

#[utoipa::path(
    delete,
    path = "/todos/{id}",
    tag = "todos",
    params(("id" = Uuid, Path, description = "Todo id")),
    responses(
        (status = 204, description = "Todo deleted"),
        (status = 404, description = "Not found")
    )
)]
async fn delete_todo(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let result = sqlx::query("DELETE FROM todos WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(ApiError::NotFound);
    }
    Ok(StatusCode::NO_CONTENT)
}
