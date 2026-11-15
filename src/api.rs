//! The HTTP side of the indexer: `/health` and `/metrics`. It asks the store and the metrics
//! and nothing else; the explorer API itself is Hasura over the database.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderName, StatusCode, header};
use axum::routing::get;
use tracing::warn;

use crate::metrics::Metrics;
use crate::store::Store;

#[derive(Clone)]
struct ApiState {
    store: Store,
    metrics: Arc<Metrics>,
}

pub fn router(store: Store, metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(prometheus))
        .with_state(ApiState { store, metrics })
}

/// 200 only when the database answers. A node that is down does not fail this check: the
/// indexer keeps serving what it has and shows the lag on /metrics.
/// Docker calls it every 10 seconds, and it is one `SELECT 1`.
async fn health(State(state): State<ApiState>) -> StatusCode {
    match state.store.health().await {
        Ok(()) => StatusCode::OK,
        Err(e) => {
            warn!("health check failed: {e}");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

async fn prometheus(State(state): State<ApiState>) -> ([(HeaderName, &'static str); 1], String) {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
}
