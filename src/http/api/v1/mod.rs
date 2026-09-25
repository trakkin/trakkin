mod connections;
mod logs;
mod openapi;
mod providers;
pub(super) mod support;
mod workspaces;

use axum::{Router, routing::get};

use support::HttpState;

pub use openapi::openapi;

pub(super) fn router() -> Router<HttpState> {
    Router::new()
        .merge(connections::router())
        .merge(logs::router())
        .merge(providers::router())
        .merge(workspaces::router())
        .route("/api/v1/openapi.json", get(openapi::get_openapi))
}
