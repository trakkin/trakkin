mod v1;

use std::sync::Arc;

use axum::Router;

use crate::application::facade::ApplicationApi;

use super::frontend;

pub use v1::openapi;

pub fn router(application: Arc<dyn ApplicationApi>) -> Router {
    v1::router()
        .fallback(frontend::fallback)
        .method_not_allowed_fallback(v1::support::method_not_allowed)
        .with_state(v1::support::state(application))
}
