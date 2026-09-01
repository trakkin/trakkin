mod v1;

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::Request,
    http::{HeaderName, HeaderValue},
    middleware::{self, Next},
    response::Response,
};
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tracing::Level;
use trakkin_provider_sdk::{InvocationContext, TRACEPARENT_HEADER, TRACESTATE_HEADER};

use crate::application::facade::ApplicationApi;

use super::frontend;

pub use v1::openapi;

const REQUEST_ID_HEADER: &str = "x-trakkin-request-id";

pub fn router(application: Arc<dyn ApplicationApi>) -> Router {
    v1::router()
        .fallback(frontend::fallback)
        .method_not_allowed_fallback(v1::support::method_not_allowed)
        .with_state(v1::support::state(application))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<Body>| {
                    tracing::span!(
                        Level::INFO,
                        "http.request",
                        http.request.method = %request.method(),
                        url.path = request.uri().path(),
                        trakkin.correlation_id = request
                            .headers()
                            .get(REQUEST_ID_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or(""),
                        otel.traceparent = request
                            .headers()
                            .get(TRACEPARENT_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or(""),
                    )
                })
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .layer(middleware::from_fn(request_context))
}

async fn request_context(mut request: Request, next: Next) -> Response {
    let request_id = request
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| InvocationContext::new(value, None, None).is_ok())
        .map(str::to_owned)
        .unwrap_or_else(|| crate::observability::generate_correlation_id("request"));
    let traceparent = request
        .headers()
        .get(TRACEPARENT_HEADER)
        .and_then(|value| value.to_str().ok());
    let tracestate = request
        .headers()
        .get(TRACESTATE_HEADER)
        .and_then(|value| value.to_str().ok());
    let context = InvocationContext::new(&request_id, traceparent, tracestate)
        .or_else(|_| InvocationContext::new(&request_id, traceparent, None))
        .unwrap_or_else(|_| InvocationContext::new(&request_id, None, None).unwrap());
    let request_id = HeaderValue::from_str(context.correlation_id())
        .expect("validated request correlation ID is an HTTP header value");
    request.headers_mut().insert(
        HeaderName::from_static(REQUEST_ID_HEADER),
        request_id.clone(),
    );
    if context.traceparent().is_none() {
        request.headers_mut().remove(TRACEPARENT_HEADER);
    }
    if context.tracestate().is_none() {
        request.headers_mut().remove(TRACESTATE_HEADER);
    }
    let mut response =
        crate::observability::in_invocation_context(context, next.run(request)).await;
    response
        .headers_mut()
        .insert(HeaderName::from_static(REQUEST_ID_HEADER), request_id);
    response
}

#[cfg(test)]
mod tests {
    use axum::{body::to_bytes, routing::get};
    use serde_json::json;
    use tower::ServiceExt;

    use super::*;

    async fn active_context(request: Request) -> axum::Json<serde_json::Value> {
        let context = crate::observability::current_invocation_context().unwrap();
        axum::Json(json!({
            "correlationId": context.correlation_id(),
            "traceparent": context.traceparent(),
            "tracestate": context.tracestate(),
            "requestTraceparent": request
                .headers()
                .get(TRACEPARENT_HEADER)
                .and_then(|value| value.to_str().ok()),
        }))
    }

    #[tokio::test]
    async fn request_context_propagates_valid_w3c_headers() {
        let router = Router::new()
            .route("/", get(active_context))
            .layer(middleware::from_fn(request_context));
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header(REQUEST_ID_HEADER, "request:42")
                    .header(
                        TRACEPARENT_HEADER,
                        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                    )
                    .header(TRACESTATE_HEADER, "vendor=value")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.headers()[REQUEST_ID_HEADER], "request:42");
        let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["correlationId"], "request:42");
        assert_eq!(
            body["traceparent"],
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
        assert_eq!(body["tracestate"], "vendor=value");
    }

    #[tokio::test]
    async fn request_context_sanitizes_invalid_trace_headers() {
        let router = Router::new()
            .route("/", get(active_context))
            .layer(middleware::from_fn(request_context));
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header(REQUEST_ID_HEADER, "request:43")
                    .header(TRACEPARENT_HEADER, "invalid")
                    .header(TRACESTATE_HEADER, "vendor=value")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(body["traceparent"].is_null());
        assert!(body["tracestate"].is_null());
        assert!(body["requestTraceparent"].is_null());
    }
}
