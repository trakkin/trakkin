use axum::{
    http::{Method, StatusCode, Uri},
    response::{IntoResponse, Response},
};

use super::error::HttpError;

#[cfg(feature = "bundled")]
use std::borrow::Cow;

#[cfg(feature = "bundled")]
use axum::{
    body::Body,
    http::{
        HeaderValue,
        header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG},
    },
};

#[cfg(feature = "bundled")]
use rust_embed::Embed;

#[cfg(feature = "bundled")]
#[derive(Embed)]
#[folder = "frontend/"]
#[include = "build/**"]
#[allow_missing]
struct FrontendAssets;

pub(super) async fn fallback(method: Method, uri: Uri) -> Response {
    if uri.path() == "/api" || uri.path().starts_with("/api/") {
        return HttpError::not_found().into_response();
    }
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }

    #[cfg(feature = "bundled")]
    {
        let requested_path = uri.path().trim_start_matches('/');
        let asset_path = if requested_path.is_empty() {
            "index.html"
        } else {
            requested_path
        };
        if let Some(file) = frontend_asset(asset_path) {
            return asset_response(&method, asset_path, file);
        }
        if !asset_path
            .rsplit('/')
            .next()
            .is_some_and(|name| name.contains('.'))
            && let Some(index) = frontend_asset("index.html")
        {
            return asset_response(&method, "index.html", index);
        }
    }

    StatusCode::NOT_FOUND.into_response()
}

#[cfg(feature = "bundled")]
fn frontend_asset(path: &str) -> Option<rust_embed::EmbeddedFile> {
    FrontendAssets::get(&format!("build/{path}"))
}

#[cfg(feature = "bundled")]
fn asset_response(method: &Method, path: &str, file: rust_embed::EmbeddedFile) -> Response {
    let length = file.data.len();
    let hash = hex::encode(file.metadata.sha256_hash());
    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        match file.data {
            Cow::Borrowed(data) => Body::from(data),
            Cow::Owned(data) => Body::from(data),
        }
    };
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(mime_guess::from_path(path).first_or_octet_stream().as_ref())
            .expect("MIME type is a valid header value"),
    );
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&length.to_string()).expect("asset length is a valid header value"),
    );
    headers.insert(
        ETAG,
        HeaderValue::from_str(&format!("\"{hash}\"")).expect("asset hash is a valid header value"),
    );
    headers.insert(
        CACHE_CONTROL,
        if path.starts_with("_app/immutable/") {
            HeaderValue::from_static("public, max-age=31536000, immutable")
        } else {
            HeaderValue::from_static("no-cache")
        },
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    response
}
