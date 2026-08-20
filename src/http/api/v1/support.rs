use std::sync::Arc;

use crate::{application::facade::ApplicationApi, http::error::HttpError};
use uuid::Uuid;

#[derive(Clone)]
pub(in crate::http::api) struct HttpState {
    pub(super) application: Arc<dyn ApplicationApi>,
}

pub(in crate::http::api) fn state(application: Arc<dyn ApplicationApi>) -> HttpState {
    HttpState { application }
}

pub(in crate::http::api) async fn method_not_allowed() -> HttpError {
    HttpError::method_not_allowed()
}

pub(super) fn generate_uuid() -> Result<Uuid, HttpError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|_| {
        HttpError::from(crate::application::error::ApplicationError::new(
            crate::application::error::ApplicationErrorCode::Internal,
            "workspace identity could not be created",
        ))
    })?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(bytes))
}
