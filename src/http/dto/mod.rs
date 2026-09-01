use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::application::error::{ApplicationError, ApplicationErrorCode};

pub(super) mod activity;
pub(super) mod connections;
pub(super) mod mappings;
pub(super) mod providers;
pub(super) mod sources;
pub(super) mod synchronization;
pub(super) mod tasks;
pub(super) mod workspaces;

pub(super) fn timestamp(value: OffsetDateTime) -> Result<String, ApplicationError> {
    value.format(&Rfc3339).map_err(|_| {
        ApplicationError::new(
            ApplicationErrorCode::Internal,
            "timestamp could not be represented",
        )
    })
}
