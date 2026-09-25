use std::{
    collections::VecDeque,
    convert::Infallible,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Query, State, rejection::QueryRejection},
    http::HeaderMap,
    response::sse::{Event, KeepAlive, Sse},
    routing::get,
};
use futures_util::{Stream, stream};
use sha2::{Digest, Sha256};

use crate::{
    application::error::{ApplicationError, ApplicationErrorCode},
    http::{
        dto::logs::{
            LogEntryResponse, LogPageResponse, LogQueryParameters, LogStreamQueryParameters,
        },
        error::{ErrorResponse, HttpError},
    },
};

use super::support::HttpState;

const DEFAULT_LOG_LIMIT: usize = 200;
const MAXIMUM_LOG_LIMIT: usize = 1_000;
const RETAINED_LOG_ARCHIVES: usize = 7;
const STREAM_POLL_INTERVAL: Duration = Duration::from_millis(750);

pub(super) fn router() -> Router<HttpState> {
    Router::new()
        .route("/api/v1/logs", get(list_logs))
        .route("/api/v1/logs/stream", get(stream_logs))
}

#[utoipa::path(get, path = "/api/v1/logs", params(LogQueryParameters), responses((status = 200, body = LogPageResponse), (status = 400, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "logs")]
pub(in crate::http::api::v1) async fn list_logs(
    State(state): State<HttpState>,
    query: Result<Query<LogQueryParameters>, QueryRejection>,
) -> Result<Json<LogPageResponse>, HttpError> {
    let Query(query) = query.map_err(|error| HttpError::invalid_request(error.body_text()))?;
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LOG_LIMIT)
        .clamp(1, MAXIMUM_LOG_LIMIT);
    Ok(Json(
        read_logs(
            &state.data_directory,
            query.before.as_deref(),
            query.after.as_deref(),
            limit,
        )
        .await?,
    ))
}

#[utoipa::path(get, path = "/api/v1/logs/stream", params(LogStreamQueryParameters), responses((status = 200, description = "Newly appended log entries", content_type = "text/event-stream", body = LogEntryResponse), (status = 400, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "logs")]
pub(in crate::http::api::v1) async fn stream_logs(
    State(state): State<HttpState>,
    headers: HeaderMap,
    query: Result<Query<LogStreamQueryParameters>, QueryRejection>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, HttpError> {
    let Query(query) = query.map_err(|error| HttpError::invalid_request(error.body_text()))?;
    let entries = read_log_entries(&state.data_directory).await?;
    let after = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .or(query.after.as_deref());
    let (cursor, pending) = initial_stream_position(entries, after)?;
    let stream = stream::unfold(
        LogStreamState {
            data_directory: state.data_directory.clone(),
            cursor,
            pending,
            reset_pending: false,
        },
        next_stream_event,
    );

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

struct LogStreamState {
    data_directory: PathBuf,
    cursor: Option<String>,
    pending: VecDeque<LogEntryResponse>,
    reset_pending: bool,
}

async fn next_stream_event(
    mut state: LogStreamState,
) -> Option<(Result<Event, Infallible>, LogStreamState)> {
    loop {
        if state.reset_pending {
            state.reset_pending = false;
            return Some((
                Ok(Event::default().event("reset").data("cursor_expired")),
                state,
            ));
        }
        if let Some(entry) = state.pending.pop_front() {
            state.cursor = Some(entry.cursor.clone());
            let data = serde_json::to_string(&entry).unwrap_or_else(|_| "{}".to_owned());
            let event = Event::default()
                .event("log")
                .id(entry.cursor.clone())
                .data(data);
            return Some((Ok(event), state));
        }

        tokio::time::sleep(STREAM_POLL_INTERVAL).await;
        let Ok(entries) = read_log_entries(&state.data_directory).await else {
            continue;
        };
        refill_stream(&mut state, entries);
    }
}

fn initial_stream_position(
    entries: Vec<LogEntryResponse>,
    after: Option<&str>,
) -> Result<(Option<String>, VecDeque<LogEntryResponse>), HttpError> {
    let Some(after) = after else {
        return Ok((
            entries.last().map(|entry| entry.cursor.clone()),
            VecDeque::new(),
        ));
    };
    let position = cursor_position(&entries, after, "after")?;
    Ok((
        Some(after.to_owned()),
        entries
            .into_iter()
            .skip(position.saturating_add(1))
            .collect(),
    ))
}

fn refill_stream(state: &mut LogStreamState, entries: Vec<LogEntryResponse>) {
    let Some(cursor) = state.cursor.as_deref() else {
        state.pending = entries.into();
        return;
    };
    let Some(position) = entries.iter().position(|entry| entry.cursor == cursor) else {
        state.cursor = entries.last().map(|entry| entry.cursor.clone());
        state.reset_pending = true;
        return;
    };
    state.pending = entries
        .into_iter()
        .skip(position.saturating_add(1))
        .collect();
}

async fn read_logs(
    data_directory: &Path,
    before: Option<&str>,
    after: Option<&str>,
    limit: usize,
) -> Result<LogPageResponse, HttpError> {
    if before.is_some() && after.is_some() {
        return Err(HttpError::invalid_field(
            "before",
            "mutually_exclusive",
            "before and after cannot be used together",
        ));
    }

    let entries = read_log_entries(data_directory).await?;
    let (start, end) = page_bounds(&entries, before, after, limit)?;
    Ok(LogPageResponse {
        entries: entries[start..end].to_vec(),
        has_more_before: start > 0,
        has_more_after: end < entries.len(),
    })
}

async fn read_log_entries(data_directory: &Path) -> Result<Vec<LogEntryResponse>, HttpError> {
    let mut entries = Vec::new();
    let log_directory = data_directory.join("logs");

    for archive in (0..=RETAINED_LOG_ARCHIVES).rev() {
        let filename = if archive == 0 {
            "trakkin.jsonl".to_owned()
        } else {
            format!("trakkin.jsonl.{archive}")
        };
        let contents = match tokio::fs::read_to_string(log_directory.join(filename)).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(ApplicationError::new(
                    ApplicationErrorCode::Internal,
                    format!("logs could not be read: {error}"),
                )
                .into());
            }
        };

        for line in contents.lines() {
            if let Ok(mut entry) = serde_json::from_str::<LogEntryResponse>(line) {
                entry.cursor = log_cursor(line);
                entries.push(entry);
            }
        }
    }

    Ok(entries)
}

fn page_bounds(
    entries: &[LogEntryResponse],
    before: Option<&str>,
    after: Option<&str>,
    limit: usize,
) -> Result<(usize, usize), HttpError> {
    if let Some(cursor) = before {
        let end = cursor_position(entries, cursor, "before")?;
        return Ok((end.saturating_sub(limit), end));
    }
    if let Some(cursor) = after {
        let start = cursor_position(entries, cursor, "after")?.saturating_add(1);
        return Ok((start, start.saturating_add(limit).min(entries.len())));
    }

    let end = entries.len();
    Ok((end.saturating_sub(limit), end))
}

fn cursor_position(
    entries: &[LogEntryResponse],
    cursor: &str,
    field: &'static str,
) -> Result<usize, HttpError> {
    entries
        .iter()
        .position(|entry| entry.cursor == cursor)
        .ok_or_else(|| {
            HttpError::invalid_field(
                field,
                "cursor_not_found",
                "log cursor is invalid or no longer retained",
            )
        })
}

fn log_cursor(line: &str) -> String {
    format!("v1_{}", hex::encode(Sha256::digest(line.as_bytes())))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::read_logs;

    #[tokio::test]
    async fn pages_retained_logs_in_both_directions_and_skips_incomplete_lines() {
        let directory = tempdir().expect("temporary data directory");
        let log_directory = directory.path().join("logs");
        fs::create_dir(&log_directory).expect("log directory is created");
        fs::write(
            log_directory.join("trakkin.jsonl.1"),
            concat!(
                "{\"timestamp\":\"2026-01-01T00:00:00Z\",\"level\":\"INFO\",\"fields\":{\"message\":\"oldest\"},\"target\":\"trakkin\"}\n",
                "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"level\":\"WARN\",\"fields\":{\"message\":\"older\"},\"target\":\"trakkin\"}\n",
            ),
        )
        .expect("archive is written");
        fs::write(
            log_directory.join("trakkin.jsonl"),
            concat!(
                "{\"timestamp\":\"2026-01-01T00:00:02Z\",\"level\":\"INFO\",\"fields\":{\"message\":\"newer\"},\"target\":\"trakkin\"}\n",
                "{\"timestamp\":\"2026-01-01T00:00:03Z\",\"level\":\"ERROR\",\"fields\":{\"message\":\"newest\"},\"target\":\"trakkin\"}\n",
                "{\"timestamp\":",
            ),
        )
        .expect("active log is written");

        let latest_page = match read_logs(directory.path(), None, None, 2).await {
            Ok(page) => page,
            Err(_) => panic!("logs should be read"),
        };

        assert_eq!(latest_page.entries.len(), 2);
        assert_eq!(latest_page.entries[0].fields["message"], "newer");
        assert_eq!(latest_page.entries[1].fields["message"], "newest");
        assert!(latest_page.has_more_before);
        assert!(!latest_page.has_more_after);

        let before = latest_page.entries[0].cursor.as_str();
        let older_page = match read_logs(directory.path(), Some(before), None, 2).await {
            Ok(page) => page,
            Err(_) => panic!("older logs should be read"),
        };
        assert_eq!(older_page.entries[0].fields["message"], "oldest");
        assert_eq!(older_page.entries[1].fields["message"], "older");
        assert!(!older_page.has_more_before);
        assert!(older_page.has_more_after);

        let after = older_page.entries[1].cursor.as_str();
        let newer_page = match read_logs(directory.path(), None, Some(after), 2).await {
            Ok(page) => page,
            Err(_) => panic!("newer logs should be read"),
        };
        assert_eq!(newer_page.entries[0].fields["message"], "newer");
        assert_eq!(newer_page.entries[1].fields["message"], "newest");
        assert!(newer_page.has_more_before);
        assert!(!newer_page.has_more_after);
    }
}
