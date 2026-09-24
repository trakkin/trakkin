use std::{fs, io, panic, path::Path};

use rolling_file::{BasicRollingFileAppender, RollingConditionBasic};
use tracing::error;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    EnvFilter, Layer,
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

const STDERR_FILTER: &str = "warn,trakkin=info,trakkin::provider=warn";
const FILE_FILTER: &str = "warn,trakkin=debug";
const MAXIMUM_LOG_FILE_BYTES: u64 = 8 * 1024 * 1024;
const RETAINED_LOG_ARCHIVES: usize = 7;

pub(crate) struct LoggingGuard {
    _file_writer: WorkerGuard,
}

pub(crate) fn initialize(data_directory: &Path) -> Result<LoggingGuard, io::Error> {
    let log_directory = data_directory.join("logs");
    fs::create_dir_all(&log_directory)?;
    let file_appender = rolling_file_appender(
        &log_directory,
        MAXIMUM_LOG_FILE_BYTES,
        RETAINED_LOG_ARCHIVES,
    )?;
    let (file_writer, file_writer_guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .compact()
                .with_span_events(FmtSpan::CLOSE)
                .with_writer(io::stderr)
                .with_filter(EnvFilter::new(STDERR_FILTER)),
        )
        .with(
            fmt::layer()
                .json()
                .with_ansi(false)
                .with_span_events(FmtSpan::CLOSE)
                .with_writer(file_writer)
                .with_filter(EnvFilter::new(FILE_FILTER)),
        )
        .try_init()
        .map_err(io::Error::other)?;
    install_panic_hook();

    Ok(LoggingGuard {
        _file_writer: file_writer_guard,
    })
}

fn rolling_file_appender(
    log_directory: &Path,
    maximum_file_bytes: u64,
    retained_archives: usize,
) -> Result<BasicRollingFileAppender, io::Error> {
    let condition = RollingConditionBasic::new()
        .daily()
        .max_size(maximum_file_bytes);
    BasicRollingFileAppender::new(
        log_directory.join("trakkin.jsonl"),
        condition,
        retained_archives,
    )
}

fn install_panic_hook() {
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic| {
        let message = panic
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.payload().downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic payload");
        let (file, line, column) = panic.location().map_or(("unknown", 0, 0), |location| {
            (location.file(), location.line(), location.column())
        });
        error!(
            panic.message = message,
            panic.file = file,
            panic.line = line,
            panic.column = column,
            "process panicked"
        );
        default_hook(panic);
    }));
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write};

    use tempfile::tempdir;

    use super::rolling_file_appender;

    #[test]
    fn retained_logs_roll_by_size_and_bound_archive_count() {
        let directory = tempdir().expect("temporary log directory");
        let mut appender =
            rolling_file_appender(directory.path(), 4, 2).expect("rolling log appender");

        for contents in [b"aaaa", b"bbbb", b"cccc", b"dddd"] {
            appender.write_all(contents).expect("log write");
        }
        appender.flush().expect("log flush");

        assert_eq!(
            fs::read(directory.path().join("trakkin.jsonl")).expect("active log"),
            b"dddd"
        );
        assert_eq!(
            fs::read(directory.path().join("trakkin.jsonl.1")).expect("newest archive"),
            b"cccc"
        );
        assert_eq!(
            fs::read(directory.path().join("trakkin.jsonl.2")).expect("oldest archive"),
            b"bbbb"
        );
        assert!(!directory.path().join("trakkin.jsonl.3").exists());
    }
}
