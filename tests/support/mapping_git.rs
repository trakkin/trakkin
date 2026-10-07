use axum::{
    Router,
    body::{Body, Bytes},
    extract::{Request, State},
    http::{HeaderName, HeaderValue, StatusCode},
    response::Response,
};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    process::Command,
    sync::{Notify, watch},
    task::JoinHandle,
};
use trakkin_mappings_corpus::git::Repository;
use trakkin_mappings_language::Decisions;

#[derive(Clone)]
pub struct GitService {
    root: PathBuf,
    pub pause: watch::Sender<bool>,
    pub requested: Arc<Notify>,
}

pub struct GitServer {
    pub source: Repository,
    pub remote: String,
    pub service: GitService,
    task: JoinHandle<()>,
}

impl GitServer {
    pub async fn open(root: &Path) -> Self {
        let source = Repository::local(&root.join("community")).await.unwrap();
        let (pause, _) = watch::channel(false);
        let service = GitService {
            root: root.to_owned(),
            pause,
            requested: Arc::new(Notify::new()),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = format!("http://{}/community", listener.local_addr().unwrap());
        let router = Router::new().fallback(git_http).with_state(service.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            source,
            remote,
            service,
            task,
        }
    }

    pub async fn commit(&self, accepted: &str) -> String {
        let current = self.source.current().await.unwrap();
        self.source
            .commit(
                &current.commit,
                Decisions::parse(accepted, "").unwrap(),
                "Update fixture corpus",
            )
            .await
            .unwrap()
            .commit
    }

    pub async fn wait_for_fetch(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.service.requested.notified())
            .await
            .expect("Git HTTP request starts");
    }
}

impl Drop for GitServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn git_http(
    State(service): State<GitService>,
    request: Request,
) -> Result<Response, (StatusCode, String)> {
    let mut pause = service.pause.subscribe();
    loop {
        let paused = *pause.borrow_and_update();
        if !paused {
            break;
        }
        service.requested.notify_one();
        pause
            .changed()
            .await
            .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    }
    let (parts, body) = request.into_parts();
    let body = axum::body::to_bytes(body, 64 * 1024 * 1024)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let mut command = Command::new("git");
    command
        .arg("http-backend")
        .env("GIT_PROJECT_ROOT", &service.root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("REQUEST_METHOD", parts.method.as_str())
        .env("PATH_INFO", parts.uri.path())
        .env("QUERY_STRING", parts.uri.query().unwrap_or(""))
        .env(
            "CONTENT_TYPE",
            parts
                .headers
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .unwrap_or(""),
        )
        .env("CONTENT_LENGTH", body.len().to_string())
        .env(
            "HTTP_GIT_PROTOCOL",
            parts
                .headers
                .get("git-protocol")
                .and_then(|value| value.to_str().ok())
                .unwrap_or(""),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().unwrap();
    child.stdin.take().unwrap().write_all(&body).await.unwrap();
    let output = child.wait_with_output().await.unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let boundary = output
        .stdout
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .expect("Git CGI response has headers");
    let mut response = Response::new(Body::from(Bytes::copy_from_slice(
        &output.stdout[boundary + 4..],
    )));
    for line in std::str::from_utf8(&output.stdout[..boundary])
        .unwrap()
        .split("\r\n")
    {
        let (name, value) = line.split_once(':').unwrap();
        if name.eq_ignore_ascii_case("status") {
            *response.status_mut() = value.trim().split_once(' ').unwrap().0.parse().unwrap();
        } else {
            response.headers_mut().append(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value.trim()).unwrap(),
            );
        }
    }
    Ok(response)
}
