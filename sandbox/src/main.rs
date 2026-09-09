use axum::{routing::get, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use std::process::Stdio;
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::{timeout, Duration};

const MAX_OUTPUT_BYTES: u64 = 64 * 1024;
const DEFAULT_TIMEOUT_SECS: u64 = 30;
const MAX_TIMEOUT_SECS: u64 = 120;

#[derive(Deserialize)]
struct RunRequest {
    repo_path: String,
    command: String,
    timeout_secs: Option<u64>,
}

#[derive(Serialize)]
struct RunResponse {
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    timed_out: bool,
    duration_ms: u128,
}

async fn read_capped<R: AsyncReadExt + Unpin>(reader: R) -> String {
    let mut buf = Vec::new();
    let mut limited = reader.take(MAX_OUTPUT_BYTES);
    let _ = limited.read_to_end(&mut buf).await;
    String::from_utf8_lossy(&buf).to_string()
}

fn kill_group(pid: u32) {
    if pid == 0 {
        return;
    }
    unsafe {
        libc::kill(-(pid as i32), libc::SIGKILL);
    }
}

async fn run(Json(req): Json<RunRequest>) -> Json<RunResponse> {
    let started = Instant::now();

    let secs = req
        .timeout_secs
        .unwrap_or(DEFAULT_TIMEOUT_SECS)
        .min(MAX_TIMEOUT_SECS);

    let spawned = Command::new("sh")
        .arg("-c")
        .arg(&req.command)
        .current_dir(&req.repo_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::null())
        .process_group(0)
        .kill_on_drop(true)
        .spawn();

    let mut child = match spawned {
        Ok(c) => c,
        Err(e) => {
            return Json(RunResponse {
                exit_code: None,
                stdout: String::new(),
                stderr: format!("could not start command: {e}"),
                timed_out: false,
                duration_ms: started.elapsed().as_millis(),
            })
        }
    };

    let pid = child.id().unwrap_or(0);
    let out = child.stdout.take().expect("stdout piped");
    let err = child.stderr.take().expect("stderr piped");

    let collect = async {
        let (o, e, status) = tokio::join!(read_capped(out), read_capped(err), child.wait());
        (o, e, status)
    };

    match timeout(Duration::from_secs(secs), collect).await {
        Ok((stdout, stderr, status)) => Json(RunResponse {
            exit_code: status.ok().and_then(|s| s.code()),
            stdout,
            stderr,
            timed_out: false,
            duration_ms: started.elapsed().as_millis(),
        }),
        Err(_) => {
            kill_group(pid);
            Json(RunResponse {
                exit_code: None,
                stdout: String::new(),
                stderr: format!("command exceeded the {secs}s limit and was killed"),
                timed_out: true,
                duration_ms: started.elapsed().as_millis(),
            })
        }
    }
}

async fn health() -> &'static str {
    "ok"
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/health", get(health))
        .route("/run", post(run));

    let port = std::env::var("PORT").unwrap_or_else(|_| "4000".into());
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");

    println!("healix-sandbox listening on {addr}");
    axum::serve(listener, app).await.expect("serve");
}
