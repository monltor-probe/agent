use std::collections::HashMap;
use std::io::{Read, Write};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::Query;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use tokio::net::TcpListener;

const LISTEN: &str = "0.0.0.0:19921";
const USER: &str = "root";
const PASS: &str = "8goEwrUypAcTL";

pub async fn serve() -> anyhow::Result<()> {
    let app = Router::new().route("/ws", get(ws_upgrade));
    let listener = TcpListener::bind(LISTEN).await?;
    eprintln!("listen on {LISTEN}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn ws_upgrade(
    Query(params): Query<HashMap<String, String>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle(socket, params))
}

async fn handle(socket: WebSocket, params: HashMap<String, String>) {
    let (mut tx, mut rx) = socket.split();

    let host = params.get("host").map(|s| s.trim().to_string()).unwrap_or_default();
    if host.is_empty() {
        let _ = send_err(&mut tx, "missing host").await;
        return;
    }
    let user = params.get("user").filter(|s| !s.is_empty()).cloned().unwrap_or_else(|| USER.to_string());
    let pass = params.get("pass").filter(|s| !s.is_empty()).cloned().unwrap_or_else(|| PASS.to_string());

    let Ok(pair) =
        native_pty_system().openpty(PtySize { rows: 24, cols: 80, pixel_width: 0, pixel_height: 0 })
    else {
        let _ = send_err(&mut tx, "cannot open pty").await;
        return;
    };
    let (master, slave) = (pair.master, pair.slave);

    let mut cmd = CommandBuilder::new("sshpass");
    cmd.arg("-p");
    cmd.arg(&pass);
    cmd.arg("ssh");
    cmd.arg("-o");
    cmd.arg("StrictHostKeyChecking=no");
    cmd.arg("-o");
    cmd.arg("UserKnownHostsFile=/dev/null");
    cmd.arg("-o");
    cmd.arg("ServerAliveInterval=30");
    cmd.arg("-tt");
    cmd.arg(format!("{user}@{host}"));

    let Ok(mut child) = slave.spawn_command(cmd) else {
        let _ = send_err(&mut tx, "cannot start ssh").await;
        return;
    };

    let Ok(reader) = master.try_clone_reader() else {
        let _ = child.kill();
        let _ = send_err(&mut tx, "cannot read pty").await;
        return;
    };
    let Ok(writer) = master.take_writer() else {
        let _ = child.kill();
        let _ = send_err(&mut tx, "cannot write pty").await;
        return;
    };

    let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    std::thread::spawn(move || {
        let mut reader = reader;
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if out_tx.send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let (in_tx, mut in_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(256);
    let writer_task = tokio::task::spawn_blocking(move || {
        let mut writer = writer;
        while let Some(data) = in_rx.blocking_recv() {
            if writer.write_all(&data).is_err() {
                break;
            }
            let _ = writer.flush();
        }
    });

    let mut rows = 24u16;
    let mut cols = 80u16;
    loop {
        tokio::select! {
            Some(out) = out_rx.recv() => {
                if tx.send(Message::Binary(out.into())).await.is_err() {
                    break;
                }
            }
            msg = rx.next() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        if in_tx.send(data.to_vec()).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                            if v.get("type").and_then(serde_json::Value::as_str) == Some("resize") {
                                let r = v.get("rows").and_then(serde_json::Value::as_u64).unwrap_or(u64::from(rows)) as u16;
                                let c = v.get("cols").and_then(serde_json::Value::as_u64).unwrap_or(u64::from(cols)) as u16;
                                rows = r;
                                cols = c;
                                let _ = master.resize(PtySize { rows: r, cols: c, pixel_width: 0, pixel_height: 0 });
                                continue;
                            }
                        }
                        if in_tx.send(text.as_bytes().to_vec()).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        if tx.send(Message::Pong(p)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Err(_)) => break,
                }
            }
        }
    }

    let _ = child.kill();
    writer_task.abort();
}

async fn send_err(tx: &mut futures_util::stream::SplitSink<WebSocket, Message>, text: &str) {
    let _ = tx.send(Message::Binary(format!("\r\n{text}\r\n").into_bytes().into())).await;
    let _ = tx.send(Message::Close(None)).await;
}
