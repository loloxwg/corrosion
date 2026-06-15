//! HTTP API：register(KV) / counter / set 三类 CRDT 操作 + 成员 + 调试。

use std::sync::Arc;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::json;

use crate::gossip::Node;

pub fn router(node: Arc<Node>) -> Router {
    Router::new()
        // Register（覆盖型 KV，LWW）
        .route("/kv/{key}", get(get_kv).put(put_kv).delete(delete_kv))
        // Counter（PN-Counter）
        .route("/counter/{key}", get(get_counter).post(post_counter))
        // Set（OR-Set）
        .route("/set/{key}", get(get_set))
        .route("/set/{key}/add", post(set_add))
        .route("/set/{key}/remove", post(set_remove))
        .route("/members", get(members))
        .route("/debug", get(debug))
        .with_state(node)
}

// ---- Register ----

async fn put_kv(
    State(node): State<Arc<Node>>,
    Path(key): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    node.put(key, body.to_vec());
    StatusCode::OK
}

async fn delete_kv(State(node): State<Arc<Node>>, Path(key): Path<String>) -> impl IntoResponse {
    node.delete(key);
    StatusCode::NO_CONTENT
}

async fn get_kv(State(node): State<Arc<Node>>, Path(key): Path<String>) -> impl IntoResponse {
    match node.get(&key) {
        Some(value) => (StatusCode::OK, value).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

// ---- Counter ----

/// body 是整数增量，可负（如 "5" 或 "-3"）。
async fn post_counter(
    State(node): State<Arc<Node>>,
    Path(key): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    let s = String::from_utf8_lossy(&body);
    match s.trim().parse::<i64>() {
        Ok(delta) => {
            node.counter_add(key, delta);
            StatusCode::OK.into_response()
        }
        Err(_) => (StatusCode::BAD_REQUEST, "body 必须是整数").into_response(),
    }
}

async fn get_counter(State(node): State<Arc<Node>>, Path(key): Path<String>) -> impl IntoResponse {
    match node.counter_read(&key) {
        Some(v) => Json(json!({ "value": v })).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

// ---- Set ----

async fn set_add(
    State(node): State<Arc<Node>>,
    Path(key): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    node.set_add(key, String::from_utf8_lossy(&body).trim().to_string());
    StatusCode::OK
}

async fn set_remove(
    State(node): State<Arc<Node>>,
    Path(key): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    node.set_remove(key, String::from_utf8_lossy(&body).trim().to_string());
    StatusCode::OK
}

async fn get_set(State(node): State<Arc<Node>>, Path(key): Path<String>) -> impl IntoResponse {
    match node.set_read(&key) {
        Some(members) => Json(json!({ "members": members })).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

// ---- 运维 ----

async fn members(State(node): State<Arc<Node>>) -> impl IntoResponse {
    let list: Vec<_> = node
        .members()
        .into_iter()
        .map(|(id, addr, state)| json!({ "id": id, "addr": addr, "state": state }))
        .collect();
    Json(json!({ "self": node.id, "members": list }))
}

async fn debug(State(node): State<Arc<Node>>) -> impl IntoResponse {
    Json(node.debug_dump())
}
