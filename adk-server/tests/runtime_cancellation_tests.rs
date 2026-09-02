use adk_core::{Agent, AgentLoader, EventStream, InvocationContext};
use adk_server::{ServerConfig, create_app};
use adk_session::InMemorySessionService;
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

struct EmptyAgent;

#[async_trait]
impl Agent for EmptyAgent {
    fn name(&self) -> &str {
        "empty"
    }

    fn description(&self) -> &str {
        "test agent"
    }

    fn sub_agents(&self) -> &[Arc<dyn Agent>] {
        &[]
    }

    async fn run(&self, _ctx: Arc<dyn InvocationContext>) -> adk_core::Result<EventStream> {
        Ok(Box::pin(futures::stream::empty()))
    }
}

struct EmptyAgentLoader;

#[async_trait]
impl AgentLoader for EmptyAgentLoader {
    async fn load_agent(&self, _app_name: &str) -> adk_core::Result<Arc<dyn Agent>> {
        Ok(Arc::new(EmptyAgent))
    }

    fn list_agents(&self) -> Vec<String> {
        vec!["test-app".to_string()]
    }

    fn root_agent(&self) -> Arc<dyn Agent> {
        Arc::new(EmptyAgent)
    }
}

fn test_app() -> axum::Router {
    create_app(ServerConfig::new(
        Arc::new(EmptyAgentLoader),
        Arc::new(InMemorySessionService::new()),
    ))
}

async fn start_run(app: &axum::Router, run_id: &str) -> axum::response::Response {
    let body = json!({
        "appName": "test-app",
        "userId": "user-1",
        "sessionId": "session-1",
        "runId": run_id,
        "newMessage": {
            "role": "user",
            "parts": [{"text": "hello"}]
        },
        "streaming": true
    });

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/run_sse")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn interrupt(app: &axum::Router, run_id: &str) -> Value {
    let body = json!({
        "appName": "test-app",
        "userId": "user-1",
        "sessionId": "session-1",
        "runId": run_id
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/runs/interrupt")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn interrupt_targets_one_runtime_run_without_cancelling_its_sibling() {
    let app = test_app();
    let run_one = start_run(&app, "run-1").await;
    let run_two = start_run(&app, "run-2").await;
    assert_eq!(run_one.status(), StatusCode::OK);
    assert_eq!(run_two.status(), StatusCode::OK);

    let missing = interrupt(&app, "run-missing").await;
    assert_eq!(missing["interruptedCount"], 0);

    let first = interrupt(&app, "run-1").await;
    assert_eq!(first["interruptedCount"], 1);
    assert_eq!(first["runId"], "run-1");

    let second = interrupt(&app, "run-2").await;
    assert_eq!(second["interruptedCount"], 1);
    assert_eq!(second["runId"], "run-2");

    drop((run_one, run_two));
}
