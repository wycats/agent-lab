use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

#[allow(deprecated)]
use rmcp::model::{LoggingLevel, LoggingMessageNotificationParam};
use rmcp::{
    ErrorData as McpError, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResult, ListToolsResult, ProgressNotificationParam,
        ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext,
};
use serde_json::{Map, json};

#[derive(Clone, Default)]
struct Fixture {
    count: Arc<AtomicU64>,
    extra_enabled: Arc<AtomicBool>,
    extra_revision: Arc<AtomicU64>,
}

impl ServerHandler for Fixture {
    #[allow(deprecated)]
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_tool_list_changed()
                .enable_logging()
                .build(),
        )
    }

    async fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: RequestContext<rmcp::RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let mut tools = vec![
            tool("session", "Return fixture process and session state"),
            tool("increment", "Increment session-local state"),
            tool("catalog", "Return a nested structured catalog"),
            tool("lifecycle", "Emit progress and a structured log"),
            tool("enable_extra", "Add a tool and notify discovery clients"),
            tool(
                "schedule_extra",
                "Add a tool after returning to the discovery client",
            ),
            tool(
                "disable_extra",
                "Remove a tool and notify discovery clients",
            ),
            tool(
                "revise_extra",
                "Change a tool descriptor and notify discovery clients",
            ),
            tool("fail", "Return a caller-visible tool error"),
            tool("protocol_fail", "Return an MCP protocol error"),
        ];
        if self.extra_enabled.load(Ordering::SeqCst) {
            let description = match self.extra_revision.load(Ordering::SeqCst) {
                0 | 1 => "A tool added during the live session",
                _ => "A revised tool in the live session",
            };
            tools.push(tool("extra", description));
        }
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<rmcp::RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let arguments = request.arguments.unwrap_or_default();
        match request.name.as_ref() {
            "session" => Ok(CallToolResult::structured(json!({
                "pid": std::process::id(),
                "count": self.count.load(Ordering::SeqCst),
            }))),
            "increment" => {
                let count = self.count.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(CallToolResult::structured(json!({ "count": count })))
            }
            "catalog" => Ok(CallToolResult::structured(json!({
                "items": [
                    { "name": "alpha", "active": true, "score": 3 },
                    { "name": "beta", "active": false, "score": 5 },
                    { "name": "gamma", "active": true, "score": 8 }
                ],
                "request": arguments,
            }))),
            "lifecycle" => {
                let token = context
                    .meta
                    .get_progress_token()
                    .ok_or_else(|| McpError::invalid_params("progress token required", None))?;
                context
                    .peer
                    .notify_progress(
                        ProgressNotificationParam::new(token.clone(), 1.0)
                            .with_total(2.0)
                            .with_message("fixture started"),
                    )
                    .await
                    .map_err(|error| service_error(&error))?;
                #[allow(deprecated)]
                context
                    .peer
                    .notify_logging_message(
                        LoggingMessageNotificationParam::new(
                            LoggingLevel::Info,
                            json!({ "phase": "fixture", "structured": true }),
                        )
                        .with_logger("agent-lab-fixture"),
                    )
                    .await
                    .map_err(|error| service_error(&error))?;
                context
                    .peer
                    .notify_progress(
                        ProgressNotificationParam::new(token, 2.0)
                            .with_total(2.0)
                            .with_message("fixture complete"),
                    )
                    .await
                    .map_err(|error| service_error(&error))?;
                Ok(CallToolResult::structured(json!({ "complete": true })))
            }
            "enable_extra" => {
                self.extra_enabled.store(true, Ordering::SeqCst);
                self.extra_revision.store(1, Ordering::SeqCst);
                notify_tools_changed(&context).await?;
                Ok(CallToolResult::structured(json!({ "enabled": "extra" })))
            }
            "schedule_extra" => {
                let fixture = self.clone();
                let peer = context.peer.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    fixture.extra_enabled.store(true, Ordering::SeqCst);
                    fixture.extra_revision.store(1, Ordering::SeqCst);
                    let _ = peer.notify_tool_list_changed().await;
                });
                Ok(CallToolResult::structured(json!({ "scheduled": "extra" })))
            }
            "disable_extra" => {
                self.extra_enabled.store(false, Ordering::SeqCst);
                notify_tools_changed(&context).await?;
                Ok(CallToolResult::structured(json!({ "disabled": "extra" })))
            }
            "revise_extra" if self.extra_enabled.load(Ordering::SeqCst) => {
                self.extra_revision.store(2, Ordering::SeqCst);
                notify_tools_changed(&context).await?;
                Ok(CallToolResult::structured(json!({ "revised": "extra" })))
            }
            "extra" if self.extra_enabled.load(Ordering::SeqCst) => {
                Ok(CallToolResult::structured(json!({ "available": true })))
            }
            "fail" => Ok(CallToolResult::structured_error(json!({
                "kind": "fixture",
                "message": "intentional tool failure",
            }))),
            "protocol_fail" => Err(McpError::internal_error(
                "intentional fixture protocol failure",
                None,
            )),
            name => Err(McpError::invalid_params(
                format!("unknown tool: {name}"),
                None,
            )),
        }
    }
}

fn tool(name: &'static str, description: &'static str) -> Tool {
    let schema = json!({ "type": "object", "additionalProperties": true });
    let schema = schema.as_object().cloned().unwrap_or_else(Map::new);
    Tool::new(name, description, schema)
}

fn service_error(error: &rmcp::service::ServiceError) -> McpError {
    McpError::internal_error(error.to_string(), None)
}

async fn notify_tools_changed(context: &RequestContext<rmcp::RoleServer>) -> Result<(), McpError> {
    context
        .peer
        .notify_tool_list_changed()
        .await
        .map_err(|error| service_error(&error))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = Fixture::default().serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;
    Ok(())
}
