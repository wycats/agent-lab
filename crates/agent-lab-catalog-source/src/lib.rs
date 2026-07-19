use std::sync::Arc;

use rmcp::{
    ErrorData as McpError, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResult, ListToolsResult, ServerCapabilities, ServerInfo,
        Tool,
    },
    service::RequestContext,
};
use serde_json::{Map, Value as JsonValue, json};

pub type SourceObserver = Arc<dyn Fn(&str, JsonValue) + Send + Sync>;

#[derive(Clone)]
pub struct CatalogSource {
    observe: SourceObserver,
}

impl CatalogSource {
    #[must_use]
    pub fn new(observe: SourceObserver) -> Self {
        Self { observe }
    }

    fn record(&self, kind: &str, payload: JsonValue) {
        (self.observe)(kind, payload);
    }
}

impl ServerHandler for CatalogSource {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }

    async fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: RequestContext<rmcp::RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        self.record("mcp.tools.listed", json!({ "count": 1 }));
        Ok(ListToolsResult::with_all_items(vec![tool(
            "list",
            "Return the controlled product catalog",
            &json!({ "type": "object", "properties": {}, "additionalProperties": false }),
        )]))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<rmcp::RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let arguments = request.arguments.unwrap_or_default();
        let name = request.name.into_owned();
        self.record(
            "mcp.tool.started",
            json!({ "name": name, "arguments": arguments }),
        );
        let result = match name.as_str() {
            "list" => Ok(CallToolResult::structured(json!({
                "items": [
                    { "name": "alpha", "active": true, "score": 3 },
                    { "name": "beta", "active": false, "score": 5 },
                    { "name": "gamma", "active": true, "score": 8 }
                ]
            }))),
            name => Err(McpError::invalid_params(
                format!("unknown catalog tool: {name}"),
                None,
            )),
        };
        self.record(
            "mcp.tool.completed",
            json!({ "name": name, "isError": result.is_err() }),
        );
        result
    }
}

#[derive(Clone)]
pub struct AnalysisSource {
    observe: SourceObserver,
}

impl AnalysisSource {
    #[must_use]
    pub fn new(observe: SourceObserver) -> Self {
        Self { observe }
    }

    fn record(&self, kind: &str, payload: JsonValue) {
        (self.observe)(kind, payload);
    }
}

impl ServerHandler for AnalysisSource {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }

    async fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: RequestContext<rmcp::RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        self.record("mcp.tools.listed", json!({ "count": 1 }));
        Ok(ListToolsResult::with_all_items(vec![tool(
            "summarize",
            "Summarize active catalog items from a catalog result",
            &json!({
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "description": "The items returned by the catalog source",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": { "type": "string" },
                                "active": { "type": "boolean" },
                                "score": { "type": "integer" }
                            },
                            "required": ["name", "active", "score"],
                            "additionalProperties": false
                        }
                    }
                },
                "required": ["items"],
                "additionalProperties": false
            }),
        )]))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<rmcp::RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let arguments = request.arguments.unwrap_or_default();
        let name = request.name.into_owned();
        self.record(
            "mcp.tool.started",
            json!({ "name": name, "arguments": arguments }),
        );
        let result = match name.as_str() {
            "summarize" => arguments
                .get("items")
                .and_then(JsonValue::as_array)
                .map_or_else(
                    || Err(McpError::invalid_params("items must be an array", None)),
                    |items| {
                        let active = items
                            .iter()
                            .filter(|item| {
                                item.get("active").and_then(JsonValue::as_bool) == Some(true)
                            })
                            .cloned()
                            .collect::<Vec<_>>();
                        let total_score = active
                            .iter()
                            .filter_map(|item| item.get("score").and_then(JsonValue::as_i64))
                            .sum::<i64>();
                        Ok(CallToolResult::structured(json!({
                            "active": active,
                            "activeCount": active.len(),
                            "totalScore": total_score
                        })))
                    },
                ),
            name => Err(McpError::invalid_params(
                format!("unknown analysis tool: {name}"),
                None,
            )),
        };
        self.record(
            "mcp.tool.completed",
            json!({ "name": name, "isError": result.is_err() }),
        );
        result
    }
}

fn tool(name: &'static str, description: &'static str, schema: &JsonValue) -> Tool {
    Tool::new(
        name,
        description,
        schema.as_object().cloned().unwrap_or_else(Map::new),
    )
}
