use std::collections::{HashMap, HashSet};

use nu_engine::CallExt;
use nu_protocol::{
    IntoPipelineData, PipelineData, ShellError, Signature, Span, SyntaxShape, Type, Value,
    debugger::WithoutDebug,
    engine::{Command, EngineState, Stack, StateWorkingSet},
    shell_error::generic::GenericError,
};
use rmcp::model::{CallToolResult, Tool};
use thiserror::Error;

use crate::{
    McpBridge,
    bridge::BridgeError,
    value::{json_to_nu, nu_record_to_json},
};

#[derive(Debug, Error)]
pub enum HostError {
    #[error("Nushell parse failed: {0}")]
    Parse(String),
    #[error("Nushell compile failed: {0}")]
    Compile(String),
    #[error(transparent)]
    Shell(#[from] ShellError),
    #[error(transparent)]
    Bridge(#[from] BridgeError),
    #[error("unknown MCP namespace: {0}")]
    UnknownNamespace(String),
    #[error("MCP namespace already attached: {0}")]
    NamespaceAlreadyAttached(String),
}

pub struct NushellHost {
    engine_state: EngineState,
    stack: Stack,
    sessions: HashMap<String, McpBridge>,
    registered_tool_lists: HashSet<String>,
    registered_tools: HashMap<String, HashMap<String, Tool>>,
}

impl NushellHost {
    #[must_use]
    pub fn new() -> Self {
        let engine_state =
            nu_command::add_shell_command_context(nu_cmd_lang::create_default_context());
        let mut stack = Stack::new().collect_value();
        let cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("/"));
        stack.add_env_var(
            "PWD".to_owned(),
            Value::string(cwd.to_string_lossy(), Span::unknown()),
        );
        Self {
            engine_state,
            stack,
            sessions: HashMap::new(),
            registered_tool_lists: HashSet::new(),
            registered_tools: HashMap::new(),
        }
    }

    /// Attach one MCP bridge and register its current tools as Nushell commands.
    ///
    /// # Errors
    ///
    /// Returns an error when discovery fails or Nushell cannot merge the new
    /// command declarations.
    pub fn attach(
        &mut self,
        namespace: impl Into<String>,
        bridge: McpBridge,
    ) -> Result<(), HostError> {
        let namespace = namespace.into();
        if self.sessions.contains_key(&namespace) {
            return Err(HostError::NamespaceAlreadyAttached(namespace));
        }
        self.sessions.insert(namespace.clone(), bridge);
        if let Err(error) = self.refresh(&namespace) {
            self.sessions.remove(&namespace);
            return Err(error);
        }
        Ok(())
    }

    /// Refresh one namespace after an MCP tool-list change.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown namespace, failed MCP discovery, or a
    /// Nushell declaration merge failure.
    pub fn refresh(&mut self, namespace: &str) -> Result<(), HostError> {
        let bridge = self
            .sessions
            .get(namespace)
            .cloned()
            .ok_or_else(|| HostError::UnknownNamespace(namespace.to_owned()))?;
        let tools = bridge.list_tools()?;
        let current_tools = tools
            .into_iter()
            .map(|tool| (tool.name.to_string(), tool))
            .collect::<HashMap<_, _>>();
        let previous_tools = self
            .registered_tools
            .get(namespace)
            .cloned()
            .unwrap_or_default();
        let mut working_set = StateWorkingSet::new(&self.engine_state);

        let list_name = format!("mcp {namespace} tools");
        let add_tool_list = !self.registered_tool_lists.contains(namespace);
        if add_tool_list {
            working_set.add_decl(Box::new(ListToolsCommand {
                name: list_name,
                namespace: namespace.to_owned(),
                bridge: bridge.clone(),
            }));
        }

        for (tool_name, previous) in &previous_tools {
            if current_tools.get(tool_name) != Some(previous) {
                let name = format!("tool {namespace} {tool_name}");
                working_set.hide_decl(name.as_bytes());
            }
        }

        for (tool_name, tool) in &current_tools {
            if previous_tools.get(tool_name) != Some(tool) {
                let name = format!("tool {namespace} {tool_name}");
                working_set.add_decl(Box::new(McpToolCommand {
                    name,
                    tool: tool.clone(),
                    bridge: bridge.clone(),
                }));
            }
        }

        self.engine_state.merge_delta(working_set.render())?;
        if add_tool_list {
            self.registered_tool_lists.insert(namespace.to_owned());
        }
        self.registered_tools
            .insert(namespace.to_owned(), current_tools);
        Ok(())
    }

    /// Evaluate source in the persistent Nushell engine and stack.
    ///
    /// # Errors
    ///
    /// Returns parse, compile, or runtime failures without recreating the
    /// embedded shell session.
    pub fn eval(&mut self, source: &str) -> Result<Value, HostError> {
        let mut working_set = StateWorkingSet::new(&self.engine_state);
        let block = nu_parser::parse(&mut working_set, None, source.as_bytes(), false);
        if let Some(error) = working_set.parse_errors.first() {
            return Err(HostError::Parse(format!("{error:?}")));
        }
        if let Some(error) = working_set.compile_errors.first() {
            return Err(HostError::Compile(format!("{error:?}")));
        }
        self.engine_state.merge_delta(working_set.render())?;
        nu_engine::eval_block::<WithoutDebug>(
            &self.engine_state,
            &mut self.stack,
            &block,
            PipelineData::empty(),
        )?
        .body
        .into_value(Span::unknown())
        .map_err(HostError::from)
    }
}

impl Default for NushellHost {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
struct McpToolCommand {
    name: String,
    tool: Tool,
    bridge: McpBridge,
}

impl Command for McpToolCommand {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> Signature {
        Signature::build(&self.name)
            .input_output_type(Type::Nothing, Type::Any)
            .optional(
                "arguments",
                SyntaxShape::Any,
                "record of arguments matching the MCP tool input schema",
            )
    }

    fn description(&self) -> &str {
        self.tool.description.as_deref().unwrap_or("MCP tool")
    }

    fn run(
        &self,
        engine_state: &EngineState,
        stack: &mut Stack,
        call: &nu_protocol::engine::Call<'_>,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let arguments = call.opt::<Value>(engine_state, stack, 0)?;
        let arguments = nu_record_to_json(arguments, call.head)?;
        let result = self
            .bridge
            .call_tool(self.tool.name.to_string(), arguments)
            .map_err(|error| shell_error("MCP request failed", error.to_string(), call.head))?;
        tool_result_to_pipeline(result, call.head)
    }
}

#[derive(Clone)]
struct ListToolsCommand {
    name: String,
    namespace: String,
    bridge: McpBridge,
}

impl Command for ListToolsCommand {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> Signature {
        Signature::build(&self.name).input_output_type(Type::Nothing, Type::Table(vec![].into()))
    }

    fn description(&self) -> &'static str {
        "List tools discovered from this MCP session"
    }

    fn run(
        &self,
        _engine_state: &EngineState,
        _stack: &mut Stack,
        call: &nu_protocol::engine::Call<'_>,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let tools = self
            .bridge
            .list_tools()
            .map_err(|error| shell_error("MCP discovery failed", error.to_string(), call.head))?;
        let values = tools
            .into_iter()
            .map(|tool| {
                json_to_nu(
                    serde_json::json!({
                        "namespace": self.namespace,
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": tool.input_schema,
                    }),
                    call.head,
                )
            })
            .collect::<Vec<_>>();
        Ok(Value::list(values, call.head).into_pipeline_data())
    }
}

fn tool_result_to_pipeline(result: CallToolResult, span: Span) -> Result<PipelineData, ShellError> {
    if result.is_error.unwrap_or(false) {
        let detail = result.structured_content.map_or_else(
            || format!("{:?}", result.content),
            |value| value.to_string(),
        );
        return Err(shell_error("MCP tool failed", detail, span));
    }
    let value = result.structured_content.map_or_else(
        || {
            json_to_nu(
                serde_json::to_value(result.content).expect("MCP content blocks serialize"),
                span,
            )
        },
        |value| json_to_nu(value, span),
    );
    Ok(value.into_pipeline_data())
}

fn shell_error(title: &'static str, detail: String, span: Span) -> ShellError {
    ShellError::Generic(GenericError::new(title, detail, span))
}
