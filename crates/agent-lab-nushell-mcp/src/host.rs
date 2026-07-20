use std::{
    collections::{HashMap, HashSet},
    io::{self, BufRead, IsTerminal, Write},
    sync::{Arc, OnceLock, atomic::AtomicBool, mpsc},
    time::Duration,
};

use nu_ansi_term::{Color, Style};
use nu_engine::CallExt;
use nu_parser::FlatShape;
use nu_protocol::{
    IntoPipelineData, ListStream, PipelineData, ShellError, Signals, Signature, Span, SyntaxShape,
    Type, Value,
    ast::{Block, Expr, PipelineRedirection, RedirectionTarget, Traverse},
    debugger::WithoutDebug,
    engine::{Command, EngineState, Stack, StateWorkingSet},
    report_error::report_compile_error,
    report_parse_error, report_shell_error,
    shell_error::generic::GenericError,
};
use reedline::{
    ColumnarMenu, Completer, DefaultPrompt, DefaultPromptSegment, Emacs, Highlighter, KeyCode,
    KeyModifiers, MenuBuilder, Reedline, ReedlineEvent, ReedlineMenu, Signal, Span as ReedlineSpan,
    StyledText, Suggestion, default_emacs_keybindings,
};
use rmcp::model::{CallToolResult, Tool};
use serde_json::Value as JsonValue;
use thiserror::Error;

use crate::{
    McpBridge,
    bridge::BridgeError,
    value::{json_to_nu, json_to_nu_tool_result, nu_record_to_json, nu_to_json},
    workbench::{ComparisonStream, WorkbenchBridge},
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
    #[error("interactive shell requires terminal stdin and stdout")]
    InteractiveTerminalRequired,
    #[error("interactive shell I/O failed: {0}")]
    InteractiveIo(#[from] io::Error),
    #[error("interactive line editor failed: {0}")]
    LineEditor(#[from] reedline::ReedlineError),
    #[error("filesystem redirection is not available in the Agent Lab Explore shell")]
    FilesystemRedirection,
}

const EXPLORE_COMMANDS: &[&str] = &[
    "alias",
    "all",
    "ansi",
    "ansi link",
    "ansi strip",
    "any",
    "append",
    "break",
    "bytes",
    "bytes add",
    "bytes at",
    "bytes build",
    "bytes collect",
    "bytes ends-with",
    "bytes index-of",
    "bytes length",
    "bytes remove",
    "bytes replace",
    "bytes reverse",
    "bytes split",
    "bytes starts-with",
    "char",
    "chunks",
    "columns",
    "combinations",
    "compact",
    "const",
    "continue",
    "date",
    "date from-human",
    "date humanize",
    "date list-timezone",
    "date now",
    "date to-timezone",
    "decode",
    "decode base32",
    "decode base32hex",
    "decode base64",
    "decode hex",
    "default",
    "def",
    "describe",
    "detect columns",
    "detect type",
    "difference",
    "do",
    "drop",
    "drop column",
    "drop nth",
    "each",
    "echo",
    "encode",
    "encode base32",
    "encode base32hex",
    "encode base64",
    "encode hex",
    "enumerate",
    "error",
    "error make",
    "every",
    "filter",
    "find",
    "first",
    "flatten",
    "for",
    "format",
    "format date",
    "format duration",
    "format filesize",
    "from",
    "from csv",
    "from json",
    "from nuon",
    "from ssv",
    "from toml",
    "from tsv",
    "from xml",
    "from yaml",
    "generate",
    "get",
    "griddle",
    "group-by",
    "hash",
    "hash md5",
    "hash sha256",
    "headers",
    "help",
    "help aliases",
    "help commands",
    "help escapes",
    "help operators",
    "help pipe-and-redirect",
    "histogram",
    "if",
    "ignore",
    "insert",
    "inspect",
    "interleave",
    "intersect",
    "into",
    "into binary",
    "into bool",
    "into cell-path",
    "into datetime",
    "into duration",
    "into filesize",
    "into float",
    "into int",
    "into record",
    "into semver",
    "into string",
    "items",
    "join",
    "last",
    "length",
    "let",
    "lines",
    "loop",
    "match",
    "math",
    "math abs",
    "math avg",
    "math ceil",
    "math floor",
    "math max",
    "math median",
    "math min",
    "math mode",
    "math product",
    "math round",
    "math sqrt",
    "math stddev",
    "math sum",
    "math variance",
    "merge",
    "merge deep",
    "metadata",
    "move",
    "mut",
    "par-each",
    "parse",
    "peek",
    "permutations",
    "prepend",
    "reduce",
    "reject",
    "rename",
    "return",
    "reverse",
    "select",
    "semver",
    "semver bump",
    "seq",
    "seq char",
    "skip",
    "skip until",
    "skip while",
    "slice",
    "sort",
    "sort-by",
    "split",
    "split chars",
    "split column",
    "split list",
    "split row",
    "split words",
    "str",
    "str capitalize",
    "str contains",
    "str distance",
    "str downcase",
    "str ends-with",
    "str index-of",
    "str join",
    "str length",
    "str replace",
    "str reverse",
    "str starts-with",
    "str stats",
    "str substring",
    "str trim",
    "str upcase",
    "table",
    "take",
    "take until",
    "take while",
    "tee",
    "to",
    "to csv",
    "to json",
    "to md",
    "to nuon",
    "to text",
    "to toml",
    "to tsv",
    "to xml",
    "to yaml",
    "transpose",
    "try",
    "uniq",
    "uniq-by",
    "union",
    "update",
    "upsert",
    "url",
    "url build-query",
    "url decode",
    "url encode",
    "url join",
    "url parse",
    "url split-query",
    "values",
    "version",
    "where",
    "while",
    "window",
    "wrap",
    "zip",
];

pub struct NushellHost {
    engine_state: EngineState,
    stack: Stack,
    sessions: HashMap<String, McpBridge>,
    registered_tool_lists: HashSet<String>,
    registered_tools: HashMap<String, HashMap<String, Tool>>,
    workbench: Option<WorkbenchBridge>,
    workbench_harnesses: Vec<String>,
    workbench_models: Vec<String>,
}

impl NushellHost {
    /// Create an Agent Lab Nushell host with workspace-safe command handling.
    ///
    /// # Panics
    ///
    /// Panics if Nushell cannot merge Agent Lab's built-in unresolved-command
    /// guard into a freshly created default engine state.
    #[must_use]
    pub fn new() -> Self {
        let mut engine_state =
            nu_command::add_shell_command_context(nu_cmd_lang::create_default_context());
        engine_state.set_signals(shell_signals());
        let mut working_set = StateWorkingSet::new(&engine_state);
        let allowed = EXPLORE_COMMANDS
            .iter()
            .map(|command| command.as_bytes())
            .collect::<HashSet<_>>();
        for (command, _) in engine_state.get_decls_sorted(false) {
            if !allowed.contains(command.as_slice()) {
                working_set.hide_decl(&command);
            }
        }
        working_set.add_decl(Box::new(AgentLabExternalGuard));
        engine_state
            .merge_delta(working_set.render())
            .expect("Agent Lab command guard should merge into the default Nushell context");
        let mut stack = Stack::new().collect_value();
        let cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("/"));
        stack.add_env_var(
            "PWD".to_owned(),
            Value::string(cwd.to_string_lossy(), Span::unknown()),
        );
        if let Some(term) = std::env::var_os("TERM") {
            stack.add_env_var(
                "TERM".to_owned(),
                Value::string(term.to_string_lossy(), Span::unknown()),
            );
        }
        Self {
            engine_state,
            stack,
            sessions: HashMap::new(),
            registered_tool_lists: HashSet::new(),
            registered_tools: HashMap::new(),
            workbench: None,
            workbench_harnesses: Vec::new(),
            workbench_models: Vec::new(),
        }
    }

    /// Attach the controller-owned workbench projection.
    ///
    /// # Errors
    ///
    /// Returns an error when the initial workbench snapshot cannot be loaded or
    /// its Nushell commands cannot be registered.
    pub fn attach_workbench(&mut self, bridge: WorkbenchBridge) -> Result<(), HostError> {
        let snapshot = bridge.assembly().map_err(|error| {
            HostError::Shell(shell_error(
                "Workbench attachment failed",
                error.to_string(),
                Span::unknown(),
            ))
        })?;
        self.workbench_harnesses = workbench_ids(&snapshot, "harnesses");
        self.workbench_models = workbench_ids(&snapshot, "modelProfiles");
        let mut working_set = StateWorkingSet::new(&self.engine_state);
        working_set.add_decl(Box::new(LabAssemblyCommand {
            bridge: bridge.clone(),
        }));
        working_set.add_decl(Box::new(LabCompareCommand {
            bridge: bridge.clone(),
        }));
        working_set.add_decl(Box::new(LabEvaluationCommand {
            bridge: bridge.clone(),
        }));
        self.engine_state.merge_delta(working_set.render())?;
        self.workbench = Some(bridge);
        Ok(())
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
        let discovery_generation = bridge.discovery_generation();
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
            working_set.add_decl(Box::new(ListToolsCommand {
                name: format!("mcp {namespace}"),
                namespace: namespace.to_owned(),
                bridge: bridge.clone(),
            }));
        }

        for (tool_name, previous) in &previous_tools {
            if current_tools.get(tool_name) != Some(previous) {
                let name = format!("{namespace} {tool_name}");
                working_set.hide_decl(name.as_bytes());
            }
        }

        for (tool_name, tool) in &current_tools {
            if previous_tools.get(tool_name) != Some(tool) {
                let name = format!("{namespace} {tool_name}");
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
        bridge.mark_discovery_fresh(discovery_generation);
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
        if block_has_file_redirection(&working_set, &block) {
            return Err(HostError::FilesystemRedirection);
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

    /// Refresh every attached namespace whose source reported a catalog change.
    ///
    /// The returned names identify declarations that changed between submitted
    /// shell lines. Keeping this boundary outside Nushell evaluation lets an
    /// interactive surface make refresh visible without teaching MCP commands
    /// to mutate the engine that is currently executing them.
    ///
    /// # Errors
    ///
    /// Returns an error when refreshed discovery or declaration merging fails.
    pub fn refresh_stale(&mut self) -> Result<Vec<String>, HostError> {
        let mut stale = self
            .sessions
            .iter()
            .filter(|(_, bridge)| bridge.discovery_is_stale())
            .map(|(namespace, _)| namespace.clone())
            .collect::<Vec<_>>();
        stale.sort();
        for namespace in &stale {
            self.refresh(namespace)?;
        }
        Ok(stale)
    }

    /// Run the evidence-oriented interactive shell on a real terminal.
    ///
    /// Nushell owns parsing, highlighting, completion, evaluation, help,
    /// structured values, tables, and error rendering. This host owns the
    /// between-line capability refresh boundary.
    ///
    /// # Errors
    ///
    /// Returns an error for non-terminal input/output, terminal I/O failure, or
    /// capability refresh failure.
    pub fn run_interactive(mut self) -> Result<(), HostError> {
        let mut stdout = io::stdout();
        if !io::stdin().is_terminal() || !stdout.is_terminal() {
            return Err(HostError::InteractiveTerminalRequired);
        }

        let mut namespaces = self.sessions.keys().cloned().collect::<Vec<_>>();
        namespaces.sort();
        writeln!(stdout, "Agent Lab")?;
        writeln!(
            stdout,
            "Explore the active workspace and learn how its agent harnesses behave."
        )?;
        if namespaces.is_empty() {
            writeln!(stdout, "MCP namespaces: none")?;
        } else {
            writeln!(stdout, "MCP namespaces: {}", namespaces.join(", "))?;
        }
        if self.workbench.is_some() {
            writeln!(stdout, "Try:")?;
            writeln!(stdout, "  catalog list | where active")?;
            writeln!(stdout, "  catalog list | where active | analysis summarize")?;
            writeln!(stdout, "  lab assembly")?;
            writeln!(stdout, "  lab compare")?;
        }
        writeln!(
            stdout,
            "Use `help <command>` to inspect any command; `exit` leaves."
        )?;

        // Raw PTY test harnesses do not emulate terminal cursor-position
        // responses. Keep a deliberately explicit headless path for them;
        // browser and real-terminal sessions always use Reedline below.
        if std::env::var_os("AGENT_LAB_PLAIN_REPL").is_some() {
            return self.run_headless_interactive(&mut stdout);
        }

        let prompt = DefaultPrompt::new(
            DefaultPromptSegment::Basic("agent-lab".to_owned()),
            DefaultPromptSegment::Empty,
        );
        let mut line_editor = self.line_editor();
        loop {
            self.engine_state.reset_signals();
            let refreshed = self.refresh_stale()?;
            for namespace in &refreshed {
                writeln!(stdout, "[capabilities refreshed: {namespace}]")?;
            }
            if !refreshed.is_empty() {
                line_editor = self.line_editor();
            }
            let source = match line_editor.read_line(&prompt)? {
                Signal::Success(source) => source,
                Signal::CtrlD => break,
                _ => continue,
            };
            if matches!(source.trim(), "exit" | "quit") {
                break;
            }

            // A capability notification may arrive while the prompt blocks on input.
            let refreshed = self.refresh_stale()?;
            for namespace in &refreshed {
                writeln!(stdout, "[capabilities refreshed: {namespace}]")?;
            }
            if !refreshed.is_empty() {
                line_editor = self.line_editor();
            }
            self.eval_and_print(&source);
        }
        Ok(())
    }

    fn run_headless_interactive(&mut self, stdout: &mut impl Write) -> Result<(), HostError> {
        let stdin = io::stdin();
        let mut lines = stdin.lock().lines();
        loop {
            self.engine_state.reset_signals();
            for namespace in self.refresh_stale()? {
                writeln!(stdout, "[capabilities refreshed: {namespace}]")?;
            }
            write!(stdout, "agent-lab> ")?;
            stdout.flush()?;
            let Some(source) = lines.next().transpose()? else {
                break;
            };
            if matches!(source.trim(), "exit" | "quit") {
                break;
            }
            for namespace in self.refresh_stale()? {
                writeln!(stdout, "[capabilities refreshed: {namespace}]")?;
            }
            self.eval_and_print(&source);
        }
        Ok(())
    }

    fn line_editor(&self) -> Reedline {
        let engine_state = Arc::new(self.engine_state.clone());
        let completion_menu = Box::new(ColumnarMenu::default().with_name("completion_menu"));
        let mut keybindings = default_emacs_keybindings();
        keybindings.add_binding(
            KeyModifiers::NONE,
            KeyCode::Tab,
            ReedlineEvent::UntilFound(vec![
                ReedlineEvent::Menu("completion_menu".to_owned()),
                ReedlineEvent::MenuNext,
            ]),
        );
        Reedline::create()
            .with_highlighter(Box::new(AgentLabHighlighter::new(engine_state.clone())))
            .with_completer(Box::new(AgentLabCompleter::new(
                &engine_state,
                self.workbench_harnesses.clone(),
                self.workbench_models.clone(),
            )))
            .with_menu(ReedlineMenu::EngineCompleter(completion_menu))
            .with_edit_mode(Box::new(Emacs::new(keybindings)))
            .with_quick_completions(true)
    }

    fn eval_and_print(&mut self, source: &str) {
        let mut working_set = StateWorkingSet::new(&self.engine_state);
        let block = nu_parser::parse(
            &mut working_set,
            Some("agent-lab-repl"),
            source.as_bytes(),
            false,
        );
        if let Some(error) = working_set.parse_errors.first() {
            report_parse_error(Some(&self.stack), &working_set, error);
            return;
        }
        if let Some(error) = working_set.compile_errors.first() {
            report_compile_error(Some(&self.stack), &working_set, error);
            return;
        }
        if block_has_file_redirection(&working_set, &block) {
            eprintln!("{}", HostError::FilesystemRedirection);
            return;
        }
        let delta = working_set.render();
        if let Err(error) = self.engine_state.merge_delta(delta) {
            report_shell_error(Some(&self.stack), &self.engine_state, &error);
            return;
        }

        let pipeline = nu_engine::eval_block::<WithoutDebug>(
            &self.engine_state,
            &mut self.stack,
            &block,
            PipelineData::empty(),
        );
        match pipeline {
            Ok(output) => {
                if let Err(error) =
                    output
                        .body
                        .print_table(&self.engine_state, &mut self.stack, false, false)
                {
                    report_shell_error(Some(&self.stack), &self.engine_state, &error);
                }
            }
            Err(error) => report_shell_error(Some(&self.stack), &self.engine_state, &error),
        }
    }
}

fn block_has_file_redirection(working_set: &StateWorkingSet<'_>, block: &Block) -> bool {
    fn directly_redirects_to_file(block: &Block) -> bool {
        block.pipelines.iter().any(|pipeline| {
            pipeline.elements.iter().any(|element| {
                element
                    .redirection
                    .as_ref()
                    .is_some_and(redirection_targets_file)
            })
        })
    }

    if directly_redirects_to_file(block) {
        return true;
    }

    let mut nested_blocks = Vec::new();
    block.flat_map(
        working_set,
        &|expression| match expression.expr {
            Expr::Block(id)
            | Expr::Closure(id)
            | Expr::RowCondition(id)
            | Expr::Subexpression(id) => vec![id],
            _ => Vec::new(),
        },
        &mut nested_blocks,
    );
    nested_blocks
        .into_iter()
        .any(|id| directly_redirects_to_file(working_set.get_block(id)))
}

fn redirection_targets_file(redirection: &PipelineRedirection) -> bool {
    let is_file = |target: &RedirectionTarget| matches!(target, RedirectionTarget::File { .. });
    match redirection {
        PipelineRedirection::Single { target, .. } => is_file(target),
        PipelineRedirection::Separate { out, err } => is_file(out) || is_file(err),
    }
}

/// Nushell syntax colors without enabling Nushell's operating-system command
/// execution path. Agent Lab deliberately treats unresolved commands as REPL
/// errors instead of falling through to an ambient shell.
struct AgentLabHighlighter {
    engine_state: Arc<EngineState>,
}

impl AgentLabHighlighter {
    fn new(engine_state: Arc<EngineState>) -> Self {
        Self { engine_state }
    }
}

impl Highlighter for AgentLabHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> StyledText {
        let mut working_set = StateWorkingSet::new(&self.engine_state);
        let block = nu_parser::parse(&mut working_set, None, line.as_bytes(), false);
        let offset = self.engine_state.next_span_start();
        let mut end = offset;
        let mut styled = StyledText::new();

        for (span, shape) in nu_parser::flatten_block(&working_set, &block) {
            if span.start < offset || span.end <= end || span.end - offset > line.len() {
                continue;
            }
            if span.start > end {
                styled.push((
                    Style::new(),
                    line[(end - offset)..(span.start - offset)].to_owned(),
                ));
            }
            styled.push((
                style_for_shape(&shape),
                line[(span.start - offset)..(span.end - offset)].to_owned(),
            ));
            end = span.end;
        }

        if end - offset < line.len() {
            styled.push((Style::new(), line[(end - offset)..].to_owned()));
        }
        styled
    }
}

fn style_for_shape(shape: &FlatShape) -> Style {
    match shape {
        FlatShape::InternalCall(_) => Style::new().fg(Color::Rgb(137, 180, 250)).bold(),
        FlatShape::String | FlatShape::RawString | FlatShape::StringInterpolation => {
            Style::new().fg(Color::Rgb(166, 227, 161))
        }
        FlatShape::Int | FlatShape::Float | FlatShape::Range => {
            Style::new().fg(Color::Rgb(203, 166, 247))
        }
        FlatShape::Variable(_) | FlatShape::VarDecl(_) => {
            Style::new().fg(Color::Rgb(249, 226, 175))
        }
        FlatShape::Operator | FlatShape::Pipe | FlatShape::Redirection => {
            Style::new().fg(Color::Rgb(137, 220, 235)).bold()
        }
        FlatShape::Flag => Style::new().fg(Color::Rgb(148, 226, 213)),
        FlatShape::Keyword | FlatShape::Bool | FlatShape::Nothing => {
            Style::new().fg(Color::Rgb(245, 194, 231))
        }
        FlatShape::Garbage | FlatShape::External(_) | FlatShape::ExternalResolved => {
            Style::new().fg(Color::Rgb(243, 139, 168)).bold()
        }
        _ => Style::new().fg(Color::Rgb(205, 214, 244)),
    }
}

struct AgentLabCompleter {
    commands: Vec<String>,
    workbench_harnesses: Vec<String>,
    workbench_models: Vec<String>,
}

fn workbench_ids(snapshot: &serde_json::Value, key: &str) -> Vec<String> {
    snapshot[key]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|value| value["id"].as_str().map(str::to_owned))
        .collect()
}

fn shell_signals() -> Signals {
    static INTERRUPTED: OnceLock<Arc<AtomicBool>> = OnceLock::new();
    let interrupted = INTERRUPTED
        .get_or_init(|| {
            let interrupted = Arc::new(AtomicBool::new(false));
            let handler = interrupted.clone();
            let _ = signal_hook::flag::register(signal_hook::consts::SIGINT, handler);
            interrupted
        })
        .clone();
    Signals::new(interrupted)
}

#[derive(Clone)]
struct LabAssemblyCommand {
    bridge: WorkbenchBridge,
}

impl Command for LabAssemblyCommand {
    fn name(&self) -> &'static str {
        "lab assembly"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Nothing, Type::Record(Vec::new().into()))
    }

    fn description(&self) -> &'static str {
        "Inspect the active Agent Lab assembly and shared workbench selection"
    }

    fn run(
        &self,
        _engine_state: &EngineState,
        _stack: &mut Stack,
        call: &nu_protocol::engine::Call<'_>,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let value = self.bridge.assembly().map_err(|error| {
            shell_error("Workbench request failed", error.to_string(), call.head)
        })?;
        Ok(json_to_nu(value, call.head).into_pipeline_data())
    }
}

#[derive(Clone)]
struct LabEvaluationCommand {
    bridge: WorkbenchBridge,
}

impl Command for LabEvaluationCommand {
    fn name(&self) -> &'static str {
        "lab evaluation"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Nothing, Type::Record(Vec::new().into()))
            .optional(
                "evaluation-id",
                SyntaxShape::String,
                "evaluation id; defaults to the latest evaluation for this workbench",
            )
    }

    fn description(&self) -> &'static str {
        "Inspect a durable evaluation associated with this workbench"
    }

    fn run(
        &self,
        engine_state: &EngineState,
        stack: &mut Stack,
        call: &nu_protocol::engine::Call<'_>,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let id = call.opt::<String>(engine_state, stack, 0)?;
        let value = self.bridge.evaluation(id.as_deref()).map_err(|error| {
            shell_error("Workbench request failed", error.to_string(), call.head)
        })?;
        Ok(json_to_nu(value, call.head).into_pipeline_data())
    }
}

#[derive(Clone)]
struct LabCompareCommand {
    bridge: WorkbenchBridge,
}

impl Command for LabCompareCommand {
    fn name(&self) -> &'static str {
        "lab compare"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Nothing, Type::Table(Vec::new().into()))
            .rest(
                "harnesses",
                SyntaxShape::String,
                "exactly two harness ids; defaults to the shared workbench pair",
            )
            .named(
                "model",
                SyntaxShape::String,
                "model profile override for this comparison",
                None,
            )
            .switch("raw", "stream raw source-labelled events", None)
            .switch("detach", "return after creating the evaluation", None)
    }

    fn description(&self) -> &'static str {
        "Compare two real harnesses from the active workspace snapshot"
    }

    fn run(
        &self,
        engine_state: &EngineState,
        stack: &mut Stack,
        call: &nu_protocol::engine::Call<'_>,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let harnesses = call.rest::<String>(engine_state, stack, 0)?;
        if !harnesses.is_empty() && harnesses.len() != 2 {
            return Err(shell_error(
                "Invalid comparison",
                "provide exactly two harness ids or omit both to use the shared selection"
                    .to_owned(),
                call.head,
            ));
        }
        let model = call.get_flag::<String>(engine_state, stack, "model")?;
        let raw = call.has_flag(engine_state, stack, "raw")?;
        let detach = call.has_flag(engine_state, stack, "detach")?;
        let comparison = self
            .bridge
            .compare(&harnesses, model, raw, !detach)
            .map_err(|error| shell_error("Comparison failed", error.to_string(), call.head))?;
        if detach {
            return Ok(json_to_nu(comparison.evaluation, call.head).into_pipeline_data());
        }
        let evaluation_id = comparison
            .evaluation
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let stream = WorkbenchValueStream {
            comparison,
            bridge: self.bridge.clone(),
            evaluation_id,
            signals: engine_state.signals().clone(),
            span: call.head,
            finished: false,
        };
        Ok(ListStream::new(stream, call.head, Signals::empty()).into())
    }
}

struct WorkbenchValueStream {
    comparison: ComparisonStream,
    bridge: WorkbenchBridge,
    evaluation_id: String,
    signals: Signals,
    span: Span,
    finished: bool,
}

impl Iterator for WorkbenchValueStream {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.signals.interrupted() {
                self.bridge.cancel(&self.evaluation_id);
                self.finished = true;
                return None;
            }
            match self
                .comparison
                .receiver
                .recv_timeout(Duration::from_millis(100))
            {
                Ok(Ok(value)) => return Some(json_to_nu(value, self.span)),
                Ok(Err(error)) => {
                    self.finished = true;
                    return Some(Value::error(
                        shell_error("Comparison stream failed", error.to_string(), self.span),
                        self.span,
                    ));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.finished = true;
                    return None;
                }
            }
        }
    }
}

impl Drop for WorkbenchValueStream {
    fn drop(&mut self) {
        if !self.finished && self.signals.interrupted() {
            self.bridge.cancel(&self.evaluation_id);
        }
    }
}

#[derive(Clone)]
struct AgentLabExternalGuard;

impl Command for AgentLabExternalGuard {
    fn name(&self) -> &'static str {
        "run-external"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Any, Type::Any)
            .rest(
                "command",
                SyntaxShape::OneOf(vec![SyntaxShape::GlobPattern, SyntaxShape::Any]),
                "command that was not resolved by the Agent Lab workspace",
            )
    }

    fn description(&self) -> &'static str {
        "Reports commands that are not available in this Agent Lab workspace"
    }

    fn run(
        &self,
        engine_state: &EngineState,
        stack: &mut Stack,
        call: &nu_protocol::engine::Call<'_>,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let command = call
            .rest::<Value>(engine_state, stack, 0)?
            .into_iter()
            .filter_map(|value| value.coerce_str().ok().map(std::borrow::Cow::into_owned))
            .collect::<Vec<_>>()
            .join(" ");
        let command = if command.is_empty() {
            "that command".to_owned()
        } else {
            format!("`{command}`")
        };
        Err(ShellError::Generic(
            GenericError::new(
                "Unknown Agent Lab command",
                format!("{command} is not available in this workspace"),
                call.head,
            )
            .with_code("agent_lab::command::unknown")
            .with_help("Press Tab to complete available commands, or use `help commands`."),
        ))
    }
}

impl AgentLabCompleter {
    fn new(
        engine_state: &EngineState,
        workbench_harnesses: Vec<String>,
        workbench_models: Vec<String>,
    ) -> Self {
        let mut commands = engine_state
            .get_decls_sorted(false)
            .into_iter()
            .filter_map(|(name, _)| String::from_utf8(name).ok())
            .collect::<Vec<_>>();
        let canonical_commands = commands.iter().cloned().collect::<HashSet<_>>();
        commands.retain(|command| {
            let is_mcp_namespace_root = command
                .strip_prefix("mcp ")
                .is_some_and(|suffix| !suffix.contains(' '));
            !is_mcp_namespace_root || !canonical_commands.contains(&format!("{command} tools"))
        });
        Self {
            commands,
            workbench_harnesses,
            workbench_models,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use reedline::Completer;

    use super::{AgentLabCompleter, EXPLORE_COMMANDS, HostError, NushellHost};

    fn completer() -> AgentLabCompleter {
        AgentLabCompleter {
            commands: vec!["lab assembly".to_owned(), "lab compare".to_owned()],
            workbench_harnesses: vec!["v0".to_owned(), "eve".to_owned()],
            workbench_models: vec!["haiku-4.5".to_owned()],
        }
    }

    fn completion_values(line: &str) -> Vec<String> {
        completer()
            .complete(line, line.len())
            .into_iter()
            .map(|suggestion| suggestion.value)
            .collect()
    }

    #[test]
    fn explore_shell_exposes_only_the_positive_command_set() {
        let host = NushellHost::new();
        for (command, _) in host.engine_state.get_decls_sorted(false) {
            let command = String::from_utf8(command).unwrap();
            assert!(
                command == "run-external" || EXPLORE_COMMANDS.contains(&command.as_str()),
                "{command} is not in the Explore command allowlist"
            );
        }
        for command in ["du", "open", "source", "start", "use"] {
            assert!(
                host.engine_state
                    .find_decl(command.as_bytes(), &[])
                    .is_none()
            );
        }
    }

    #[test]
    fn explore_shell_rejects_file_redirection_before_evaluation() {
        let mut host = NushellHost::new();
        let path = std::env::temp_dir().join(format!(
            "agent-lab-explore-redirection-{}",
            std::process::id()
        ));
        let source = format!("'outside' o> '{}'", path.display());
        assert!(matches!(
            host.eval(&source),
            Err(HostError::FilesystemRedirection)
        ));
        assert!(!path.exists());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn explore_shell_rejects_nested_file_redirection() {
        let mut host = NushellHost::new();
        let path = std::env::temp_dir().join(format!(
            "agent-lab-explore-nested-redirection-{}",
            std::process::id()
        ));
        let source = format!("do {{ 'outside' o> '{}' }}", path.display());
        assert!(matches!(
            host.eval(&source),
            Err(HostError::FilesystemRedirection)
        ));
        assert!(!path.exists());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn compare_completion_distinguishes_harnesses_models_and_flags() {
        assert_eq!(completion_values("lab compare e"), ["eve"]);
        assert_eq!(completion_values("lab compare --model h"), ["haiku-4.5"]);
        assert_eq!(completion_values("lab compare --r"), ["--raw"]);
        assert_eq!(completion_values("lab compare v0 "), ["eve"]);
    }
}

impl Completer for AgentLabCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let before_cursor = &line[..pos.min(line.len())];
        let segment_start = before_cursor.rfind('|').map_or(0, |index| index + 1);
        let segment = &before_cursor[segment_start..];
        let leading = segment.len() - segment.trim_start().len();
        let mut replace_start = segment_start + leading;
        let mut prefix = &before_cursor[replace_start..];

        if let Some(help_target) = prefix.strip_prefix("help ") {
            replace_start += "help ".len();
            prefix = help_target;
        }

        if let Some(arguments) = prefix.strip_prefix("lab compare ") {
            let value_start = arguments.rfind(' ').map_or(0, |index| index + 1);
            let value_prefix = &arguments[value_start..];
            let replace_start = replace_start + "lab compare ".len() + value_start;
            let prior = arguments[..value_start]
                .split_whitespace()
                .collect::<Vec<_>>();
            if value_prefix.starts_with('-') {
                return ["--model", "--raw", "--detach"]
                    .into_iter()
                    .filter(|flag| flag.starts_with(value_prefix) && *flag != value_prefix)
                    .map(|flag| Suggestion {
                        value: flag.to_owned(),
                        description: Some("lab compare option".to_owned()),
                        span: ReedlineSpan::new(replace_start, pos),
                        append_whitespace: true,
                        ..Suggestion::default()
                    })
                    .collect();
            }
            let is_model = prior.last() == Some(&"--model");
            let values = if is_model {
                &self.workbench_models
            } else {
                &self.workbench_harnesses
            };
            return values
                .iter()
                .filter(|value| {
                    value.starts_with(value_prefix)
                        && value.as_str() != value_prefix
                        && !prior.contains(&value.as_str())
                })
                .map(|value| Suggestion {
                    value: value.clone(),
                    description: Some(if is_model {
                        "Agent Lab model profile".to_owned()
                    } else {
                        "Agent Lab harness".to_owned()
                    }),
                    span: ReedlineSpan::new(replace_start, pos),
                    append_whitespace: true,
                    ..Suggestion::default()
                })
                .collect();
        }

        self.commands
            .iter()
            .filter(|command| command.starts_with(prefix) && command.as_str() != prefix)
            .map(|command| Suggestion {
                value: command.clone(),
                description: Some("Nushell command".to_owned()),
                span: ReedlineSpan::new(replace_start, pos),
                append_whitespace: true,
                ..Suggestion::default()
            })
            .collect()
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
            .input_output_type(Type::Any, Type::Any)
            .switch(
                "envelope",
                "preserve the exact MCP structured result envelope",
                None,
            )
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
        input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let arguments = match call.opt::<Value>(engine_state, stack, 0)? {
            Some(arguments) => Some(arguments),
            None if matches!(input, PipelineData::Empty) => None,
            None => Some(input.into_value(call.head)?),
        };
        let arguments = tool_arguments_to_json(arguments, &self.tool, call.head)?;
        let result = self
            .bridge
            .call_tool(self.tool.name.to_string(), arguments)
            .map_err(|error| shell_error("MCP request failed", error.to_string(), call.head))?;
        let project_single_collection = !call.has_flag(engine_state, stack, "envelope")?
            && tool_projects_single_collection(&self.tool);
        tool_result_to_pipeline(result, call.head, project_single_collection)
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

fn tool_result_to_pipeline(
    result: CallToolResult,
    span: Span,
    project_single_collection: bool,
) -> Result<PipelineData, ShellError> {
    if result.is_error.unwrap_or(false) {
        let detail = result.structured_content.map_or_else(
            || format!("{:?}", result.content),
            |value| value.to_string(),
        );
        return Err(shell_error("MCP tool failed", detail, span));
    }
    let value = if let Some(value) = result.structured_content {
        json_to_nu_tool_result(value, span, project_single_collection)
    } else {
        let content = serde_json::to_value(result.content).map_err(|error| {
            shell_error("MCP result serialization failed", error.to_string(), span)
        })?;
        json_to_nu(content, span)
    };
    Ok(value.into_pipeline_data())
}

fn tool_projects_single_collection(tool: &Tool) -> bool {
    tool.meta
        .as_ref()
        .and_then(|meta| meta.0.get("io.agent-lab/nushellProjection"))
        .and_then(JsonValue::as_str)
        == Some("soleCollection")
}

fn tool_arguments_to_json(
    arguments: Option<Value>,
    tool: &Tool,
    span: Span,
) -> Result<serde_json::Map<String, JsonValue>, ShellError> {
    if matches!(arguments, Some(Value::List { .. }))
        && let Some(parameter) = single_collection_parameter(tool)
    {
        let mut wrapped = serde_json::Map::new();
        wrapped.insert(
            parameter,
            nu_to_json(arguments.expect("list arguments were matched above"))?,
        );
        return Ok(wrapped);
    }
    nu_record_to_json(arguments, span)
}

fn single_collection_parameter(tool: &Tool) -> Option<String> {
    let properties = tool.input_schema.get("properties")?.as_object()?;
    if properties.len() != 1 {
        return None;
    }
    let (name, schema) = properties.iter().next()?;
    (schema.get("type")?.as_str()? == "array").then(|| name.clone())
}

fn shell_error(title: &'static str, detail: String, span: Span) -> ShellError {
    ShellError::Generic(GenericError::new(title, detail, span))
}
