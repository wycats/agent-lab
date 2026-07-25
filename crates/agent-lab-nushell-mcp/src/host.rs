use std::{
    collections::{HashMap, HashSet},
    io::{self, BufRead, IsTerminal, Read, Write},
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use agent_lab_driver_protocol::MAX_DRIVER_RECORD_BYTES;
use nu_ansi_term::{Color, Style};
use nu_engine::CallExt;
use nu_parser::FlatShape;
use nu_protocol::{
    ByteStream, ByteStreamType, IntoPipelineData, ListStream, OutDest, PipelineData, ShellError,
    Signals, Signature, Span, SyntaxShape, Type, Value,
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
use serde_json::{Value as JsonValue, json};
use termimad::crossterm::{
    QueueableCommand,
    cursor::MoveToColumn,
    terminal::{Clear, ClearType},
};
use thiserror::Error;

use crate::{
    McpBridge,
    bridge::BridgeError,
    value::{json_to_nu, json_to_nu_tool_result, nu_record_to_json, nu_to_json},
    workbench::{
        AgentTurnOutput, AgentTurnStream, ComparisonStream, WorkbenchBridge, WorkbenchError,
    },
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
    "from md",
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
    workbench_sessions: Arc<RwLock<Vec<String>>>,
    agent_status: AgentStatusPresenter,
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
            workbench_sessions: Arc::new(RwLock::new(Vec::new())),
            agent_status: AgentStatusPresenter::terminal(),
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
        replace_session_ids(
            &self.workbench_sessions,
            workbench_ids(&snapshot, "agentSessions"),
        );
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
        working_set.add_decl(Box::new(AgentCommand {
            bridge: bridge.clone(),
            session_ids: self.workbench_sessions.clone(),
            status: self.agent_status.clone(),
        }));
        working_set.add_decl(Box::new(AgentNewCommand {
            bridge: bridge.clone(),
            session_ids: self.workbench_sessions.clone(),
            status: self.agent_status.clone(),
        }));
        working_set.add_decl(Box::new(AgentSessionsCommand {
            bridge: bridge.clone(),
            session_ids: self.workbench_sessions.clone(),
        }));
        working_set.add_decl(Box::new(AgentSwitchCommand {
            bridge: bridge.clone(),
            session_ids: self.workbench_sessions.clone(),
        }));
        working_set.add_decl(Box::new(AgentTurnCommand {
            bridge: bridge.clone(),
        }));
        working_set.add_decl(Box::new(AgentCancelCommand {
            bridge: bridge.clone(),
        }));
        working_set.add_decl(Box::new(AgentCloseCommand {
            bridge: bridge.clone(),
            session_ids: self.workbench_sessions.clone(),
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
        let term = std::env::var("TERM").ok();
        self.agent_status
            .set_enabled(interactive_agent_status_enabled(
                io::stderr().is_terminal(),
                term.as_deref(),
            ));

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
            writeln!(stdout, "  agent \"What matters about this workspace?\"")?;
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
                self.workbench_sessions.clone(),
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
                if let Err(error) = print_repl_output(
                    output.body,
                    &self.engine_state,
                    &mut self.stack,
                    &TermimadMarkdownRenderer,
                    &mut io::stdout(),
                ) {
                    report_shell_error(Some(&self.stack), &self.engine_state, &error);
                }
            }
            Err(error) => report_shell_error(Some(&self.stack), &self.engine_state, &error),
        }
    }
}

const AGENT_ANSWER_TYPE: &str = "agent-answer";
const MARKDOWN_WIDTH_FALLBACK: usize = 80;
const MARKDOWN_WIDTH_MINIMUM: usize = 20;
const TERMINAL_ESCAPE: u8 = 0x1b;
const AGENT_STATUS_WIDTH_FALLBACK: u16 = 80;

type TerminalWidthProvider = Arc<dyn Fn() -> Option<u16> + Send + Sync>;

#[derive(Clone)]
struct AgentStatusPresenter {
    inner: Arc<AgentStatusPresenterInner>,
}

struct AgentStatusPresenterInner {
    enabled: AtomicBool,
    writer: Mutex<Box<dyn Write + Send>>,
    terminal_width: TerminalWidthProvider,
}

impl AgentStatusPresenter {
    fn terminal() -> Self {
        Self::with_writer(
            Box::new(io::stderr()),
            Arc::new(|| {
                termimad::crossterm::terminal::size()
                    .ok()
                    .map(|(width, _)| width)
            }),
        )
    }

    fn with_writer(writer: Box<dyn Write + Send>, terminal_width: TerminalWidthProvider) -> Self {
        Self {
            inner: Arc::new(AgentStatusPresenterInner {
                enabled: AtomicBool::new(false),
                writer: Mutex::new(writer),
                terminal_width,
            }),
        }
    }

    fn set_enabled(&self, enabled: bool) {
        self.inner.enabled.store(enabled, Ordering::Relaxed);
    }

    fn begin(&self, stack: &Stack) -> Option<AgentStatusGuard> {
        if !self.inner.enabled.load(Ordering::Relaxed)
            || !matches!(stack.stderr(), OutDest::Inherit | OutDest::Print)
        {
            return None;
        }
        let started = Instant::now();
        let mut guard = AgentStatusGuard {
            presenter: self.clone(),
            started,
            last_elapsed_second: 0,
            phase: "preparing".to_owned(),
            detail: None,
            attempted_display: false,
        };
        guard.redraw_at(started);
        Some(guard)
    }

    fn write_line(&self, line: &str) {
        let Ok(mut writer) = self.inner.writer.lock() else {
            return;
        };
        let writer = writer.as_mut();
        let _ = writer.queue(MoveToColumn(0));
        let _ = writer.queue(Clear(ClearType::CurrentLine));
        let _ = writer.write_all(line.as_bytes());
        let _ = writer.flush();
    }

    fn clear(&self) {
        let Ok(mut writer) = self.inner.writer.lock() else {
            return;
        };
        let writer = writer.as_mut();
        let _ = writer.queue(MoveToColumn(0));
        let _ = writer.queue(Clear(ClearType::CurrentLine));
        let _ = writer.flush();
    }

    fn width(&self) -> u16 {
        (self.inner.terminal_width)()
            .filter(|width| *width > 1)
            .unwrap_or(AGENT_STATUS_WIDTH_FALLBACK)
    }
}

struct AgentStatusGuard {
    presenter: AgentStatusPresenter,
    started: Instant,
    last_elapsed_second: u64,
    phase: String,
    detail: Option<String>,
    attempted_display: bool,
}

impl AgentStatusGuard {
    fn set_phase(&mut self, phase: &str, detail: Option<&str>) {
        let phase = friendly_agent_phase(phase);
        let detail = detail.and_then(sanitize_agent_status_detail);
        if self.phase == phase && self.detail == detail {
            self.tick();
            return;
        }
        self.phase = phase;
        self.detail = detail;
        self.redraw_at(Instant::now());
    }

    fn apply_progress(&mut self, progress: &JsonValue) {
        if progress.is_null() {
            self.tick();
            return;
        }
        let projected = progress.get("progress").unwrap_or(progress);
        let payload = projected.get("payload").unwrap_or(projected);
        let phase = projected
            .get("phase")
            .or_else(|| payload.get("phase"))
            .or_else(|| projected.get("status"))
            .or_else(|| payload.get("status"))
            .and_then(JsonValue::as_str)
            .unwrap_or("working");
        let detail = projected
            .get("detail")
            .or_else(|| payload.get("detail"))
            .or_else(|| projected.get("message"))
            .or_else(|| payload.get("message"))
            .and_then(JsonValue::as_str);
        self.set_phase(phase, detail);
    }

    fn tick(&mut self) {
        self.tick_at(Instant::now());
    }

    fn tick_at(&mut self, now: Instant) {
        let elapsed_second = now.saturating_duration_since(self.started).as_secs();
        if elapsed_second > self.last_elapsed_second {
            self.redraw_at(now);
        }
    }

    fn redraw_at(&mut self, now: Instant) {
        let elapsed_second = now.saturating_duration_since(self.started).as_secs();
        self.last_elapsed_second = elapsed_second;
        let mut line = format!("Agent: {} \u{b7} {elapsed_second}s", self.phase);
        if let Some(detail) = &self.detail {
            line.push_str(" \u{b7} ");
            line.push_str(detail);
        }
        let width = usize::from(self.presenter.width().saturating_sub(1).max(1));
        self.presenter
            .write_line(&truncate_agent_status(&line, width));
        self.attempted_display = true;
    }
}

impl Drop for AgentStatusGuard {
    fn drop(&mut self) {
        if self.attempted_display {
            self.presenter.clear();
        }
    }
}

fn interactive_agent_status_enabled(stderr_is_terminal: bool, term: Option<&str>) -> bool {
    stderr_is_terminal && !term.is_some_and(|term| term.eq_ignore_ascii_case("dumb"))
}

fn friendly_agent_phase(phase: &str) -> String {
    let phase = sanitize_agent_status_detail(phase).unwrap_or_else(|| "working".to_owned());
    match phase.as_str() {
        "starting" => "starting".to_owned(),
        "preparing" => "preparing".to_owned(),
        "reasoning" => "thinking".to_owned(),
        "responding" => "answering".to_owned(),
        "acting" => "using tools".to_owned(),
        "waiting" => "waiting".to_owned(),
        "finalizing" => "finalizing".to_owned(),
        _ => phase.replace('-', " "),
    }
}

fn sanitize_agent_status_detail(detail: &str) -> Option<String> {
    let mut sanitized = String::with_capacity(detail.len().min(160));
    let mut needs_space = false;
    for character in detail.chars() {
        if character.is_whitespace() {
            needs_space = !sanitized.is_empty();
            continue;
        }
        if needs_space {
            sanitized.push(' ');
            needs_space = false;
        }
        if character.is_ascii_graphic() {
            sanitized.push(character);
        } else if !character.is_control() {
            sanitized.push('?');
        }
        if sanitized.len() >= 160 {
            break;
        }
    }
    (!sanitized.is_empty()).then_some(sanitized)
}

fn truncate_agent_status(line: &str, width: usize) -> String {
    if line.chars().count() <= width {
        return line.to_owned();
    }
    if width <= 3 {
        return ".".repeat(width);
    }
    let mut truncated = line.chars().take(width - 3).collect::<String>();
    truncated.push_str("...");
    truncated
}

trait MarkdownRenderer {
    fn render(&self, markdown: &str, width: usize) -> Result<String, String>;
}

struct TermimadMarkdownRenderer;

impl MarkdownRenderer for TermimadMarkdownRenderer {
    fn render(&self, markdown: &str, width: usize) -> Result<String, String> {
        std::panic::catch_unwind(|| {
            let skin = agent_lab_markdown_skin();
            skin.text(markdown, Some(width)).to_string()
        })
        .map_err(|_| "the Markdown renderer stopped unexpectedly".to_owned())
    }
}

fn agent_lab_markdown_skin() -> termimad::MadSkin {
    let foreground = termimad::rgb(216, 224, 219);
    let bright = termimad::rgb(244, 247, 245);
    let green = termimad::rgb(143, 181, 115);
    let neutral = termimad::rgb(170, 182, 176);
    let muted = termimad::rgb(82, 104, 93);
    let code_foreground = termimad::rgb(185, 199, 192);
    let code_background = termimad::rgb(9, 16, 13);
    let inline_code_foreground = termimad::rgb(186, 216, 170);
    let inline_code_background = termimad::rgb(21, 31, 26);

    let mut skin = termimad::MadSkin::default_dark();
    skin.set_fg(foreground);
    skin.bold.set_fg(bright);
    skin.italic.set_fg(neutral);
    skin.strikeout.set_fg(neutral);
    skin.set_headers_fg(green);
    skin.bullet.set_fg(green);
    skin.quote_mark.set_fg(neutral);
    skin.horizontal_rule.set_fg(muted);
    skin.table.set_fg(neutral);
    skin.code_block.set_fgbg(code_foreground, code_background);
    skin.inline_code
        .set_fgbg(inline_code_foreground, inline_code_background);
    skin
}

fn print_repl_output(
    output: PipelineData,
    engine_state: &EngineState,
    stack: &mut Stack,
    renderer: &impl MarkdownRenderer,
    writer: &mut impl Write,
) -> Result<(), ShellError> {
    if let PipelineData::Value(value, _) = &output
        && let Some(response) = agent_answer_response(value)
    {
        let safe_response = neutralize_terminal_controls(response);
        let width = markdown_render_width(termimad::crossterm::terminal::size().ok());
        let formatted = renderer
            .render(&safe_response, width)
            .map(|rendered| filter_terminal_rendering(&rendered))
            .unwrap_or(safe_response);
        writer.write_all(formatted.as_bytes()).map_err(|error| {
            shell_error(
                "Agent answer display failed",
                error.to_string(),
                value.span(),
            )
        })?;
        if formatted.contains("\u{1b}[") {
            writer.write_all(b"\x1b[0m").map_err(|error| {
                shell_error(
                    "Agent answer display failed",
                    error.to_string(),
                    value.span(),
                )
            })?;
        }
        if !formatted.ends_with('\n') {
            writer.write_all(b"\n").map_err(|error| {
                shell_error(
                    "Agent answer display failed",
                    error.to_string(),
                    value.span(),
                )
            })?;
        }
        writer.flush().map_err(|error| {
            shell_error(
                "Agent answer display failed",
                error.to_string(),
                value.span(),
            )
        })?;
        return Ok(());
    }
    match output {
        PipelineData::ByteStream(stream, _) if stream.type_() == ByteStreamType::String => {
            print_terminal_text_stream(stream, writer)
        }
        output => output.print_table(engine_state, stack, false, false),
    }
}

fn print_terminal_text_stream(
    stream: ByteStream,
    writer: &mut impl Write,
) -> Result<(), ShellError> {
    let span = stream.span();
    let Some(mut reader) = stream.reader() else {
        return Ok(());
    };
    // Byte streams can contain model-authored text. Keep their source bytes
    // intact inside pipelines, but admit only inert text at the final PTY
    // display boundary. Styled Markdown takes the separate trusted-renderer
    // path above.
    let mut filter = TerminalOutputFilter::plain_text();
    let mut buffer = [0_u8; 4096];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|error| shell_error("Text stream display failed", error.to_string(), span))?;
        if read == 0 {
            break;
        }
        filter
            .feed(&buffer[..read], writer)
            .map_err(|error| shell_error("Text stream display failed", error.to_string(), span))?;
    }
    filter
        .finish(writer)
        .and_then(|()| writer.flush())
        .map_err(|error| shell_error("Text stream display failed", error.to_string(), span))
}

fn agent_answer_response(value: &Value) -> Option<&str> {
    let record = value.as_record().ok()?;
    if record.get("type")?.as_str().ok()? != AGENT_ANSWER_TYPE {
        return None;
    }
    record.get("response")?.as_str().ok()
}

fn markdown_render_width(size: Option<(u16, u16)>) -> usize {
    size.map_or(MARKDOWN_WIDTH_FALLBACK, |(width, _)| {
        usize::from(width)
            .saturating_sub(1)
            .max(MARKDOWN_WIDTH_MINIMUM)
    })
}

fn neutralize_terminal_controls(markdown: &str) -> String {
    let mut safe = String::with_capacity(markdown.len());
    let mut characters = markdown.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '\r' => {
                if characters.peek() == Some(&'\n') {
                    continue;
                }
                safe.push('\n');
            }
            '\n' | '\t' => safe.push(character),
            character if character.is_control() => safe.push('\u{fffd}'),
            character => safe.push(character),
        }
    }
    safe
}

fn filter_terminal_rendering(rendered: &str) -> String {
    let mut safe = Vec::with_capacity(rendered.len());
    let mut filter = TerminalOutputFilter::renderer_styling();
    filter
        .feed(rendered.as_bytes(), &mut safe)
        .and_then(|()| filter.finish(&mut safe))
        .expect("writing to an in-memory buffer cannot fail");
    String::from_utf8(safe).expect("terminal filtering emits valid UTF-8")
}

struct TerminalOutputFilter {
    state: TerminalFilterState,
    utf8_pending: Vec<u8>,
    allow_sgr: bool,
}

impl TerminalOutputFilter {
    fn plain_text() -> Self {
        Self {
            state: TerminalFilterState::Text,
            utf8_pending: Vec::new(),
            allow_sgr: false,
        }
    }

    fn renderer_styling() -> Self {
        Self {
            allow_sgr: true,
            ..Self::plain_text()
        }
    }
}

#[derive(Default)]
enum TerminalFilterState {
    #[default]
    Text,
    Escape,
    Csi(Vec<u8>),
    DiscardCsi,
    Osc {
        escape: bool,
    },
    StringControl {
        escape: bool,
    },
}

impl TerminalOutputFilter {
    fn feed(&mut self, bytes: &[u8], writer: &mut impl Write) -> io::Result<()> {
        for byte in bytes {
            self.feed_byte(*byte, writer)?;
        }
        Ok(())
    }

    fn finish(&mut self, writer: &mut impl Write) -> io::Result<()> {
        self.finish_utf8(writer)?;
        self.state = TerminalFilterState::Text;
        Ok(())
    }

    fn feed_byte(&mut self, byte: u8, writer: &mut impl Write) -> io::Result<()> {
        let state = std::mem::take(&mut self.state);
        match state {
            TerminalFilterState::Text if byte == TERMINAL_ESCAPE => {
                self.finish_utf8(writer)?;
                self.state = TerminalFilterState::Escape;
            }
            TerminalFilterState::Text => {
                self.write_text_byte(byte, writer)?;
            }
            TerminalFilterState::Escape => match byte {
                b'[' => {
                    self.state = TerminalFilterState::Csi(vec![TERMINAL_ESCAPE, b'[']);
                }
                b']' => self.state = TerminalFilterState::Osc { escape: false },
                b'P' | b'^' | b'_' => {
                    self.state = TerminalFilterState::StringControl { escape: false };
                }
                byte if byte.is_ascii() => {}
                byte => {
                    self.state = TerminalFilterState::Text;
                    self.write_text_byte(byte, writer)?;
                }
            },
            TerminalFilterState::Csi(mut sequence) => {
                if byte == TERMINAL_ESCAPE {
                    self.state = TerminalFilterState::Escape;
                } else if (0x40..=0x7e).contains(&byte) {
                    let parameters = &sequence[2..];
                    if self.allow_sgr
                        && byte == b'm'
                        && parameters.iter().all(|parameter| {
                            parameter.is_ascii_digit() || matches!(parameter, b';' | b':')
                        })
                    {
                        sequence.push(byte);
                        writer.write_all(&sequence)?;
                    }
                } else if sequence.len() < 128 {
                    sequence.push(byte);
                    self.state = TerminalFilterState::Csi(sequence);
                } else {
                    self.state = TerminalFilterState::DiscardCsi;
                }
            }
            TerminalFilterState::DiscardCsi if byte == TERMINAL_ESCAPE => {
                self.state = TerminalFilterState::Escape;
            }
            TerminalFilterState::DiscardCsi if !(0x40..=0x7e).contains(&byte) => {
                self.state = TerminalFilterState::DiscardCsi;
            }
            TerminalFilterState::DiscardCsi => {}
            TerminalFilterState::Osc { escape } => {
                if byte == 0x07 || (escape && byte == b'\\') {
                    return Ok(());
                }
                self.state = TerminalFilterState::Osc {
                    escape: byte == TERMINAL_ESCAPE,
                };
            }
            TerminalFilterState::StringControl { escape } => {
                if escape && byte == b'\\' {
                    return Ok(());
                }
                self.state = TerminalFilterState::StringControl {
                    escape: byte == TERMINAL_ESCAPE,
                };
            }
        }
        Ok(())
    }

    fn write_text_byte(&mut self, byte: u8, writer: &mut impl Write) -> io::Result<()> {
        if byte.is_ascii() {
            self.finish_utf8(writer)?;
            if matches!(byte, b'\n' | b'\t') || (0x20..=0x7e).contains(&byte) {
                writer.write_all(&[byte])?;
            }
            return Ok(());
        }
        self.utf8_pending.push(byte);
        match std::str::from_utf8(&self.utf8_pending) {
            Ok(text) => {
                let character = text
                    .chars()
                    .next()
                    .expect("a completed UTF-8 sequence contains one character");
                match character {
                    '\u{009b}' => self.state = TerminalFilterState::DiscardCsi,
                    '\u{009d}' => self.state = TerminalFilterState::Osc { escape: false },
                    '\u{0090}' | '\u{009e}' | '\u{009f}' => {
                        self.state = TerminalFilterState::StringControl { escape: false };
                    }
                    character if !character.is_control() => writer.write_all(text.as_bytes())?,
                    _ => {}
                }
                self.utf8_pending.clear();
            }
            Err(error) if error.error_len().is_some() => {
                writer.write_all("\u{fffd}".as_bytes())?;
                self.utf8_pending.clear();
            }
            Err(_) => {}
        }
        Ok(())
    }

    fn finish_utf8(&mut self, writer: &mut impl Write) -> io::Result<()> {
        if !self.utf8_pending.is_empty() {
            writer.write_all("\u{fffd}".as_bytes())?;
            self.utf8_pending.clear();
        }
        Ok(())
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
    workbench_sessions: Arc<RwLock<Vec<String>>>,
}

fn workbench_ids(snapshot: &serde_json::Value, key: &str) -> Vec<String> {
    snapshot[key]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|value| value["id"].as_str().map(str::to_owned))
        .collect()
}

fn session_ids(cache: &RwLock<Vec<String>>) -> Vec<String> {
    cache
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

fn replace_session_ids(cache: &RwLock<Vec<String>>, mut ids: Vec<String>) {
    ids.sort();
    ids.dedup();
    *cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = ids;
}

fn remember_session(cache: &RwLock<Vec<String>>, session: &JsonValue) {
    let Some(id) = session.get("id").and_then(JsonValue::as_str) else {
        return;
    };
    let mut ids = cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !ids.iter().any(|known| known == id) {
        ids.push(id.to_owned());
        ids.sort();
    }
}

fn refresh_session_ids(bridge: &WorkbenchBridge, cache: &RwLock<Vec<String>>) {
    if let Ok(sessions) = bridge.agent_sessions() {
        replace_session_ids(
            cache,
            workbench_ids(&json!({ "sessions": sessions }), "sessions"),
        );
    }
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

fn active_agent_session(bridge: &WorkbenchBridge) -> Result<Option<JsonValue>, WorkbenchError> {
    let sessions = bridge.agent_sessions()?;
    Ok(sessions.as_array().and_then(|sessions| {
        sessions
            .iter()
            .find(|session| session.get("active").and_then(JsonValue::as_bool) == Some(true))
            .cloned()
    }))
}

fn agent_turn_session(bridge: &WorkbenchBridge) -> Result<Option<JsonValue>, WorkbenchError> {
    let snapshot = bridge.assembly()?;
    Ok(agent_turn_session_from_snapshot(&snapshot))
}

fn agent_turn_session_from_snapshot(snapshot: &JsonValue) -> Option<JsonValue> {
    ["activeAgentSession", "replayAgentSession"]
        .into_iter()
        .find_map(|field| {
            snapshot
                .get(field)
                .filter(|session| !session.is_null())
                .cloned()
        })
}

fn select_agent_turn(detail: &JsonValue, requested: Option<&str>) -> Result<JsonValue, String> {
    let turns = detail
        .get("turns")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| "the session detail has no turns list".to_owned())?;
    let turn = requested.map_or_else(
        || turns.last(),
        |id| {
            turns
                .iter()
                .find(|turn| turn.get("id").and_then(JsonValue::as_str) == Some(id))
        },
    );
    turn.cloned().ok_or_else(|| {
        requested.map_or_else(
            || "the selected session has no turns yet".to_owned(),
            |id| format!("turn {id} does not belong to the selected session"),
        )
    })
}

fn select_agent_turn_across_sessions(
    details: &[(String, JsonValue)],
    turn_id: &str,
) -> Result<JsonValue, String> {
    let mut matches = details
        .iter()
        .filter_map(|(session_id, detail)| {
            detail
                .get("turns")
                .and_then(JsonValue::as_array)
                .and_then(|turns| {
                    turns
                        .iter()
                        .find(|turn| turn.get("id").and_then(JsonValue::as_str) == Some(turn_id))
                })
                .map(|turn| (session_id.as_str(), turn.clone()))
        })
        .collect::<Vec<_>>();
    matches.sort_by_key(|(session_id, _)| *session_id);
    match matches.as_slice() {
        [] => Err(format!("turn {turn_id} does not belong to this workspace")),
        [(_, turn)] => Ok(turn.clone()),
        _ => Err(format!(
            "turn {turn_id} is ambiguous across sessions: {}",
            matches
                .iter()
                .map(|(session_id, _)| *session_id)
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn resolve_agent_turn_across_workspace(
    bridge: &WorkbenchBridge,
    turn_id: &str,
) -> Result<JsonValue, WorkbenchError> {
    let sessions = bridge.agent_sessions()?;
    let sessions = sessions.as_array().ok_or_else(|| {
        WorkbenchError::Malformed("agent session list is not an array".to_owned())
    })?;
    let mut session_ids = sessions
        .iter()
        .map(|session| {
            session
                .get("id")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
                .ok_or_else(|| {
                    WorkbenchError::Malformed("agent session summary has no id".to_owned())
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    session_ids.sort();
    session_ids.dedup();
    let details = session_ids
        .into_iter()
        .map(|session_id| {
            bridge
                .agent_session(&session_id)
                .map(|detail| (session_id, detail))
        })
        .collect::<Result<Vec<_>, _>>()?;
    select_agent_turn_across_sessions(&details, turn_id).map_err(WorkbenchError::Request)
}

fn agent_answer_record(turn: &JsonValue) -> Result<JsonValue, String> {
    let session_id = turn
        .get("sessionId")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "the durable agent turn has no sessionId".to_owned())?;
    let turn_id = turn
        .get("id")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "the durable agent turn has no id".to_owned())?;
    let presentation = turn
        .get("presentation")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| "the durable agent turn has no presentation".to_owned())?;
    for field in [
        "messages",
        "activity",
        "completeness",
        "sourceDigest",
        "sourceEventSequences",
    ] {
        if !presentation.contains_key(field) {
            return Err(format!(
                "the durable agent turn presentation has no {field}"
            ));
        }
    }
    Ok(json!({
        "type": AGENT_ANSWER_TYPE,
        "sessionId": session_id,
        "turnId": turn_id,
        "status": turn.get("status"),
        "outcome": turn.get("outcome"),
        "prompt": turn.get("prompt"),
        "input": turn.get("input"),
        "response": presentation.get("response"),
        "messages": presentation.get("messages"),
        "activity": presentation.get("activity"),
        "usage": presentation.get("usage"),
        "error": turn.get("error"),
        "evidence": {
            "sourceRevision": turn.get("sourceRevision"),
            "capabilityRevisions": turn.get("capabilityRevisions"),
            "sourceDigest": presentation.get("sourceDigest"),
            "sourceEventSequences": presentation.get("sourceEventSequences"),
            "completeness": presentation.get("completeness"),
        },
    }))
}

fn load_agent_answer(
    bridge: &WorkbenchBridge,
    session_id: &str,
    turn_id: &str,
    span: Span,
) -> Result<PipelineData, ShellError> {
    let detail = bridge.agent_session(session_id).map_err(|error| {
        recoverable_agent_turn_error(
            "Agent answer recovery failed",
            error,
            session_id,
            turn_id,
            span,
        )
    })?;
    let turn = select_agent_turn(&detail, Some(turn_id)).map_err(|error| {
        recoverable_agent_turn_error(
            "Agent answer recovery failed",
            error,
            session_id,
            turn_id,
            span,
        )
    })?;
    let answer = agent_answer_record(&turn).map_err(|error| {
        recoverable_agent_turn_error(
            "Agent answer projection failed",
            error,
            session_id,
            turn_id,
            span,
        )
    })?;
    Ok(json_to_nu(answer, span).into_pipeline_data())
}

fn recoverable_agent_turn_error(
    title: &'static str,
    detail: impl std::fmt::Display,
    session_id: &str,
    turn_id: &str,
    span: Span,
) -> ShellError {
    shell_error(
        title,
        format!(
            "{detail}; session {session_id} and turn {turn_id} are durable and can be reopened with `agent turn {turn_id}`"
        ),
        span,
    )
}

fn agent_session_start_error(error: WorkbenchError, span: Span) -> ShellError {
    match error {
        WorkbenchError::Cancelled(detail) => {
            shell_error("Agent session start cancelled", detail, span)
        }
        error => shell_error("Agent session start failed", error.to_string(), span),
    }
}

fn structured_agent_input(
    input: PipelineData,
    span: Span,
) -> Result<Option<JsonValue>, ShellError> {
    let value = match input {
        PipelineData::Empty => return Ok(None),
        PipelineData::Value(value, ..) => nu_to_json(value)?,
        PipelineData::ListStream(stream, ..) => {
            let mut values = Vec::new();
            let mut encoded_len = 2_usize;
            for value in stream {
                let value = nu_to_json(value)?;
                let item_len = serde_json::to_vec(&value)
                    .map_err(|error| {
                        shell_error("Agent input encoding failed", error.to_string(), span)
                    })?
                    .len();
                encoded_len = encoded_len
                    .saturating_add(item_len)
                    .saturating_add(usize::from(!values.is_empty()));
                if encoded_len > MAX_DRIVER_RECORD_BYTES {
                    return Err(agent_input_limit_error(span));
                }
                values.push(value);
            }
            JsonValue::Array(values)
        }
        PipelineData::ByteStream(stream, ..) => {
            let stream_type = stream.type_();
            if stream.known_size().is_some_and(|size| {
                size > u64::try_from(MAX_DRIVER_RECORD_BYTES).unwrap_or(u64::MAX)
            }) {
                return Err(agent_input_limit_error(span));
            }
            if stream_type == ByteStreamType::Binary {
                return Err(shell_error(
                    "Unsupported agent input",
                    "binary pipeline input cannot be represented as JSON".to_owned(),
                    span,
                ));
            }
            let mut bytes = Vec::new();
            if let Some(reader) = stream.reader() {
                reader
                    .take(u64::try_from(MAX_DRIVER_RECORD_BYTES).unwrap_or(u64::MAX) + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|error| {
                        shell_error("Agent input read failed", error.to_string(), span)
                    })?;
            }
            if bytes.len() > MAX_DRIVER_RECORD_BYTES {
                return Err(agent_input_limit_error(span));
            }
            JsonValue::String(String::from_utf8(bytes).map_err(|_| {
                shell_error(
                    "Unsupported agent input",
                    "pipeline input must be UTF-8 text or structured Nushell values".to_owned(),
                    span,
                )
            })?)
        }
    };
    let encoded_len = serde_json::to_vec(&value)
        .map_err(|error| shell_error("Agent input encoding failed", error.to_string(), span))?
        .len();
    if encoded_len > MAX_DRIVER_RECORD_BYTES {
        return Err(agent_input_limit_error(span));
    }
    Ok(Some(value))
}

fn agent_input_limit_error(span: Span) -> ShellError {
    shell_error(
        "Agent input exceeds the transport limit",
        format!(
            "structured pipeline input must be at most {MAX_DRIVER_RECORD_BYTES} encoded bytes"
        ),
        span,
    )
}

#[derive(Clone)]
struct AgentCommand {
    bridge: WorkbenchBridge,
    session_ids: Arc<RwLock<Vec<String>>>,
    status: AgentStatusPresenter,
}

impl Command for AgentCommand {
    fn name(&self) -> &'static str {
        "agent"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Any, Type::Any)
            .optional(
                "prompt",
                SyntaxShape::String,
                "prompt for the active agent session",
            )
            .switch(
                "stream",
                "stream assistant Markdown as text instead of returning a durable answer record",
                None,
            )
            .switch("raw", "stream raw attributable session events", None)
    }

    fn description(&self) -> &'static str {
        "Continue the active harness-native agent session"
    }

    #[allow(clippy::too_many_lines)]
    fn run(
        &self,
        engine_state: &EngineState,
        stack: &mut Stack,
        call: &nu_protocol::engine::Call<'_>,
        input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let prompt = call.opt::<String>(engine_state, stack, 0)?;
        let Some(prompt) = prompt else {
            let value = active_agent_session(&self.bridge).map_err(|error| {
                shell_error("Agent session request failed", error.to_string(), call.head)
            })?;
            return Ok(json_to_nu(value.unwrap_or(JsonValue::Null), call.head).into_pipeline_data());
        };
        let raw = call.has_flag(engine_state, stack, "raw")?;
        let stream = call.has_flag(engine_state, stack, "stream")?;
        if raw && stream {
            return Err(shell_error(
                "Conflicting agent projections",
                "choose either `--stream` for assistant Markdown or `--raw` for attributable events"
                    .to_owned(),
                call.head,
            ));
        }
        let structured_input = structured_agent_input(input, call.head)?;
        let mut status = (!raw && !stream)
            .then(|| self.status.begin(stack))
            .flatten();
        let active = active_agent_session(&self.bridge).map_err(|error| {
            shell_error("Agent session request failed", error.to_string(), call.head)
        })?;
        let (session, include_startup) = if let Some(session) = active {
            (session, false)
        } else {
            let starting = self
                .bridge
                .start_agent_session_interruptible(None, None, || {
                    engine_state.signals().interrupted()
                })
                .map_err(|error| agent_session_start_error(error, call.head))?;
            let session_id = starting
                .get("id")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    shell_error(
                        "Agent session start failed",
                        "the controller returned a session without an id".to_owned(),
                        call.head,
                    )
                })?;
            if let Some(status) = status.as_mut() {
                status.set_phase("starting", Some("Starting agent session"));
            }
            let mut on_progress = |progress: &JsonValue| {
                if let Some(status) = status.as_mut() {
                    status.apply_progress(progress);
                }
            };
            let mut interrupted = || engine_state.signals().interrupted();
            let ready = self
                .bridge
                .wait_for_agent_session_ready(session_id, &mut on_progress, &mut interrupted)
                .map_err(|error| agent_session_start_error(error, call.head))?;
            (ready, true)
        };
        remember_session(&self.session_ids, &session);
        if let Some(status) = status.as_mut() {
            status.set_phase("preparing", Some("Preparing turn"));
        }
        let turn = self
            .bridge
            .start_agent_turn(
                session,
                &prompt,
                structured_input.as_ref(),
                raw,
                include_startup,
                !raw && !stream,
            )
            .map_err(|error| shell_error("Agent turn failed", error.to_string(), call.head))?;
        let session_id = turn
            .session
            .get("id")
            .and_then(JsonValue::as_str)
            .unwrap_or("unknown")
            .to_owned();
        if raw {
            return Ok(ListStream::new(
                AgentTurnValueStream {
                    turn,
                    bridge: self.bridge.clone(),
                    session_id,
                    signals: engine_state.signals().clone(),
                    span: call.head,
                    finished: false,
                },
                call.head,
                Signals::empty(),
            )
            .into());
        }
        if stream {
            return Ok(ByteStream::from_result_iter(
                AgentTurnTextStream {
                    turn,
                    bridge: self.bridge.clone(),
                    session_id,
                    signals: engine_state.signals().clone(),
                    span: call.head,
                    finished: false,
                    saw_text: false,
                    ended_with_newline: false,
                    streamed_text: String::new(),
                    saw_completed: false,
                    pending_error: None,
                },
                call.head,
                engine_state.signals().clone(),
                ByteStreamType::String,
            )
            .into());
        }
        let result = collect_agent_answer(
            &turn,
            &self.bridge,
            &session_id,
            engine_state.signals(),
            call.head,
            status.as_mut(),
        );
        drop(status);
        result
    }
}

fn collect_agent_answer(
    turn: &AgentTurnStream,
    bridge: &WorkbenchBridge,
    session_id: &str,
    signals: &Signals,
    span: Span,
    mut status: Option<&mut AgentStatusGuard>,
) -> Result<PipelineData, ShellError> {
    let turn_id = turn
        .turn
        .get("id")
        .and_then(JsonValue::as_str)
        .unwrap_or("unknown")
        .to_owned();
    loop {
        if signals.interrupted() {
            bridge.cancel_agent_turn_detached(session_id);
            return Err(recoverable_agent_turn_error(
                "Agent turn cancelled",
                "the controller is preserving the cancelled turn",
                session_id,
                &turn_id,
                span,
            ));
        }
        match turn.receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(Ok(AgentTurnOutput::Progress(progress))) => {
                if let Some(status) = status.as_deref_mut() {
                    status.apply_progress(&progress);
                }
            }
            Ok(Ok(AgentTurnOutput::AssistantDelta(_))) => {
                if let Some(status) = status.as_deref_mut() {
                    status.set_phase("responding", Some("Writing response"));
                }
            }
            Ok(Ok(AgentTurnOutput::AssistantCompleted(_))) => {
                if let Some(status) = status.as_deref_mut() {
                    status.set_phase("finalizing", Some("Finalizing answer"));
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Some(status) = status.as_deref_mut() {
                    status.tick();
                }
            }
            Ok(Ok(AgentTurnOutput::Finished { .. })) => {
                if let Some(status) = status.as_deref_mut() {
                    status.set_phase("finalizing", Some("Saving evidence"));
                }
                return load_agent_answer(bridge, session_id, &turn_id, span);
            }
            Ok(Ok(AgentTurnOutput::Raw(_))) => {
                return Err(recoverable_agent_turn_error(
                    "Agent answer projection failed",
                    "received a raw event in answer mode",
                    session_id,
                    &turn_id,
                    span,
                ));
            }
            Ok(Err(error)) => {
                return Err(recoverable_agent_turn_error(
                    "Agent turn stream failed",
                    error,
                    session_id,
                    &turn_id,
                    span,
                ));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(recoverable_agent_turn_error(
                    "Agent turn stream failed",
                    "the event stream ended before the turn completed",
                    session_id,
                    &turn_id,
                    span,
                ));
            }
        }
    }
}

struct AgentTurnValueStream {
    turn: AgentTurnStream,
    bridge: WorkbenchBridge,
    session_id: String,
    signals: Signals,
    span: Span,
    finished: bool,
}

impl Iterator for AgentTurnValueStream {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.signals.interrupted() {
                self.bridge.cancel_agent_turn_detached(&self.session_id);
                self.finished = true;
                return None;
            }
            match self.turn.receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(Ok(AgentTurnOutput::Raw(value))) => {
                    return Some(json_to_nu(value, self.span));
                }
                Ok(Ok(
                    AgentTurnOutput::Progress(_)
                    | AgentTurnOutput::AssistantDelta(_)
                    | AgentTurnOutput::AssistantCompleted(_)
                    | AgentTurnOutput::Finished { .. },
                )) => {
                    self.finished = true;
                    return Some(Value::error(
                        shell_error(
                            "Agent raw stream failed",
                            format!(
                                "received an answer projection in raw mode; session {} and turn {} can be reopened",
                                self.session_id,
                                self.turn_id()
                            ),
                            self.span,
                        ),
                        self.span,
                    ));
                }
                Ok(Err(error)) => {
                    self.finished = true;
                    return Some(Value::error(
                        shell_error("Agent turn stream failed", error.to_string(), self.span),
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

impl AgentTurnValueStream {
    fn turn_id(&self) -> &str {
        self.turn
            .turn
            .get("id")
            .and_then(JsonValue::as_str)
            .unwrap_or("unknown")
    }
}

impl Drop for AgentTurnValueStream {
    fn drop(&mut self) {
        if !self.finished && self.signals.interrupted() {
            self.bridge.cancel_agent_turn_detached(&self.session_id);
        }
    }
}

#[allow(clippy::struct_excessive_bools)]
struct AgentTurnTextStream {
    turn: AgentTurnStream,
    bridge: WorkbenchBridge,
    session_id: String,
    signals: Signals,
    span: Span,
    finished: bool,
    saw_text: bool,
    ended_with_newline: bool,
    streamed_text: String,
    saw_completed: bool,
    pending_error: Option<ShellError>,
}

impl AgentTurnTextStream {
    fn turn_id(&self) -> &str {
        self.turn
            .turn
            .get("id")
            .and_then(JsonValue::as_str)
            .unwrap_or("unknown")
    }

    fn recoverable_error(&self, title: &'static str, detail: impl std::fmt::Display) -> ShellError {
        recoverable_agent_turn_error(title, detail, &self.session_id, self.turn_id(), self.span)
    }

    fn finish_with(&mut self, error: Option<ShellError>) -> Option<Result<Vec<u8>, ShellError>> {
        self.finished = true;
        if self.saw_text && !self.ended_with_newline {
            self.pending_error = error;
            self.ended_with_newline = true;
            return Some(Ok(vec![b'\n']));
        }
        error.map(Err)
    }
}

impl Iterator for AgentTurnTextStream {
    type Item = Result<Vec<u8>, ShellError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(error) = self.pending_error.take() {
            return Some(Err(error));
        }
        if self.finished {
            return None;
        }
        loop {
            if self.signals.interrupted() {
                self.bridge.cancel_agent_turn_detached(&self.session_id);
                self.finished = true;
                return None;
            }
            match self.turn.receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(Ok(AgentTurnOutput::AssistantDelta(text))) => {
                    if text.is_empty() {
                        continue;
                    }
                    self.saw_text = true;
                    self.ended_with_newline = text.ends_with('\n');
                    self.streamed_text.push_str(&text);
                    return Some(Ok(text.into_bytes()));
                }
                Ok(Ok(AgentTurnOutput::AssistantCompleted(text))) => {
                    self.saw_completed = true;
                    if text == self.streamed_text {
                        continue;
                    }
                    if let Some(suffix) = text.strip_prefix(&self.streamed_text) {
                        if suffix.is_empty() {
                            continue;
                        }
                        let suffix = suffix.to_owned();
                        self.saw_text = true;
                        self.ended_with_newline = suffix.ends_with('\n');
                        text.clone_into(&mut self.streamed_text);
                        return Some(Ok(suffix.into_bytes()));
                    }
                    let error = self.recoverable_error(
                        "Agent answer stream changed",
                        "the completed answer differs from the text already streamed",
                    );
                    return self.finish_with(Some(error));
                }
                Ok(Ok(AgentTurnOutput::Finished { outcome })) => {
                    let error = match outcome.as_str() {
                        "completed" if self.saw_completed => None,
                        "completed" => Some(self.recoverable_error(
                            "Agent returned no answer",
                            "the turn completed without an authoritative assistant answer",
                        )),
                        "aborted" | "cancelled" => None,
                        _ => Some(self.recoverable_error(
                            "Agent turn failed",
                            format_args!("the turn finished with outcome {outcome}"),
                        )),
                    };
                    return self.finish_with(error);
                }
                Ok(Ok(AgentTurnOutput::Raw(_))) => {
                    let error = self.recoverable_error(
                        "Agent answer stream failed",
                        "received a raw event in answer mode",
                    );
                    return self.finish_with(Some(error));
                }
                Ok(Err(error)) => {
                    let error = self.recoverable_error("Agent turn stream failed", error);
                    return self.finish_with(Some(error));
                }
                Ok(Ok(AgentTurnOutput::Progress(_))) | Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    let error = self.recoverable_error(
                        "Agent turn stream failed",
                        "the event stream ended before the turn completed",
                    );
                    return self.finish_with(Some(error));
                }
            }
        }
    }
}

impl Drop for AgentTurnTextStream {
    fn drop(&mut self) {
        if !self.finished && self.signals.interrupted() {
            self.bridge.cancel_agent_turn_detached(&self.session_id);
        }
    }
}

#[derive(Clone)]
struct AgentNewCommand {
    bridge: WorkbenchBridge,
    session_ids: Arc<RwLock<Vec<String>>>,
    status: AgentStatusPresenter,
}

impl Command for AgentNewCommand {
    fn name(&self) -> &'static str {
        "agent new"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Nothing, Type::Record(Vec::new().into()))
            .named("harness", SyntaxShape::String, "harness override", None)
            .named("model", SyntaxShape::String, "model profile override", None)
    }

    fn description(&self) -> &'static str {
        "Start and activate a new harness-native agent session"
    }

    fn run(
        &self,
        engine_state: &EngineState,
        stack: &mut Stack,
        call: &nu_protocol::engine::Call<'_>,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let harness = call.get_flag::<String>(engine_state, stack, "harness")?;
        let model = call.get_flag::<String>(engine_state, stack, "model")?;
        let mut status = self.status.begin(stack);
        if let Some(status) = status.as_mut() {
            status.set_phase("starting", Some("Starting agent session"));
        }
        let starting = self
            .bridge
            .start_agent_session_interruptible(harness.as_deref(), model.as_deref(), || {
                engine_state.signals().interrupted()
            })
            .map_err(|error| agent_session_start_error(error, call.head))?;
        let session_id = starting
            .get("id")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                shell_error(
                    "Agent session start failed",
                    "the controller returned a session without an id".to_owned(),
                    call.head,
                )
            })?;
        let mut on_progress = |progress: &JsonValue| {
            if let Some(status) = status.as_mut() {
                status.apply_progress(progress);
            }
        };
        let mut interrupted = || engine_state.signals().interrupted();
        let session = self
            .bridge
            .wait_for_agent_session_ready(session_id, &mut on_progress, &mut interrupted)
            .map_err(|error| agent_session_start_error(error, call.head))?;
        remember_session(&self.session_ids, &session);
        Ok(json_to_nu(session, call.head).into_pipeline_data())
    }
}

#[derive(Clone)]
struct AgentSessionsCommand {
    bridge: WorkbenchBridge,
    session_ids: Arc<RwLock<Vec<String>>>,
}

impl Command for AgentSessionsCommand {
    fn name(&self) -> &'static str {
        "agent sessions"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Nothing, Type::Table(Vec::new().into()))
    }

    fn description(&self) -> &'static str {
        "List persistent agent sessions for this workspace"
    }

    fn run(
        &self,
        _engine_state: &EngineState,
        _stack: &mut Stack,
        call: &nu_protocol::engine::Call<'_>,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let sessions = self.bridge.agent_sessions().map_err(|error| {
            shell_error("Agent session request failed", error.to_string(), call.head)
        })?;
        replace_session_ids(
            &self.session_ids,
            workbench_ids(&json!({ "sessions": sessions.clone() }), "sessions"),
        );
        Ok(json_to_nu(sessions, call.head).into_pipeline_data())
    }
}

#[derive(Clone)]
struct AgentSwitchCommand {
    bridge: WorkbenchBridge,
    session_ids: Arc<RwLock<Vec<String>>>,
}

impl Command for AgentSwitchCommand {
    fn name(&self) -> &'static str {
        "agent switch"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Nothing, Type::Record(Vec::new().into()))
            .required(
                "session-id",
                SyntaxShape::String,
                "ready session to activate",
            )
    }

    fn description(&self) -> &'static str {
        "Switch the active workspace agent session"
    }

    fn run(
        &self,
        engine_state: &EngineState,
        stack: &mut Stack,
        call: &nu_protocol::engine::Call<'_>,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let session_id = call.req::<String>(engine_state, stack, 0)?;
        let session = self
            .bridge
            .activate_agent_session(&session_id)
            .map_err(|error| shell_error("Agent switch failed", error.to_string(), call.head))?;
        remember_session(&self.session_ids, &session);
        refresh_session_ids(&self.bridge, &self.session_ids);
        Ok(json_to_nu(session, call.head).into_pipeline_data())
    }
}

#[derive(Clone)]
struct AgentTurnCommand {
    bridge: WorkbenchBridge,
}

impl Command for AgentTurnCommand {
    fn name(&self) -> &'static str {
        "agent turn"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Nothing, Type::Record(Vec::new().into()))
            .optional(
                "turn-id",
                SyntaxShape::String,
                "durable turn to inspect across this workspace; omit for the active or replay session's latest turn",
            )
    }

    fn description(&self) -> &'static str {
        "Inspect a durable agent turn"
    }

    fn run(
        &self,
        engine_state: &EngineState,
        stack: &mut Stack,
        call: &nu_protocol::engine::Call<'_>,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let requested = call.opt::<String>(engine_state, stack, 0)?;
        if let Some(turn_id) = requested {
            let turn =
                resolve_agent_turn_across_workspace(&self.bridge, &turn_id).map_err(|error| {
                    shell_error("Agent turn request failed", error.to_string(), call.head)
                })?;
            let answer = agent_answer_record(&turn).map_err(|detail| {
                shell_error("Agent answer projection failed", detail, call.head)
            })?;
            return Ok(json_to_nu(answer, call.head).into_pipeline_data());
        }
        let session = agent_turn_session(&self.bridge)
            .map_err(|error| {
                shell_error("Agent turn request failed", error.to_string(), call.head)
            })?
            .ok_or_else(|| {
                shell_error(
                    "No active agent",
                    "start an agent session first".to_owned(),
                    call.head,
                )
            })?;
        let session_id = session
            .get("id")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                shell_error(
                    "Agent turn request failed",
                    "the selected session has no id".to_owned(),
                    call.head,
                )
            })?;
        let detail = self.bridge.agent_session(session_id).map_err(|error| {
            shell_error("Agent turn request failed", error.to_string(), call.head)
        })?;
        let turn = select_agent_turn(&detail, None)
            .map_err(|detail| shell_error("Agent turn not found", detail, call.head))?;
        let answer = agent_answer_record(&turn)
            .map_err(|detail| shell_error("Agent answer projection failed", detail, call.head))?;
        Ok(json_to_nu(answer, call.head).into_pipeline_data())
    }
}

#[derive(Clone)]
struct AgentCancelCommand {
    bridge: WorkbenchBridge,
}

impl Command for AgentCancelCommand {
    fn name(&self) -> &'static str {
        "agent cancel"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Nothing, Type::Record(Vec::new().into()))
    }

    fn description(&self) -> &'static str {
        "Cancel the active agent turn"
    }

    fn run(
        &self,
        _engine_state: &EngineState,
        _stack: &mut Stack,
        call: &nu_protocol::engine::Call<'_>,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let session = active_agent_session(&self.bridge)
            .map_err(|error| shell_error("Agent cancel failed", error.to_string(), call.head))?
            .ok_or_else(|| {
                shell_error(
                    "No active agent",
                    "start an agent turn first".to_owned(),
                    call.head,
                )
            })?;
        let session_id = session["id"].as_str().unwrap_or("unknown");
        self.bridge
            .cancel_agent_turn(session_id)
            .map_err(|error| shell_error("Agent cancel failed", error.to_string(), call.head))?;
        Ok(json_to_nu(
            json!({ "sessionId": session_id, "status": "cancelling" }),
            call.head,
        )
        .into_pipeline_data())
    }
}

#[derive(Clone)]
struct AgentCloseCommand {
    bridge: WorkbenchBridge,
    session_ids: Arc<RwLock<Vec<String>>>,
}

impl Command for AgentCloseCommand {
    fn name(&self) -> &'static str {
        "agent close"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Nothing, Type::Record(Vec::new().into()))
            .optional(
                "session-id",
                SyntaxShape::String,
                "session to close; omit for the active session",
            )
    }

    fn description(&self) -> &'static str {
        "Close an agent session"
    }

    fn run(
        &self,
        engine_state: &EngineState,
        stack: &mut Stack,
        call: &nu_protocol::engine::Call<'_>,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let requested = call.opt::<String>(engine_state, stack, 0)?;
        let session_id = if let Some(session_id) = requested {
            session_id
        } else {
            active_agent_session(&self.bridge)
                .map_err(|error| shell_error("Agent close failed", error.to_string(), call.head))?
                .ok_or_else(|| {
                    shell_error(
                        "No active agent",
                        "provide a session id or start an agent session first".to_owned(),
                        call.head,
                    )
                })?
                .get("id")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    shell_error(
                        "Agent close failed",
                        "the active session has no id".to_owned(),
                        call.head,
                    )
                })?
                .to_owned()
        };
        let session = self
            .bridge
            .close_agent_session(&session_id)
            .map_err(|error| shell_error("Agent close failed", error.to_string(), call.head))?;
        refresh_session_ids(&self.bridge, &self.session_ids);
        Ok(json_to_nu(session, call.head).into_pipeline_data())
    }
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
        workbench_sessions: Arc<RwLock<Vec<String>>>,
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
            workbench_sessions,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::mpsc};

    use reedline::Completer;

    use super::*;

    fn completer() -> AgentLabCompleter {
        AgentLabCompleter {
            commands: vec!["lab assembly".to_owned(), "lab compare".to_owned()],
            workbench_harnesses: vec!["v0".to_owned(), "eve".to_owned()],
            workbench_models: vec!["haiku-4.5".to_owned()],
            workbench_sessions: Arc::new(RwLock::new(vec!["agent-session-1".to_owned()])),
        }
    }

    fn text_stream(outputs: impl IntoIterator<Item = AgentTurnOutput>) -> AgentTurnTextStream {
        let bridge = WorkbenchBridge::new(
            "http://127.0.0.1:9",
            "workspace-1".to_owned(),
            "token".to_owned(),
        )
        .unwrap();
        let (sender, receiver) = mpsc::channel();
        for output in outputs {
            sender.send(Ok(output)).unwrap();
        }
        drop(sender);
        AgentTurnTextStream {
            turn: AgentTurnStream {
                session: json!({ "id": "agent-session-1" }),
                turn: json!({ "id": "agent-turn-1" }),
                receiver,
            },
            bridge,
            session_id: "agent-session-1".to_owned(),
            signals: Signals::empty(),
            span: Span::unknown(),
            finished: false,
            saw_text: false,
            ended_with_newline: false,
            streamed_text: String::new(),
            saw_completed: false,
            pending_error: None,
        }
    }

    fn completion_values(line: &str) -> Vec<String> {
        completer()
            .complete(line, line.len())
            .into_iter()
            .map(|suggestion| suggestion.value)
            .collect()
    }

    fn durable_turn() -> JsonValue {
        json!({
            "id": "agent-turn-1",
            "sessionId": "agent-session-1",
            "status": "completed",
            "outcome": "completed",
            "prompt": "What changed?",
            "input": { "scope": "workspace" },
            "sourceRevision": "revision-1",
            "capabilityRevisions": { "catalog": "catalog-2" },
            "error": null,
            "presentation": {
                "schemaVersion": 2,
                "response": "# Answer\n\nIt worked.",
                "messages": [{
                    "id": "message-1",
                    "text": "# Answer\n\nIt worked.",
                    "complete": true,
                    "sourceEventSequences": [4]
                }],
                "activity": [{
                    "kind": "capability-call",
                    "title": "catalog · list",
                    "detail": null,
                    "status": "completed",
                    "source": "catalog",
                    "path": null,
                    "operation": "list",
                    "callId": "call-1",
                    "arguments": { "active": true },
                    "result": {
                        "items": [{ "name": "gamma", "score": 8 }]
                    },
                    "sourceEventSequences": [2, 3]
                }],
                "usage": { "inputTokens": 12 },
                "completeness": {
                    "assistantOutput": "complete",
                    "capabilityActivity": "complete",
                    "nativeActivity": "complete",
                    "workspaceEffects": "complete",
                    "usage": "complete"
                },
                "sourceEventSequences": [3, 4, 5],
                "sourceDigest": "sha256:answer"
            }
        })
    }

    struct TestMarkdownRenderer {
        fail: bool,
    }

    impl MarkdownRenderer for TestMarkdownRenderer {
        fn render(&self, markdown: &str, width: usize) -> Result<String, String> {
            if self.fail {
                Err("renderer unavailable".to_owned())
            } else {
                Ok(format!("rendered[{width}]:{markdown}"))
            }
        }
    }

    struct TerminalControlRenderer;

    impl MarkdownRenderer for TerminalControlRenderer {
        fn render(&self, _markdown: &str, _width: usize) -> Result<String, String> {
            Ok(concat!(
                "safe",
                "\u{1b}[38;2;143;181;115m",
                "green",
                "\u{1b}[0m",
                "\u{1b}]52;c;clipboard payload\u{7}",
                "\u{1b}[2J",
                "done\n\t"
            )
            .to_owned())
        }
    }

    #[derive(Clone)]
    struct RecordingWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for RecordingWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.bytes.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("status sink unavailable"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("status sink unavailable"))
        }
    }

    fn recording_status_presenter(width: u16) -> (AgentStatusPresenter, Arc<Mutex<Vec<u8>>>) {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let presenter = AgentStatusPresenter::with_writer(
            Box::new(RecordingWriter {
                bytes: bytes.clone(),
            }),
            Arc::new(move || Some(width)),
        );
        (presenter, bytes)
    }

    fn assert_only_clear_line_controls(bytes: &[u8]) {
        let mut index = 0;
        while index < bytes.len() {
            if bytes[index] != TERMINAL_ESCAPE {
                index += 1;
                continue;
            }
            let tail = &bytes[index..];
            assert!(
                tail.starts_with(b"\x1b[1G") || tail.starts_with(b"\x1b[2K"),
                "unexpected terminal control in {bytes:?}"
            );
            index += 4;
        }
    }

    #[derive(Clone)]
    struct SplitControlStreamCommand;

    impl Command for SplitControlStreamCommand {
        fn name(&self) -> &'static str {
            "test control stream"
        }

        fn signature(&self) -> Signature {
            Signature::build(self.name()).input_output_type(Type::Nothing, Type::String)
        }

        fn description(&self) -> &'static str {
            "Test-only split text stream"
        }

        fn run(
            &self,
            _engine_state: &EngineState,
            _stack: &mut Stack,
            call: &nu_protocol::engine::Call<'_>,
            _input: PipelineData,
        ) -> Result<PipelineData, ShellError> {
            Ok(ByteStream::from_iter(
                split_control_chunks(),
                call.head,
                Signals::empty(),
                ByteStreamType::String,
            )
            .into())
        }
    }

    fn split_control_chunks() -> Vec<Vec<u8>> {
        vec![
            b"safe\x1b[38;2;".to_vec(),
            b"143;181;115mgreen\x1b]52;c;clip".to_vec(),
            b"board\x1b".to_vec(),
            b"\\after-osc\x1b[2".to_vec(),
            b"Jafter-cursor\xc2".to_vec(),
            b"\x9b2Junicode-\xc3".to_vec(),
            b"\xa9\x1b[0m\n".to_vec(),
        ]
    }

    fn split_control_bytes() -> Vec<u8> {
        split_control_chunks().concat()
    }

    fn host_with_split_control_stream() -> NushellHost {
        let mut host = NushellHost::new();
        let mut working_set = StateWorkingSet::new(&host.engine_state);
        working_set.add_decl(Box::new(SplitControlStreamCommand));
        host.engine_state.merge_delta(working_set.render()).unwrap();
        host
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
        assert!(host.engine_state.find_decl(b"from md", &[]).is_some());
    }

    #[test]
    fn explore_shell_executes_from_md_as_a_structured_parser() {
        let mut host = NushellHost::new();

        let parsed = host.eval("'# Title\n\n- one\n- two' | from md").unwrap();
        let parsed = nu_to_json(parsed).unwrap();
        let rows = parsed.as_array().unwrap();
        let content = rows
            .iter()
            .filter_map(|row| row.get("content").and_then(JsonValue::as_str))
            .collect::<Vec<_>>();

        assert!(content.contains(&"Title"));
        assert!(content.contains(&"one"));
        assert!(content.contains(&"two"));
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

    #[test]
    fn agent_completion_exposes_the_two_explicit_stream_projections() {
        assert_eq!(completion_values("agent 'prompt' --s"), ["--stream"]);
        assert_eq!(completion_values("agent 'prompt' --r"), ["--raw"]);
    }

    #[test]
    fn session_completion_reads_live_session_ids() {
        let sessions = Arc::new(RwLock::new(vec!["agent-session-1".to_owned()]));
        let mut completer = AgentLabCompleter {
            commands: vec!["agent switch".to_owned(), "agent close".to_owned()],
            workbench_harnesses: Vec::new(),
            workbench_models: Vec::new(),
            workbench_sessions: sessions.clone(),
        };
        replace_session_ids(&sessions, vec!["agent-session-2".to_owned()]);

        for line in ["agent switch agent-session-", "agent close agent-session-"] {
            let values = completer
                .complete(line, line.len())
                .into_iter()
                .map(|suggestion| suggestion.value)
                .collect::<Vec<_>>();
            assert_eq!(values, ["agent-session-2"]);
        }
    }

    #[test]
    fn structured_agent_input_preserves_values_and_enforces_the_driver_limit() {
        let value = structured_agent_input(
            json_to_nu(json!({ "active": true }), Span::unknown()).into_pipeline_data(),
            Span::unknown(),
        )
        .unwrap();
        assert_eq!(value, Some(json!({ "active": true })));

        let oversized = Value::string("x".repeat(MAX_DRIVER_RECORD_BYTES), Span::unknown())
            .into_pipeline_data();
        assert!(structured_agent_input(oversized, Span::unknown()).is_err());
    }

    #[test]
    fn transient_agent_status_requires_an_interactive_stderr_and_capable_terminal() {
        assert!(interactive_agent_status_enabled(true, None));
        assert!(interactive_agent_status_enabled(
            true,
            Some("xterm-256color")
        ));
        assert!(!interactive_agent_status_enabled(true, Some("dumb")));
        assert!(!interactive_agent_status_enabled(true, Some("DUMB")));
        assert!(!interactive_agent_status_enabled(
            false,
            Some("xterm-256color")
        ));
    }

    #[test]
    fn transient_agent_status_is_opt_in_and_respects_stderr_redirection() {
        let (presenter, bytes) = recording_status_presenter(80);
        assert!(presenter.begin(&Stack::new()).is_none());
        assert!(bytes.lock().unwrap().is_empty());

        presenter.set_enabled(true);
        assert!(presenter.begin(&Stack::new().capture_all()).is_none());
        assert!(bytes.lock().unwrap().is_empty());

        drop(presenter.begin(&Stack::new()));
        assert!(!bytes.lock().unwrap().is_empty());
    }

    #[test]
    fn transient_agent_status_is_bounded_sanitized_and_cleared_on_drop() {
        let (presenter, bytes) = recording_status_presenter(52);
        presenter.set_enabled(true);
        {
            let mut status = presenter.begin(&Stack::new()).unwrap();
            status.apply_progress(&json!({
                "progress": {
                    "phase": "acting",
                    "detail": "Inspecting\u{1b}[2J\n  catalog \u{2603} with a very long explanation"
                }
            }));
            let after_one_second = status.started + Duration::from_secs(1);
            status.tick_at(after_one_second);
            assert_eq!(status.phase, "using tools");
            assert_eq!(
                status.detail.as_deref(),
                Some("Inspecting[2J catalog ? with a very long explanation")
            );
        }

        let bytes = bytes.lock().unwrap().clone();
        let output = String::from_utf8_lossy(&bytes);
        assert!(output.contains("Agent: preparing"));
        assert!(output.contains("Agent: using tools"));
        assert!(output.contains("\u{b7} 1s"));
        assert!(!output.contains("\u{1b}[2J"));
        assert!(!output.contains('\u{2603}'));
        assert!(output.contains("..."));
        assert_only_clear_line_controls(&bytes);
        assert!(bytes.ends_with(b"\x1b[2K"));
    }

    #[test]
    fn transient_agent_status_accepts_projection_and_payload_shapes() {
        let (presenter, _bytes) = recording_status_presenter(120);
        presenter.set_enabled(true);
        let mut status = presenter.begin(&Stack::new()).unwrap();

        status.apply_progress(&json!({
            "phase": "reasoning",
            "detail": "Considering the workspace"
        }));
        assert_eq!(status.phase, "thinking");
        assert_eq!(status.detail.as_deref(), Some("Considering the workspace"));

        status.apply_progress(&json!({
            "payload": {
                "phase": "responding",
                "message": "Writing the answer"
            }
        }));
        assert_eq!(status.phase, "answering");
        assert_eq!(status.detail.as_deref(), Some("Writing the answer"));
    }

    #[test]
    fn transient_agent_status_writer_failure_is_best_effort() {
        let presenter =
            AgentStatusPresenter::with_writer(Box::new(FailingWriter), Arc::new(|| Some(80)));
        presenter.set_enabled(true);
        let mut status = presenter.begin(&Stack::new()).unwrap();
        status.set_phase("responding", Some("still succeeds"));
        drop(status);
    }

    #[test]
    fn transient_agent_status_truncation_honors_small_widths() {
        assert_eq!(truncate_agent_status("abcdef", 6), "abcdef");
        assert_eq!(truncate_agent_status("abcdef", 5), "ab...");
        assert_eq!(truncate_agent_status("abcdef", 3), "...");
        assert_eq!(truncate_agent_status("abcdef", 1), ".");
    }

    #[test]
    fn durable_turns_project_to_the_canonical_agent_answer_shape() {
        let answer = agent_answer_record(&durable_turn()).unwrap();

        let mut fields = answer.as_object().unwrap().keys().collect::<Vec<_>>();
        fields.sort();
        assert_eq!(
            fields,
            [
                "activity",
                "error",
                "evidence",
                "input",
                "messages",
                "outcome",
                "prompt",
                "response",
                "sessionId",
                "status",
                "turnId",
                "type",
                "usage",
            ]
        );

        assert_eq!(answer["type"], AGENT_ANSWER_TYPE);
        assert_eq!(answer["sessionId"], "agent-session-1");
        assert_eq!(answer["turnId"], "agent-turn-1");
        assert_eq!(answer["prompt"], "What changed?");
        assert_eq!(answer["response"], "# Answer\n\nIt worked.");
        assert_eq!(answer["evidence"]["sourceRevision"], "revision-1");
        assert_eq!(
            answer["evidence"]["capabilityRevisions"]["catalog"],
            "catalog-2"
        );
        assert_eq!(answer["evidence"]["sourceEventSequences"], json!([3, 4, 5]));
        assert!(answer["evidence"].get("prompt").is_none());
    }

    #[test]
    fn typed_capability_activity_remains_structured_in_nushell() {
        let answer = json_to_nu(
            agent_answer_record(&durable_turn()).unwrap(),
            Span::unknown(),
        );
        let activity = answer
            .as_record()
            .unwrap()
            .get("activity")
            .unwrap()
            .as_list()
            .unwrap();
        let capability = activity[0].as_record().unwrap();
        let arguments = capability.get("arguments").unwrap().as_record().unwrap();
        let result = capability.get("result").unwrap().as_record().unwrap();
        let items = result.get("items").unwrap().as_list().unwrap();

        assert!(capability.get("detail").unwrap().is_nothing());
        assert_eq!(arguments.get("active").unwrap().as_bool(), Ok(true));
        assert_eq!(
            items[0].as_record().unwrap().get("score").unwrap().as_int(),
            Ok(8)
        );
    }

    #[test]
    fn failed_and_partial_turns_remain_structured_and_reopenable() {
        let mut turn = durable_turn();
        turn["status"] = json!("failed");
        turn["outcome"] = json!("failed");
        turn["error"] = json!("driver stopped");
        turn["presentation"]["response"] = JsonValue::Null;
        turn["presentation"]["completeness"]["assistantOutput"] = json!("partial");

        let answer = agent_answer_record(&turn).unwrap();

        assert_eq!(answer["status"], "failed");
        assert_eq!(answer["outcome"], "failed");
        assert_eq!(answer["error"], "driver stopped");
        assert!(answer["response"].is_null());
        assert_eq!(
            answer["evidence"]["completeness"]["assistantOutput"],
            "partial"
        );
    }

    #[test]
    fn explicit_turn_recovery_finds_a_failed_inactive_session() {
        let mut failed_turn = durable_turn();
        failed_turn["sessionId"] = json!("failed-session");
        failed_turn["status"] = json!("failed");
        failed_turn["outcome"] = json!("failed");
        failed_turn["error"] = json!("driver stopped");
        failed_turn["presentation"]["completeness"]["assistantOutput"] = json!("partial");
        let details = vec![
            (
                "ready-session".to_owned(),
                json!({ "turns": [{ "id": "another-turn" }] }),
            ),
            (
                "failed-session".to_owned(),
                json!({ "turns": [failed_turn] }),
            ),
        ];

        let recovered = select_agent_turn_across_sessions(&details, "agent-turn-1").unwrap();
        let answer = agent_answer_record(&recovered).unwrap();

        assert_eq!(answer["sessionId"], "failed-session");
        assert_eq!(answer["status"], "failed");
        assert_eq!(answer["error"], "driver stopped");
        assert_eq!(
            answer["evidence"]["completeness"]["assistantOutput"],
            "partial"
        );
    }

    #[test]
    fn explicit_turn_recovery_reports_ambiguity_in_session_order() {
        let details = vec![
            (
                "session-z".to_owned(),
                json!({ "turns": [{ "id": "shared-turn" }] }),
            ),
            (
                "session-a".to_owned(),
                json!({ "turns": [{ "id": "shared-turn" }] }),
            ),
        ];

        let error = select_agent_turn_across_sessions(&details, "shared-turn").unwrap_err();

        assert_eq!(
            error,
            "turn shared-turn is ambiguous across sessions: session-a, session-z"
        );
    }

    #[test]
    fn turn_inspection_prefers_active_session_over_replay() {
        let active = json!({
            "id": "active-session",
            "status": "ready",
            "active": true,
            "turnCount": 2
        });
        let replay = json!({
            "id": "interrupted-session",
            "status": "interrupted",
            "active": false,
            "turnCount": 3
        });
        let snapshot = json!({
            "activeAgentSession": active,
            "replayAgentSession": replay
        });

        let selected = agent_turn_session_from_snapshot(&snapshot).unwrap();

        assert_eq!(selected["id"], "active-session");
    }

    #[test]
    fn interrupted_workbench_session_remains_the_default_turn_replay() {
        let replay = json!({
            "id": "interrupted-session",
            "status": "interrupted",
            "active": false,
            "turnCount": 3
        });
        let snapshot = json!({
            "activeAgentSession": null,
            "replayAgentSession": replay
        });

        let selected = agent_turn_session_from_snapshot(&snapshot).unwrap();

        assert_eq!(selected["id"], "interrupted-session");
        assert_eq!(selected["status"], "interrupted");
        assert_eq!(selected["turnCount"], 3);
    }

    #[test]
    fn turn_inspection_has_no_default_without_active_or_replay_session() {
        let snapshot = json!({
            "activeAgentSession": null,
            "replayAgentSession": null
        });

        assert!(agent_turn_session_from_snapshot(&snapshot).is_none());
    }

    #[test]
    fn raw_and_stream_flags_are_rejected_before_starting_a_turn() {
        let mut host = NushellHost::new();
        let bridge = WorkbenchBridge::new(
            "http://127.0.0.1:9",
            "workspace-1".to_owned(),
            "token".to_owned(),
        )
        .unwrap();
        let mut working_set = StateWorkingSet::new(&host.engine_state);
        working_set.add_decl(Box::new(AgentCommand {
            bridge,
            session_ids: Arc::new(RwLock::new(Vec::new())),
            status: AgentStatusPresenter::terminal(),
        }));
        host.engine_state.merge_delta(working_set.render()).unwrap();

        let error = host
            .eval("agent 'prompt' --raw --stream")
            .unwrap_err()
            .to_string();

        assert!(error.contains("Conflicting agent projections"));
    }

    #[test]
    fn recovery_errors_name_the_durable_turn_command() {
        let error = format!(
            "{:?}",
            recoverable_agent_turn_error(
                "Agent turn cancelled",
                "cancel requested",
                "agent-session-1",
                "agent-turn-1",
                Span::unknown(),
            )
        );

        assert!(error.contains("agent-session-1"));
        assert!(error.contains("`agent turn agent-turn-1`"));
    }

    #[test]
    fn only_tagged_agent_answers_are_terminal_render_candidates() {
        let answer = json_to_nu(
            agent_answer_record(&durable_turn()).unwrap(),
            Span::unknown(),
        );
        let ordinary = json_to_nu(
            json!({ "type": "other", "response": "# raw" }),
            Span::unknown(),
        );
        let missing_response = json_to_nu(
            json!({ "type": AGENT_ANSWER_TYPE, "response": null }),
            Span::unknown(),
        );

        assert_eq!(
            agent_answer_response(&answer),
            Some("# Answer\n\nIt worked.")
        );
        assert_eq!(agent_answer_response(&ordinary), None);
        assert_eq!(agent_answer_response(&missing_response), None);
    }

    #[test]
    fn direct_answer_rendering_uses_safe_text_and_pty_width() {
        let host = NushellHost::new();
        let mut stack = Stack::new();
        let answer = json_to_nu(
            json!({
                "type": AGENT_ANSWER_TYPE,
                "response": "# Safe\u{1b}[31m\r\nnext"
            }),
            Span::unknown(),
        )
        .into_pipeline_data();
        let mut output = Vec::new();

        print_repl_output(
            answer,
            &host.engine_state,
            &mut stack,
            &TestMarkdownRenderer { fail: false },
            &mut output,
        )
        .unwrap();

        let output = String::from_utf8(output).unwrap();
        assert!(output.starts_with("rendered["));
        assert!(output.contains("# Safe\u{fffd}[31m\nnext"));
        assert!(output.ends_with('\n'));
        assert!(!output.contains('\u{1b}'));
    }

    #[test]
    fn renderer_failure_falls_back_to_safe_plain_markdown() {
        let host = NushellHost::new();
        let mut stack = Stack::new();
        let answer = json_to_nu(
            json!({
                "type": AGENT_ANSWER_TYPE,
                "response": "plain\u{1b}[2J"
            }),
            Span::unknown(),
        )
        .into_pipeline_data();
        let mut output = Vec::new();

        print_repl_output(
            answer,
            &host.engine_state,
            &mut stack,
            &TestMarkdownRenderer { fail: true },
            &mut output,
        )
        .unwrap();

        assert_eq!(String::from_utf8(output).unwrap(), "plain\u{fffd}[2J\n");
    }

    #[test]
    fn renderer_output_preserves_only_text_whitespace_and_sgr() {
        let host = NushellHost::new();
        let mut stack = Stack::new();
        let answer = json_to_nu(
            json!({
                "type": AGENT_ANSWER_TYPE,
                "response": "ignored"
            }),
            Span::unknown(),
        )
        .into_pipeline_data();
        let mut output = Vec::new();

        print_repl_output(
            answer,
            &host.engine_state,
            &mut stack,
            &TerminalControlRenderer,
            &mut output,
        )
        .unwrap();

        assert_eq!(
            String::from_utf8(output).unwrap(),
            "safe\u{1b}[38;2;143;181;115mgreen\u{1b}[0mdone\n\t\u{1b}[0m\n"
        );
    }

    #[test]
    fn piped_text_stream_preserves_exact_markdown_bytes() {
        let mut host = host_with_split_control_stream();

        let piped = host
            .eval("test control stream | into string")
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();

        assert_eq!(piped.as_bytes(), split_control_bytes());
    }

    #[test]
    fn direct_text_stream_filters_all_split_terminal_controls_including_sgr() {
        let host = NushellHost::new();
        let mut stack = Stack::new();
        let stream = ByteStream::from_iter(
            split_control_chunks(),
            Span::unknown(),
            Signals::empty(),
            ByteStreamType::String,
        )
        .into();
        let mut output = Vec::new();

        print_repl_output(
            stream,
            &host.engine_state,
            &mut stack,
            &TestMarkdownRenderer { fail: false },
            &mut output,
        )
        .unwrap();

        assert_eq!(
            String::from_utf8(output).unwrap(),
            "safegreenafter-oscafter-cursorunicode-é\n"
        );
    }

    #[test]
    fn direct_text_stream_cannot_leave_conceal_blink_or_inverse_active() {
        let host = NushellHost::new();
        let mut stack = Stack::new();
        let stream = ByteStream::from_iter(
            [
                b"visible\x1b[8".to_vec(),
                b"mconcealed\x1b[5mblink\x1b[".to_vec(),
                b"7minverse\x1b[38;2;143;181".to_vec(),
                b";115mcolor-without-reset\n".to_vec(),
            ],
            Span::unknown(),
            Signals::empty(),
            ByteStreamType::String,
        )
        .into();
        let mut output = Vec::new();

        print_repl_output(
            stream,
            &host.engine_state,
            &mut stack,
            &TestMarkdownRenderer { fail: false },
            &mut output,
        )
        .unwrap();

        assert_eq!(
            String::from_utf8(output.clone()).unwrap(),
            "visibleconcealedblinkinversecolor-without-reset\n"
        );
        assert!(
            !output.contains(&TERMINAL_ESCAPE),
            "untrusted text must emit no terminal escape bytes"
        );
        assert!(
            output.ends_with(b"\n"),
            "the following prompt must begin in the default terminal state"
        );
    }

    #[test]
    fn markdown_skin_uses_the_agent_lab_green_and_neutral_palette() {
        let skin = agent_lab_markdown_skin();

        assert_eq!(
            skin.paragraph.compound_style.get_fg(),
            Some(termimad::rgb(216, 224, 219))
        );
        assert_eq!(
            skin.headers[0].compound_style.get_fg(),
            Some(termimad::rgb(143, 181, 115))
        );
        assert_eq!(skin.bullet.get_fg(), Some(termimad::rgb(143, 181, 115)));
        assert_eq!(
            skin.inline_code.get_fg(),
            Some(termimad::rgb(186, 216, 170))
        );
        assert_eq!(
            skin.code_block.compound_style.get_bg(),
            Some(termimad::rgb(9, 16, 13))
        );
    }

    #[test]
    fn markdown_width_uses_the_pty_with_a_bounded_fallback() {
        assert_eq!(markdown_render_width(Some((120, 40))), 119);
        assert_eq!(markdown_render_width(Some((1, 1))), MARKDOWN_WIDTH_MINIMUM);
        assert_eq!(markdown_render_width(None), MARKDOWN_WIDTH_FALLBACK);
    }

    #[test]
    fn answer_stream_concatenates_deltas_and_adds_one_newline() {
        let stream = text_stream([
            AgentTurnOutput::AssistantDelta("hello ".to_owned()),
            AgentTurnOutput::AssistantDelta("world".to_owned()),
            AgentTurnOutput::AssistantCompleted("hello world".to_owned()),
            AgentTurnOutput::Finished {
                outcome: "completed".to_owned(),
            },
        ]);

        let chunks = stream.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(chunks.concat(), b"hello world\n");
    }

    #[test]
    fn answer_stream_does_not_duplicate_an_existing_newline() {
        let stream = text_stream([
            AgentTurnOutput::AssistantDelta("hello\n".to_owned()),
            AgentTurnOutput::AssistantCompleted("hello\n".to_owned()),
            AgentTurnOutput::Finished {
                outcome: "completed".to_owned(),
            },
        ]);

        let chunks = stream.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(chunks.concat(), b"hello\n");
    }

    #[test]
    fn answer_stream_uses_an_authoritative_completion_without_deltas() {
        let stream = text_stream([
            AgentTurnOutput::AssistantCompleted("complete answer".to_owned()),
            AgentTurnOutput::Finished {
                outcome: "completed".to_owned(),
            },
        ]);

        let chunks = stream.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(chunks.concat(), b"complete answer\n");
    }

    #[test]
    fn answer_stream_failure_keeps_recoverable_ids_after_partial_text() {
        let mut stream = text_stream([AgentTurnOutput::AssistantDelta("partial".to_owned())]);

        assert_eq!(stream.next().unwrap().unwrap(), b"partial");
        assert_eq!(stream.next().unwrap().unwrap(), b"\n");
        let error = format!("{:?}", stream.next().unwrap().unwrap_err());
        assert!(error.contains("agent-session-1"));
        assert!(error.contains("agent-turn-1"));
    }

    #[test]
    fn completed_turn_without_an_answer_is_a_recoverable_error() {
        let mut stream = text_stream([AgentTurnOutput::Finished {
            outcome: "completed".to_owned(),
        }]);

        let error = format!("{:?}", stream.next().unwrap().unwrap_err());
        assert!(error.contains("Agent returned no answer"));
        assert!(error.contains("agent-session-1"));
        assert!(error.contains("agent-turn-1"));
    }

    #[test]
    fn turn_selection_defaults_to_latest_and_preserves_presentation_fields() {
        let detail = json!({
            "turns": [
                { "id": "turn-1", "prompt": "first" },
                { "id": "turn-2", "presentation": { "answer": "second" } }
            ]
        });

        assert_eq!(select_agent_turn(&detail, None).unwrap()["id"], "turn-2");
        assert_eq!(
            select_agent_turn(&detail, Some("turn-2")).unwrap()["presentation"]["answer"],
            "second"
        );
        assert!(select_agent_turn(&detail, Some("missing")).is_err());
    }
}

impl Completer for AgentLabCompleter {
    #[allow(clippy::too_many_lines)]
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

        let session_command = ["agent switch ", "agent close "]
            .into_iter()
            .find(|command| prefix.starts_with(command));
        if let Some(session_command) = session_command {
            let session_prefix = &prefix[session_command.len()..];
            let replace_start = replace_start + session_command.len();
            return session_ids(&self.workbench_sessions)
                .into_iter()
                .filter(|session| {
                    session.starts_with(session_prefix) && session.as_str() != session_prefix
                })
                .map(|session| Suggestion {
                    value: session.clone(),
                    description: Some("Agent Lab session".to_owned()),
                    span: ReedlineSpan::new(replace_start, pos),
                    append_whitespace: true,
                    ..Suggestion::default()
                })
                .collect();
        }

        if let Some(arguments) = prefix.strip_prefix("agent new ") {
            let value_start = arguments.rfind(' ').map_or(0, |index| index + 1);
            let value_prefix = &arguments[value_start..];
            let replace_start = replace_start + "agent new ".len() + value_start;
            let prior = arguments[..value_start]
                .split_whitespace()
                .collect::<Vec<_>>();
            if value_prefix.starts_with('-') {
                return ["--harness", "--model"]
                    .into_iter()
                    .filter(|flag| flag.starts_with(value_prefix) && *flag != value_prefix)
                    .map(|flag| Suggestion {
                        value: flag.to_owned(),
                        description: Some("agent new option".to_owned()),
                        span: ReedlineSpan::new(replace_start, pos),
                        append_whitespace: true,
                        ..Suggestion::default()
                    })
                    .collect();
            }
            let is_model = prior.last() == Some(&"--model");
            let values = if is_model {
                &self.workbench_models
            } else if prior.last() == Some(&"--harness") {
                &self.workbench_harnesses
            } else {
                return Vec::new();
            };
            return values
                .iter()
                .filter(|value| value.starts_with(value_prefix) && value.as_str() != value_prefix)
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

        if let Some(arguments) = prefix.strip_prefix("agent ")
            && !["new", "sessions", "switch", "turn", "cancel", "close"]
                .contains(&arguments.split_whitespace().next().unwrap_or_default())
        {
            let value_start = arguments.rfind(' ').map_or(0, |index| index + 1);
            let value_prefix = &arguments[value_start..];
            if value_prefix.starts_with('-') {
                let replace_start = replace_start + "agent ".len() + value_start;
                return ["--stream", "--raw"]
                    .into_iter()
                    .filter(|flag| flag.starts_with(value_prefix) && *flag != value_prefix)
                    .map(|flag| Suggestion {
                        value: flag.to_owned(),
                        description: Some("agent output projection".to_owned()),
                        span: ReedlineSpan::new(replace_start, pos),
                        append_whitespace: true,
                        ..Suggestion::default()
                    })
                    .collect();
            }
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
