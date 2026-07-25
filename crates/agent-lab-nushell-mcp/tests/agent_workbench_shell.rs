#![cfg(unix)]

use std::{
    collections::BTreeMap,
    fs,
    net::Ipv4Addr,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use agent_lab_driver_protocol::DriverLaunch;
use agent_lab_web::{
    AgentSessionStatus, AgentTurnStatus, FixtureSessionProvider, HarnessProfile, PrepareRunRequest,
    RunController, RunControllerConfig, ServerConfig, SessionProvider, app_with_runs,
};
use expectrl::{ControlCode, Eof, Expect, Session};

const PROMPT: &str = "agent-lab> ";
const ANSWER_HEADING: &str = "Fixture answer";

#[test]
#[allow(clippy::too_many_lines)]
fn agent_commands_cross_the_real_pty_http_and_sse_boundary() {
    let root = temporary_root("agent-shell");
    let scenarios = root.join("scenarios");
    let data = root.join("runs");
    fs::create_dir(&scenarios).expect("scenario directory should be created");
    fs::create_dir(&data).expect("run directory should be created");
    write_scenario(&scenarios);

    let harness = HarnessProfile {
        id: "fixture".to_owned(),
        display_name: "Fixture".to_owned(),
        launch: interactive_fixture_launch(&root.join("driver-start-count")),
        models: BTreeMap::from([("test".to_owned(), "fixture/test".to_owned())]),
    };
    let controller = RunController::new_with_harnesses(
        RunControllerConfig {
            scenarios_dir: scenarios,
            data_dir: data,
            driver: DriverLaunch::new("/bin/false"),
        },
        vec![harness],
        BTreeMap::from([("test".to_owned(), "Test".to_owned())]),
    )
    .expect("controller should start");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("test runtime should build");
    let explore = runtime
        .block_on(controller.prepare(PrepareRunRequest {
            scenario_id: "catalog".to_owned(),
        }))
        .expect("Explore workspace should prepare");

    let listener = runtime
        .block_on(tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)))
        .expect("test HTTP listener should bind");
    let origin = format!(
        "http://{}",
        listener
            .local_addr()
            .expect("listener address should resolve")
    );
    let shell_path = PathBuf::from(env!("CARGO_BIN_EXE_agent-lab-nushell-mcp-shell"));
    let config = ServerConfig::new(root.join("assets"), origin.clone());
    let provider: Arc<dyn SessionProvider> =
        Arc::new(FixtureSessionProvider::new(&shell_path, &root));
    let app = app_with_runs(config.clone(), provider, Some(controller.clone()));
    let server = runtime.spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test HTTP server should stay healthy");
    });

    let binding = controller
        .terminal_binding(&explore.id)
        .expect("prepared workspace should issue a scoped shell grant");
    let mut command = Command::new(shell_path);
    for (index, source) in binding.sources.iter().enumerate() {
        let token_env = format!("AGENT_LAB_TEST_MCP_TOKEN_{index}");
        command.args([
            "--mcp-http",
            source.id.as_str(),
            source.url.as_str(),
            token_env.as_str(),
        ]);
        command.env(token_env, &source.token);
    }
    command.args([
        "--workbench",
        origin.as_str(),
        explore.id.as_str(),
        "AGENT_LAB_TEST_WORKBENCH_TOKEN",
    ]);
    command
        .env("AGENT_LAB_TEST_WORKBENCH_TOKEN", &binding.control_token)
        .env("AGENT_LAB_PLAIN_REPL", "1")
        .env("NO_COLOR", "1")
        .env("TERM", "xterm-256color")
        .current_dir(&binding.workspace);

    let mut shell = Session::spawn(command).expect("workbench shell should start in a PTY");
    shell.set_expect_timeout(Some(Duration::from_secs(20)));
    shell
        .expect("MCP namespaces: analysis, catalog")
        .expect("the real HTTP MCP sources should attach");
    shell.expect(PROMPT).expect("shell prompt should open");

    let conflict = command_output(&mut shell, "agent 'invalid' --raw --stream");
    assert!(conflict.contains("Conflicting agent projections"));
    assert!(!conflict.contains("Agent:"));
    run_command(&mut shell, "agent sessions | length", &["0"]);

    for (prompt, startup_detail) in [
        ("cancel before driver ready", "Waiting before driver.ready"),
        (
            "cancel before session opened",
            "Waiting before session.opened",
        ),
    ] {
        shell
            .send_line(format!("agent '{prompt}'"))
            .expect("cold-starting agent command should be submitted");
        let session_id = wait_for_startup_phase(
            &controller,
            &explore.id,
            startup_detail,
            Duration::from_secs(5),
        );
        let interrupted_at = Instant::now();
        shell
            .send(ControlCode::ETX)
            .expect("Ctrl-C should reach cold session startup");
        let cancelled = shell
            .expect(PROMPT)
            .expect("the prompt should recover from cold-start cancellation");
        assert!(
            interrupted_at.elapsed() < Duration::from_secs(2),
            "cold-start Ctrl-C should recover promptly"
        );
        let cancelled = String::from_utf8_lossy(cancelled.before());
        assert!(
            cancelled.contains("Agent session start cancelled"),
            "cold-start cancellation should be explicit: {cancelled:?}"
        );
        assert_status_cleared_before(&cancelled, "Agent session start cancelled");
        wait_for_closed_session(
            &controller,
            &explore.id,
            &session_id,
            Duration::from_secs(5),
            true,
        );
    }
    run_command(&mut shell, "agent sessions | length", &["2"]);

    let rendered = command_output(&mut shell, "agent 'render the fixture answer'");
    for needle in [ANSWER_HEADING, "Gamma", "leads the catalog.", "Agent:"] {
        assert!(
            rendered.contains(needle),
            "default agent answer should show `{needle}`: {rendered:?}"
        );
    }
    assert_status_cleared_before(&rendered, ANSWER_HEADING);
    run_command(&mut shell, "agent turn | get type", &["agent-answer"]);
    run_command(
        &mut shell,
        "agent turn | get response",
        &["# Fixture answer", "**Gamma** leads the catalog."],
    );
    run_command(
        &mut shell,
        "agent 'return a structured answer' | get type",
        &["agent-answer"],
    );
    run_command(
        &mut shell,
        "agent turn | get response | from md | get content | first",
        &[ANSWER_HEADING],
    );
    let inspect_session = command_output(&mut shell, "agent | get status");
    assert!(inspect_session.contains("ready"));
    assert!(!inspect_session.contains("Agent:"));

    let stream = command_output(&mut shell, "agent 'stream the fixture answer' --stream");
    for needle in ["# Fixture answer", "**Gamma** leads the catalog."] {
        assert!(
            stream.contains(needle),
            "stream should show `{needle}`: {stream:?}"
        );
    }
    assert!(!stream.contains("Agent:"));

    let raw = command_output(
        &mut shell,
        "agent 'show attributable events' --raw | get kind",
    );
    for needle in [
        "observation.assistant.delta",
        "observation.assistant.completed",
        "observation.native-action",
        "observation.usage",
        "agent.turn.finished",
    ] {
        assert!(
            raw.contains(needle),
            "raw output should show `{needle}`: {raw:?}"
        );
    }
    assert!(!raw.contains("Agent:"));

    shell
        .send_line("agent 'wait-for-abort'")
        .expect("long-running turn should be submitted");
    shell
        .expect("Agent:")
        .expect("the transient agent status should render while the turn is running");
    let (session_id, turn_id) =
        wait_for_running_turn(&controller, &explore.id, Duration::from_secs(5));
    shell
        .send(ControlCode::ETX)
        .expect("Ctrl-C should reach the running Nushell command");
    let cancelled = shell
        .expect(PROMPT)
        .expect("the prompt should recover after Ctrl-C");
    let cancelled = String::from_utf8_lossy(cancelled.before());
    assert!(cancelled.contains("Agent turn cancelled"));
    assert_status_cleared_before(&cancelled, "Agent turn cancelled");
    wait_for_cancelled_turn(
        &controller,
        &explore.id,
        &session_id,
        &turn_id,
        Duration::from_secs(5),
    );
    run_command(&mut shell, "agent turn | get status", &["cancelled"]);
    run_command(
        &mut shell,
        "agent turn | get evidence.completeness.assistantOutput",
        &["partial"],
    );

    let explicit_new = command_output(&mut shell, "agent new | get id");
    for needle in [
        "Agent: starting",
        "Preparing explicit agent new",
        "agent-session-",
    ] {
        assert!(
            explicit_new.contains(needle),
            "`agent new` should show `{needle}` while preserving structured output: {explicit_new:?}"
        );
    }
    assert_status_cleared_before(&explicit_new, "agent-session-");

    let active_session_id = controller
        .list_agent_sessions(&explore.id)
        .into_iter()
        .find(|session| session.active)
        .expect("the completed fixture session should remain active")
        .id;
    run_command(&mut shell, "agent close", &["closing"]);
    wait_for_closed_session(
        &controller,
        &explore.id,
        &active_session_id,
        Duration::from_secs(5),
        false,
    );

    shell.send_line("exit").expect("exit should be submitted");
    shell.expect(Eof).expect("shell should exit cleanly");

    config.shutdown();
    server.abort();
    drop(controller);
    drop(runtime);
    fs::remove_dir_all(root).expect("test root should be removable");
}

fn run_command(shell: &mut expectrl::session::OsSession, command: &str, expected: &[&str]) {
    let output = command_output(shell, command);
    for needle in expected {
        assert!(
            output.contains(needle),
            "command `{command}` should show `{needle}`: {output:?}"
        );
    }
}

fn command_output(shell: &mut expectrl::session::OsSession, command: &str) -> String {
    shell
        .send_line(command)
        .unwrap_or_else(|error| panic!("command should be submitted ({command}): {error}"));
    let capture = shell
        .expect(PROMPT)
        .unwrap_or_else(|error| panic!("command `{command}` should return to the prompt: {error}"));
    String::from_utf8_lossy(capture.before()).into_owned()
}

fn assert_status_cleared_before(output: &str, durable_output: &str) {
    let clear = output
        .rfind("\u{1b}[2K")
        .unwrap_or_else(|| panic!("transient status should be cleared: {output:?}"));
    let durable = output
        .find(durable_output)
        .unwrap_or_else(|| panic!("durable output should contain `{durable_output}`: {output:?}"));
    assert!(
        clear < durable,
        "transient status should clear before `{durable_output}`: {output:?}"
    );
}

fn wait_for_running_turn(
    controller: &RunController,
    workspace_id: &str,
    timeout: Duration,
) -> (String, String) {
    let deadline = Instant::now() + timeout;
    loop {
        for session in controller.list_agent_sessions(workspace_id) {
            let detail = controller
                .agent_session(workspace_id, &session.id)
                .expect("listed session should remain readable");
            if let Some(turn) = detail
                .turns
                .iter()
                .find(|turn| turn.status == AgentTurnStatus::Running)
            {
                return (session.id, turn.id.clone());
            }
        }
        assert!(
            Instant::now() < deadline,
            "agent turn should become running before cancellation"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_startup_phase(
    controller: &RunController,
    workspace_id: &str,
    detail: &str,
    timeout: Duration,
) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        for session in controller.list_agent_sessions(workspace_id).iter().rev() {
            let session_detail = controller
                .agent_session(workspace_id, &session.id)
                .expect("listed session should remain readable");
            if session.status == AgentSessionStatus::Starting
                && session_detail.events.iter().any(|event| {
                    event.kind == "startup.event"
                        && event
                            .payload
                            .get("detail")
                            .and_then(serde_json::Value::as_str)
                            == Some(detail)
                })
            {
                return session.id.clone();
            }
        }
        assert!(
            Instant::now() < deadline,
            "agent session should reach startup phase `{detail}` before cancellation"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_closed_session(
    controller: &RunController,
    workspace_id: &str,
    session_id: &str,
    timeout: Duration,
    expect_no_turns: bool,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let detail = controller
            .agent_session(workspace_id, session_id)
            .expect("cancelled cold-start session should remain durable");
        let close_event_count = detail
            .events
            .iter()
            .filter(|event| event.kind == "agent.session.closed")
            .count();
        assert!(
            close_event_count <= 1,
            "cold-start cancellation must record at most one terminal close event"
        );
        if detail.summary.status == AgentSessionStatus::Closed && close_event_count == 1 {
            assert!(!detail.summary.active);
            if expect_no_turns {
                assert!(
                    detail.turns.is_empty(),
                    "cold-start cancellation must not create a turn"
                );
            }
            return;
        }
        assert!(
            Instant::now() < deadline,
            "cancelled cold-start session should reach durable closed state"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_cancelled_turn(
    controller: &RunController,
    workspace_id: &str,
    session_id: &str,
    turn_id: &str,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let detail = controller
            .agent_session(workspace_id, session_id)
            .expect("durable session should remain readable");
        if detail
            .turns
            .iter()
            .any(|turn| turn.id == turn_id && turn.status == AgentTurnStatus::Cancelled)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "cancelled turn should reach durable terminal state"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn interactive_fixture_launch(startup_counter: &Path) -> DriverLaunch {
    let script = r#"
counter=${AGENT_LAB_TEST_STARTUP_COUNTER:?}
launch_number=$(cat "$counter" 2>/dev/null || printf '0')
launch_number=$((launch_number + 1))
printf '%s' "$launch_number" > "$counter"
sequence=1
if [ "$launch_number" -eq 1 ]; then
  printf '%s\n' '{"protocolVersion":1,"sequence":1,"causedBy":null,"type":"startup.event","phase":"driver-ready","status":"started","detail":"Waiting before driver.ready"}'
  while :; do sleep 1; done
fi
if [ "$launch_number" -eq 4 ]; then
  printf '%s\n' '{"protocolVersion":1,"sequence":1,"causedBy":null,"type":"startup.event","phase":"explicit-new","status":"started","detail":"Preparing explicit agent new"}'
  sleep 1
fi
printf '%s\n' '{"protocolVersion":1,"sequence":1,"causedBy":null,"type":"driver.ready","driver":{"name":"interactive-shell-fixture","version":"1","revision":null,"features":["streaming","turn-observations-v1"]}}'
while IFS= read -r line; do
  session=$(printf '%s' "$line" | sed -E 's/.*"sessionId":"([^"]+)".*/\1/')
  case "$line" in
    *'"type":"session.open"'*)
      sequence=$((sequence + 1))
      if [ "$launch_number" -eq 2 ]; then
        printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"startup.event","phase":"session-open","status":"started","detail":"Waiting before session.opened"}\n' "$sequence"
        while :; do sleep 1; done
      fi
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"session.opened","sessionId":"%s","processId":4242}\n' "$sequence" "$session"
      ;;
    *'"type":"turn.start"'*)
      turn=$(printf '%s' "$line" | sed -E 's/.*"turnId":"([^"]+)".*/\1/')
      first='# Fixture answer\n\n**Gamma** leads the catalog.\n\n'
      second='| Item | Score |\n| --- | ---: |\n| `gamma` | **8** |\n| `alpha` | 3 |'
      complete='# Fixture answer\n\n**Gamma** leads the catalog.\n\n| Item | Score |\n| --- | ---: |\n| `gamma` | **8** |\n| `alpha` | 3 |'
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"turn.event","sessionId":"%s","turnId":"%s","eventType":"observation.assistant.delta","payload":{"messageId":"message-%s","text":"%s"}}\n' "$sequence" "$session" "$turn" "$turn" "$first"
      case "$line" in *wait-for-abort*) continue ;; esac
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"turn.event","sessionId":"%s","turnId":"%s","eventType":"observation.assistant.delta","payload":{"messageId":"message-%s","text":"%s"}}\n' "$sequence" "$session" "$turn" "$turn" "$second"
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"turn.event","sessionId":"%s","turnId":"%s","eventType":"observation.assistant.completed","payload":{"messageId":"message-%s","text":"%s"}}\n' "$sequence" "$session" "$turn" "$turn" "$complete"
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"turn.event","sessionId":"%s","turnId":"%s","eventType":"observation.native-action","payload":{"actionId":"inspect-%s","name":"Inspect catalog","status":"completed","summary":"Compared active item scores."}}\n' "$sequence" "$session" "$turn" "$turn"
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"turn.event","sessionId":"%s","turnId":"%s","eventType":"observation.usage","payload":{"inputTokens":7,"outputTokens":21,"totalTokens":28}}\n' "$sequence" "$session" "$turn"
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"turn.finished","sessionId":"%s","turnId":"%s","outcome":"completed","evidence":{"fixture":true}}\n' "$sequence" "$session" "$turn"
      ;;
    *'"type":"turn.abort"'*)
      turn=$(printf '%s' "$line" | sed -E 's/.*"turnId":"([^"]+)".*/\1/')
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"turn.finished","sessionId":"%s","turnId":"%s","outcome":"aborted","evidence":{"fixture":true}}\n' "$sequence" "$session" "$turn"
      ;;
    *'"type":"session.close"'*)
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"session.closed","sessionId":"%s"}\n' "$sequence" "$session"
      exit 0
      ;;
  esac
done
"#;
    let mut launch = DriverLaunch::new("/bin/sh");
    launch.args = vec!["-c".into(), script.into()];
    launch.env.push((
        "AGENT_LAB_TEST_STARTUP_COUNTER".into(),
        startup_counter.as_os_str().to_owned(),
    ));
    launch
}

fn write_scenario(root: &Path) {
    fs::create_dir_all(root.join("catalog/workspace")).expect("seed workspace should be created");
    fs::write(root.join("catalog/workspace/README.md"), "seed\n")
        .expect("seed file should be written");
    fs::write(
        root.join("catalog.toml"),
        r#"
version = 1
id = "catalog"
title = "Catalog"
description = "test"
question = "How does the harness produce the expected catalog artifact?"
seed = "catalog/workspace"
prompt = "write output"
output = "result.json"

[limits]
maxDurationMs = 1000
maxCommandCount = 1
maxOrchestratorInvocations = 1
maxToolInvocations = 1

[assertions]
activeNames = ["alpha", "gamma"]
totalScore = 11
"#,
    )
    .expect("scenario manifest should be written");
}

fn temporary_root(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the Unix epoch")
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("agent-lab-{label}-{}-{nonce}", std::process::id()));
    fs::create_dir(&root).expect("temporary root should be created");
    root
}
