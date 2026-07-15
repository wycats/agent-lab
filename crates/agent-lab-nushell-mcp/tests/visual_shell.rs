use std::{process::Command, time::Duration};

use expectrl::{Eof, Expect, Session};

#[test]
fn pty_session_preserves_visible_nushell_and_mcp_behavior() {
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-lab-nushell-mcp-shell"));
    command.arg("--fixture").env("NO_COLOR", "1");
    let mut shell = Session::spawn(command).expect("visual shell should start in a PTY");
    shell.set_expect_timeout(Some(Duration::from_secs(10)));

    shell
        .expect("Agent Lab visual shell")
        .expect("banner should be visible");
    shell
        .expect("MCP namespaces: fixture")
        .expect("attached source should be visible");
    shell
        .expect("agent-lab> ")
        .expect("prompt should be visible");

    shell
        .send_line("mut session_value = 41")
        .expect("state declaration should be submitted");
    shell
        .expect("agent-lab> ")
        .expect("state declaration should return to the prompt");
    shell
        .send_line("$session_value += 1; $session_value")
        .expect("state read should be submitted");
    shell
        .expect("42")
        .expect("state should persist between lines");
    shell.expect("agent-lab> ").expect("prompt should return");

    shell
        .send_line("tool fixture catalog { probe: 'visual' } | get items | where active | get name")
        .expect("structured pipeline should be submitted");
    shell
        .expect("╭")
        .expect("native table border should be visible");
    shell
        .expect("alpha")
        .expect("native table should contain alpha");
    shell
        .expect("gamma")
        .expect("native table should contain gamma");
    shell.expect("agent-lab> ").expect("prompt should return");

    shell
        .send_line("help tool fixture catalog")
        .expect("help should be submitted");
    shell
        .expect("Return a nested structured catalog")
        .expect("dynamic command help should be visible");
    shell.expect("agent-lab> ").expect("prompt should return");

    shell
        .send_line("tool fixture enable_extra {}")
        .expect("catalog mutation should be submitted");
    shell
        .expect("[capabilities refreshed: fixture]")
        .expect("catalog refresh should be visible between lines");
    shell.expect("agent-lab> ").expect("prompt should return");
    shell
        .send_line("tool fixture extra {}")
        .expect("new command should be submitted");
    shell
        .expect("available")
        .expect("new command output should be visible");
    shell.expect("agent-lab> ").expect("prompt should return");

    shell
        .send_line("tool fixture fail {}")
        .expect("tool error should be submitted");
    shell
        .expect("MCP tool failed")
        .expect("tool failure should retain its visible classification");
    shell
        .expect("intentional tool failure")
        .expect("tool failure detail should be visible");
    shell.expect("agent-lab> ").expect("prompt should recover");

    shell.send_line("exit").expect("exit should be submitted");
    shell.expect(Eof).expect("shell should exit cleanly");

    #[cfg(unix)]
    {
        use expectrl::process::unix::WaitStatus;

        let status = shell
            .get_process()
            .wait()
            .expect("shell exit status should be available");
        assert!(
            matches!(status, WaitStatus::Exited(_, 0)),
            "shell should exit with status 0, got {status:?}"
        );
    }
}
