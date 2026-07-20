use std::{
    thread,
    time::{Duration, Instant},
};

use agent_lab_nushell_mcp::{HostError, McpBridge, NushellHost};
use nu_protocol::Value;

fn fixture_bridge() -> McpBridge {
    McpBridge::connect(
        env!("CARGO_BIN_EXE_agent-lab-mcp-fixture"),
        std::iter::empty::<String>(),
    )
    .expect("fixture should connect")
}

#[test]
fn persistent_nushell_and_mcp_session_preserve_structured_behavior() {
    let bridge = fixture_bridge();
    let runtime_id = bridge.runtime_id();
    let mut host = NushellHost::new();
    host.attach("fixture", bridge.clone())
        .expect("fixture should attach");
    assert!(matches!(
        host.attach("fixture", bridge.clone()),
        Err(HostError::NamespaceAlreadyAttached(_))
    ));

    host.eval("mut answer = 41")
        .expect("state declaration should evaluate");
    assert_eq!(host.eval("$answer += 1; $answer").unwrap().as_int(), Ok(42));

    let first_pid = host
        .eval("fixture session | get pid")
        .unwrap()
        .as_int()
        .unwrap();
    assert_eq!(
        host.eval("fixture increment | get count").unwrap().as_int(),
        Ok(1)
    );
    assert_eq!(
        host.eval("fixture increment | get count").unwrap().as_int(),
        Ok(2)
    );
    assert_eq!(
        host.eval("fixture session | get pid").unwrap().as_int(),
        Ok(first_pid)
    );
    assert_eq!(bridge.runtime_id(), runtime_id);

    let active_names = host
        .eval("fixture catalog | get items | where active | get name")
        .expect("structured pipeline should evaluate");
    assert_eq!(strings(active_names), vec!["alpha", "gamma"]);
    assert_eq!(
        host.eval(r#"{ probe: "pipeline" } | fixture catalog | get request.probe"#)
            .expect("pipeline input should become MCP arguments")
            .as_str(),
        Ok("pipeline")
    );

    let discovered = host
        .eval("mcp fixture tools | where name == catalog | get name.0")
        .expect("discovery should be structured");
    assert_eq!(discovered.as_str(), Ok("catalog"));
    let discovered_from_namespace = host
        .eval("mcp fixture | where name == catalog | get name.0")
        .expect("the namespace root should make discovery forgiving");
    assert_eq!(discovered_from_namespace.as_str(), Ok("catalog"));

    let unknown = host
        .eval("mcp fix")
        .expect_err("an incomplete namespace should remain a REPL error");
    assert!(
        unknown.to_string().contains("Unknown Agent Lab command"),
        "unknown commands should not expose the missing external runner: {unknown}"
    );
    assert!(!unknown.to_string().contains("run-external"));
}

#[test]
fn lifecycle_and_discovery_changes_remain_observable() {
    let bridge = fixture_bridge();
    let mut host = NushellHost::new();
    host.attach("fixture", bridge.clone())
        .expect("fixture should attach");

    assert_eq!(
        host.eval("fixture lifecycle | get complete")
            .unwrap()
            .as_bool(),
        Ok(true)
    );
    let events = bridge.events();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "mcp.progress")
            .count(),
        2
    );
    assert!(events.iter().any(|event| event.kind == "mcp.log"));

    host.eval("fixture enable_extra")
        .expect("fixture should change its tool list");
    wait_for_stale_discovery(&bridge);
    host.eval("mcp fixture tools")
        .expect("catalog inspection should succeed");
    assert!(
        bridge.discovery_is_stale(),
        "catalog inspection must not mark host commands refreshed"
    );
    host.refresh("fixture")
        .expect("refresh should merge commands");
    assert!(!bridge.discovery_is_stale());
    assert_eq!(
        host.eval("fixture extra | get available")
            .unwrap()
            .as_bool(),
        Ok(true)
    );
    let initial_help = host
        .eval(r#"help "fixture extra""#)
        .expect("dynamic command help should be visible");
    assert!(
        initial_help
            .as_str()
            .unwrap()
            .starts_with("A tool added during the live session")
    );

    host.eval("fixture revise_extra")
        .expect("fixture should revise a tool descriptor");
    wait_for_stale_discovery(&bridge);
    host.refresh("fixture")
        .expect("refresh should replace revised commands");
    let revised_help = host
        .eval(r#"help "fixture extra""#)
        .expect("revised dynamic command help should be visible");
    assert!(
        revised_help
            .as_str()
            .unwrap()
            .starts_with("A revised tool in the live session")
    );

    host.eval("fixture disable_extra")
        .expect("fixture should remove a tool from its tool list");
    wait_for_stale_discovery(&bridge);
    host.refresh("fixture")
        .expect("refresh should hide removed commands");
    let removed_tool_error = host
        .eval("fixture extra")
        .expect_err("removed command should no longer evaluate");
    assert!(
        removed_tool_error
            .to_string()
            .contains("Unknown Agent Lab command"),
        "unexpected removed-command error: {removed_tool_error:?}"
    );
    let discovered_extra = host
        .eval("mcp fixture tools | where name == extra | length")
        .expect("discovery should reflect removal");
    assert_eq!(discovered_extra.as_int(), Ok(0));

    let tool_error = bridge.call_tool("fail", serde_json::Map::new()).unwrap();
    assert_eq!(tool_error.is_error, Some(true));
    let protocol_error = bridge
        .call_tool("protocol_fail", serde_json::Map::new())
        .unwrap_err();
    assert!(protocol_error.to_string().contains("MCP request failed"));
    let events = bridge.events();
    assert!(
        events.iter().any(|event| {
            event.kind == "mcp.tool.completed" && event.payload["isError"] == true
        })
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == "mcp.tool.protocol_failed")
    );
}

#[test]
fn synchronous_bridge_calls_are_safe_inside_a_tokio_context() {
    let bridge = fixture_bridge();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime should build");

    runtime.block_on(async {
        assert!(!bridge.list_tools().expect("tools should list").is_empty());
        assert!(
            bridge
                .call_tool("session", serde_json::Map::new())
                .expect("tool should run")
                .structured_content
                .is_some()
        );
    });
}

#[test]
fn stale_namespaces_refresh_in_stable_visible_order() {
    let alpha = fixture_bridge();
    let zeta = fixture_bridge();
    let mut host = NushellHost::new();
    host.attach("zeta", zeta.clone())
        .expect("zeta fixture should attach");
    host.attach("alpha", alpha.clone())
        .expect("alpha fixture should attach");

    host.eval("zeta enable_extra")
        .expect("zeta fixture should change its tool list");
    host.eval("alpha enable_extra")
        .expect("alpha fixture should change its tool list");
    wait_for_stale_discovery(&alpha);
    wait_for_stale_discovery(&zeta);

    assert_eq!(
        host.refresh_stale()
            .expect("both namespaces should refresh"),
        ["alpha", "zeta"]
    );
}

fn wait_for_stale_discovery(bridge: &McpBridge) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if bridge.discovery_is_stale() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "tool-list notification should mark discovery stale"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn strings(value: Value) -> Vec<String> {
    value
        .into_list()
        .expect("value should be a list")
        .into_iter()
        .map(|value| value.as_str().expect("item should be a string").to_owned())
        .collect()
}
