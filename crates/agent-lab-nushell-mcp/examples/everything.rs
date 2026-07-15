use agent_lab_nushell_mcp::{McpBridge, NushellHost};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bridge = McpBridge::connect(
        "npx",
        [
            "-y",
            "@modelcontextprotocol/server-everything@2026.7.4",
            "stdio",
        ],
    )?;
    let tools = bridge.list_tools()?;
    assert!(
        tools
            .iter()
            .any(|tool| tool.name == "get-structured-content"),
        "Everything Server should expose its structured-content tool"
    );

    let mut host = NushellHost::new();
    host.attach("everything", bridge.clone())?;
    let result = host.eval(
        "tool everything get-structured-content { location: 'New York' } \
         | select temperature conditions humidity",
    )?;

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "runtimeId": bridge.runtime_id(),
            "toolCount": tools.len(),
            "resultType": result.get_type().to_string(),
            "result": format!("{result:?}"),
            "eventKinds": bridge
                .events()
                .into_iter()
                .map(|event| event.kind)
                .collect::<Vec<_>>(),
        }))?
    );
    Ok(())
}
