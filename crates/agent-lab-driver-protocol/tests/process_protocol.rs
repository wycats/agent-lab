use std::{
    fs,
    path::PathBuf,
    process::Command as ProcessCommand,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use agent_lab_driver_protocol::{
    CanonicalizationPolicy, CommandBody, ControllerCommand, DriverBody, DriverEvidenceBundle,
    DriverFailureScope, DriverLaunch, DriverMessage, DriverProcess, PROTOCOL_VERSION, ProcessError,
};
use serde_json::json;

const TIMEOUT: Duration = Duration::from_secs(2);

fn fixture() -> DriverProcess {
    DriverProcess::spawn(
        env!("CARGO_BIN_EXE_agent-lab-driver-fixture"),
        std::iter::empty::<String>(),
    )
    .expect("fixture driver should spawn")
}

fn command(message_id: &str, body: CommandBody) -> ControllerCommand {
    ControllerCommand {
        protocol_version: PROTOCOL_VERSION,
        message_id: message_id.to_owned(),
        body,
    }
}

fn open_session(driver: &mut DriverProcess) -> u32 {
    let ready = driver.receive(TIMEOUT).expect("driver should become ready");
    assert!(ready.raw.ends_with(b"\n"));
    assert!(matches!(ready.parsed.body, DriverBody::Ready { .. }));
    driver
        .send(&command(
            "open-1",
            CommandBody::OpenSession {
                session_id: "session-1".to_owned(),
                config: json!({ "driver": "fixture" }),
                limits: json!({ "turns": 2 }),
            },
        ))
        .unwrap();
    let opened = driver.receive(TIMEOUT).unwrap();
    let DriverBody::SessionOpened {
        session_id,
        process_id,
    } = opened.parsed.body
    else {
        panic!("expected session.opened")
    };
    assert_eq!(session_id, "session-1");
    assert_eq!(opened.parsed.caused_by.as_deref(), Some("open-1"));
    process_id
}

#[test]
fn one_process_streams_two_turns_and_cancels_the_second() {
    let mut driver = fixture();
    let process_id = driver.process_id();
    assert_eq!(open_session(&mut driver), process_id);

    driver
        .send(&command(
            "turn-1",
            CommandBody::StartTurn {
                session_id: "session-1".to_owned(),
                turn_id: "turn-1".to_owned(),
                task: json!({ "prompt": "inspect" }),
                capability_sources: json!([{ "id": "fixture-mcp" }]),
            },
        ))
        .unwrap();
    let started = driver.receive(TIMEOUT).unwrap();
    assert!(matches!(
        started.parsed.body,
        DriverBody::TurnEvent { ref event_type, .. } if event_type == "fixture.started"
    ));
    let capabilities = driver.receive(TIMEOUT).unwrap();
    assert!(matches!(
        capabilities.parsed.body,
        DriverBody::TurnEvent { ref event_type, .. } if event_type == "fixture.capabilities"
    ));
    let finished = driver.receive(TIMEOUT).unwrap();
    assert!(matches!(
        finished.parsed.body,
        DriverBody::TurnFinished { ref outcome, .. } if outcome == "completed"
    ));

    driver
        .send(&command(
            "turn-2",
            CommandBody::StartTurn {
                session_id: "session-1".to_owned(),
                turn_id: "turn-2".to_owned(),
                task: json!({ "mode": "wait-for-abort" }),
                capability_sources: json!([]),
            },
        ))
        .unwrap();
    let waiting = driver.receive(TIMEOUT).unwrap();
    assert!(matches!(
        waiting.parsed.body,
        DriverBody::TurnEvent { ref event_type, .. } if event_type == "fixture.waiting"
    ));
    driver
        .send(&command(
            "abort-2",
            CommandBody::AbortTurn {
                session_id: "session-1".to_owned(),
                turn_id: "turn-2".to_owned(),
                reason: Some("test cancellation".to_owned()),
            },
        ))
        .unwrap();
    let aborted = driver.receive(TIMEOUT).unwrap();
    assert!(matches!(
        aborted.parsed.body,
        DriverBody::TurnEvent { ref event_type, .. } if event_type == "fixture.aborted"
    ));
    let finished = driver.receive(TIMEOUT).unwrap();
    assert!(matches!(
        finished.parsed.body,
        DriverBody::TurnFinished { ref outcome, .. } if outcome == "aborted"
    ));

    driver
        .send(&command(
            "close-1",
            CommandBody::CloseSession {
                session_id: "session-1".to_owned(),
            },
        ))
        .unwrap();
    assert!(matches!(
        driver.receive(TIMEOUT).unwrap().parsed.body,
        DriverBody::SessionClosed { .. }
    ));
    assert_eq!(driver.wait_for_exit(TIMEOUT).unwrap(), Some(0));
    assert_eq!(driver.sent_records().len(), 5);
    let transcript = driver.transcript();
    assert_eq!(transcript.controller_records.len(), 5);
    assert_eq!(transcript.driver_records.len(), 9);
    assert!(
        transcript
            .driver_records
            .iter()
            .all(|raw| raw.ends_with(b"\n"))
    );
}

#[test]
fn malformed_output_reported_failure_and_process_exit_are_distinct() {
    let mut malformed = fixture();
    open_session(&mut malformed);
    malformed
        .send(&command(
            "malformed",
            CommandBody::StartTurn {
                session_id: "session-1".to_owned(),
                turn_id: "malformed".to_owned(),
                task: json!({ "mode": "malformed-output" }),
                capability_sources: json!([]),
            },
        ))
        .unwrap();
    let malformed_error = malformed.receive(TIMEOUT).unwrap_err();
    assert!(matches!(
        malformed_error,
        ProcessError::MalformedOutput { ref raw, .. } if raw == b"{not-json}\n"
    ));
    assert_eq!(
        malformed.transcript().driver_records.last().unwrap(),
        b"{not-json}\n"
    );

    let mut failed = fixture();
    open_session(&mut failed);
    failed
        .send(&command(
            "failed",
            CommandBody::StartTurn {
                session_id: "session-1".to_owned(),
                turn_id: "failed".to_owned(),
                task: json!({ "mode": "fail" }),
                capability_sources: json!([]),
            },
        ))
        .unwrap();
    assert!(matches!(
        failed.receive(TIMEOUT).unwrap().parsed.body,
        DriverBody::Failed {
            scope: DriverFailureScope::Turn,
            ref code,
            ..
        } if code == "fixture-failure"
    ));

    let mut exited = fixture();
    open_session(&mut exited);
    exited
        .send(&command(
            "exit",
            CommandBody::StartTurn {
                session_id: "session-1".to_owned(),
                turn_id: "exit".to_owned(),
                task: json!({ "mode": "exit" }),
                capability_sources: json!([]),
            },
        ))
        .unwrap();
    assert!(matches!(
        exited.receive(TIMEOUT),
        Err(ProcessError::UnexpectedExit { code: Some(17) })
    ));
}

#[test]
fn protocol_version_and_sequence_violations_are_distinct() {
    let mut version = fixture();
    open_session(&mut version);
    version
        .send(&command(
            "version",
            CommandBody::StartTurn {
                session_id: "session-1".to_owned(),
                turn_id: "version".to_owned(),
                task: json!({ "mode": "unsupported-version" }),
                capability_sources: json!([]),
            },
        ))
        .unwrap();
    assert!(matches!(
        version.receive(TIMEOUT),
        Err(ProcessError::UnsupportedVersion {
            expected: PROTOCOL_VERSION,
            actual
        }) if actual == PROTOCOL_VERSION + 1
    ));

    let mut sequence = fixture();
    open_session(&mut sequence);
    sequence
        .send(&command(
            "sequence",
            CommandBody::StartTurn {
                session_id: "session-1".to_owned(),
                turn_id: "sequence".to_owned(),
                task: json!({ "mode": "repeat-sequence" }),
                capability_sources: json!([]),
            },
        ))
        .unwrap();
    assert!(matches!(
        sequence.receive(TIMEOUT),
        Err(ProcessError::UnexpectedSequence {
            expected: 3,
            actual: 2
        })
    ));
}

#[cfg(unix)]
#[test]
fn unterminated_driver_records_are_rejected_before_parsing() {
    let mut process = DriverProcess::spawn(
        "sh",
        [
            "-c",
            "printf '%s' '{\"protocolVersion\":1,\"sequence\":1,\"causedBy\":null,\"type\":\"driver.ready\",\"driver\":{\"name\":\"partial\",\"version\":\"1\",\"revision\":null,\"features\":[]}}'",
        ],
    )
    .unwrap();

    assert!(matches!(
        process.receive(TIMEOUT),
        Err(ProcessError::UnterminatedOutput { ref raw }) if !raw.ends_with(b"\n")
    ));
}

#[test]
fn raw_runs_remain_distinct_while_named_canonical_evidence_matches() {
    let first = completed_fixture_bundle();
    let second = completed_fixture_bundle();

    assert_ne!(
        first.transcript.driver_records,
        second.transcript.driver_records
    );
    assert_eq!(first.canonical, second.canonical);
    assert_eq!(
        first.canonical.policy.removed_object_keys,
        ["processId".to_owned()].into_iter().collect()
    );
}

#[test]
fn durable_evidence_reopens_and_rejects_tampering() {
    let bundle = completed_fixture_bundle();
    let root = temporary_root("durable-evidence");
    let evidence = root.join("run-1");

    bundle.write_to_dir(&evidence).unwrap();
    assert_eq!(
        fs::read(evidence.join("controller.jsonl")).unwrap(),
        bundle
            .transcript
            .controller_records
            .iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>()
    );
    assert_eq!(
        fs::read(evidence.join("driver.jsonl")).unwrap(),
        bundle
            .transcript
            .driver_records
            .iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>()
    );
    assert_eq!(
        DriverEvidenceBundle::read_from_dir(&evidence).unwrap(),
        bundle
    );
    assert!(bundle.write_to_dir(&evidence).is_err());

    fs::write(
        evidence.join("canonical.json"),
        br#"{"policy":{"name":"fixture-v1","removedObjectKeys":[]},"driverRecords":[]}"#,
    )
    .unwrap();
    assert!(DriverEvidenceBundle::read_from_dir(&evidence).is_err());

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn evidence_rejects_protocol_and_manifest_identity_mismatches() {
    let bundle = completed_fixture_bundle();

    let mut wrong_version = bundle.transcript.clone();
    let mut message: DriverMessage =
        serde_json::from_slice(&wrong_version.driver_records[1]).unwrap();
    message.protocol_version += 1;
    wrong_version.driver_records[1] = driver_record(&message);
    assert!(
        DriverEvidenceBundle::new(
            bundle.controller_revision.clone(),
            bundle.driver.clone(),
            bundle.process_id,
            wrong_version,
            bundle.canonical.policy.clone(),
        )
        .is_err()
    );

    let mut wrong_sequence = bundle.transcript.clone();
    let mut message: DriverMessage =
        serde_json::from_slice(&wrong_sequence.driver_records[1]).unwrap();
    message.sequence += 1;
    wrong_sequence.driver_records[1] = driver_record(&message);
    assert!(
        DriverEvidenceBundle::new(
            bundle.controller_revision.clone(),
            bundle.driver.clone(),
            bundle.process_id,
            wrong_sequence,
            bundle.canonical.policy.clone(),
        )
        .is_err()
    );

    let mut stale_driver = bundle.driver.clone();
    stale_driver.revision = Some("stale".to_owned());
    assert!(
        DriverEvidenceBundle::new(
            bundle.controller_revision.clone(),
            stale_driver,
            bundle.process_id,
            bundle.transcript.clone(),
            bundle.canonical.policy.clone(),
        )
        .is_err()
    );

    assert!(
        DriverEvidenceBundle::new(
            bundle.controller_revision.clone(),
            bundle.driver.clone(),
            bundle.process_id + 1,
            bundle.transcript.clone(),
            bundle.canonical.policy.clone(),
        )
        .is_err()
    );
}

#[test]
fn probe_can_finalize_fixture_evidence_for_direct_inspection() {
    let root = temporary_root("probe-evidence");
    let evidence = root.join("run-1");
    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-lab-driver-probe"))
        .arg(env!("CARGO_BIN_EXE_agent-lab-driver-fixture"))
        .env("AGENT_LAB_EVIDENCE_DIR", &evidence)
        .env("AGENT_LAB_CONTROLLER_REVISION", "test-controller")
        .env(
            "AGENT_LAB_CANONICAL_POLICY_JSON",
            r#"{"name":"fixture-v1","removedObjectKeys":["processId"]}"#,
        )
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(summary["outcome"], "completed");
    assert_eq!(summary["evidenceDir"], evidence.to_string_lossy().as_ref());
    let bundle = DriverEvidenceBundle::read_from_dir(&evidence).unwrap();
    assert_eq!(
        bundle.controller_revision.as_deref(),
        Some("test-controller")
    );
    assert_eq!(bundle.canonical.policy.name, "fixture-v1");

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn probe_rejects_a_nonzero_driver_exit_before_finalizing_evidence() {
    let root = temporary_root("probe-nonzero-exit");
    let evidence = root.join("run-1");
    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-lab-driver-probe"))
        .arg(env!("CARGO_BIN_EXE_agent-lab-driver-fixture"))
        .env("AGENT_LAB_EVIDENCE_DIR", &evidence)
        .env("AGENT_LAB_FIXTURE_EXIT_CODE_AFTER_CLOSE", "23")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("driver exited unsuccessfully"));
    assert!(!evidence.exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn clean_exit_drains_trailing_stderr_before_transcript_capture() {
    let mut launch = DriverLaunch::new(env!("CARGO_BIN_EXE_agent-lab-driver-fixture"));
    launch
        .env
        .push(("AGENT_LAB_FIXTURE_TRAILING_STDERR".into(), "1".into()));
    let mut process = DriverProcess::spawn_with(launch).unwrap();
    open_session(&mut process);
    process
        .send(&command(
            "close-stderr",
            CommandBody::CloseSession {
                session_id: "session-1".to_owned(),
            },
        ))
        .unwrap();
    assert!(matches!(
        process.receive(TIMEOUT).unwrap().parsed.body,
        DriverBody::SessionClosed { .. }
    ));
    assert_eq!(process.wait_for_exit(TIMEOUT).unwrap(), Some(0));
    assert_eq!(process.stderr(), b"fixture trailing stderr\n");
}

#[cfg(unix)]
#[test]
fn eof_from_a_running_driver_respects_the_receive_timeout() {
    let mut process = DriverProcess::spawn("sh", ["-c", "exec 1>&-; sleep 30"]).unwrap();
    let timeout = Duration::from_millis(50);
    let started = Instant::now();

    assert!(matches!(
        process.receive(timeout),
        Err(ProcessError::Timeout)
    ));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "receive blocked for {:?}",
        started.elapsed()
    );
}

#[cfg(unix)]
#[test]
fn dropping_a_driver_terminates_its_process_group() {
    let root = temporary_root("process-group");
    let pid_file = root.join("grandchild.pid");
    let script = format!(
        "sleep 30 & child=$!; printf '%s' \"$child\" > '{}'; wait",
        pid_file.display()
    );
    let process = DriverProcess::spawn("sh", ["-c", &script]).unwrap();
    wait_for_file(&pid_file);
    let grandchild = fs::read_to_string(&pid_file).unwrap();
    assert!(process_exists(&grandchild));

    drop(process);

    let deadline = Instant::now() + Duration::from_secs(2);
    while process_exists(&grandchild) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !process_exists(&grandchild),
        "grandchild {grandchild} survived"
    );
    fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
fn wait_for_file(path: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(path.exists(), "{} was not created", path.display());
}

#[cfg(unix)]
fn process_exists(pid: &str) -> bool {
    ProcessCommand::new("kill")
        .args(["-0", pid])
        .output()
        .is_ok_and(|output| output.status.success())
}

fn temporary_root(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "agent-lab-driver-{label}-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir(&root).unwrap();
    root
}

fn completed_fixture_bundle() -> DriverEvidenceBundle {
    let mut process = fixture();
    let ready = process.receive(TIMEOUT).unwrap();
    let DriverBody::Ready { driver } = ready.parsed.body else {
        panic!("expected driver.ready")
    };
    process
        .send(&command(
            "open-evidence",
            CommandBody::OpenSession {
                session_id: "evidence-session".to_owned(),
                config: json!({}),
                limits: json!({}),
            },
        ))
        .unwrap();
    let opened = process.receive(TIMEOUT).unwrap();
    assert!(matches!(
        opened.parsed.body,
        DriverBody::SessionOpened { .. }
    ));
    process
        .send(&command(
            "close-evidence",
            CommandBody::CloseSession {
                session_id: "evidence-session".to_owned(),
            },
        ))
        .unwrap();
    assert!(matches!(
        process.receive(TIMEOUT).unwrap().parsed.body,
        DriverBody::SessionClosed { .. }
    ));
    assert_eq!(process.wait_for_exit(TIMEOUT).unwrap(), Some(0));

    DriverEvidenceBundle::new(
        Some("test-controller".to_owned()),
        driver,
        process.process_id(),
        process.transcript(),
        CanonicalizationPolicy::new("fixture-v1", ["processId"]),
    )
    .unwrap()
}

fn driver_record(message: &DriverMessage) -> Vec<u8> {
    let mut raw = serde_json::to_vec(message).unwrap();
    raw.push(b'\n');
    raw
}
