use std::{
    fs,
    path::PathBuf,
    process::Command as ProcessCommand,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use agent_lab_driver_protocol::{
    CanonicalizationPolicy, CommandBody, ControllerCommand, DriverBody, DriverEvidenceBundle,
    DriverFailureScope, DriverLaunch, DriverMessage, DriverProcess, MAX_DRIVER_RECORD_BYTES,
    MAX_DRIVER_STDERR_BYTES, MAX_DRIVER_TRANSCRIPT_BYTES, PROTOCOL_VERSION, ProcessError,
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
fn clean_exit_closes_stdin_for_eof_driven_drivers() {
    let mut launch = DriverLaunch::new(env!("CARGO_BIN_EXE_agent-lab-driver-fixture"));
    launch.env.push((
        "AGENT_LAB_FIXTURE_WAIT_FOR_STDIN_EOF_AFTER_CLOSE".into(),
        "1".into(),
    ));
    let mut driver = DriverProcess::spawn_with(launch).unwrap();
    open_session(&mut driver);
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
    assert!(matches!(
        driver.send(&command(
            "after-close",
            CommandBody::CloseSession {
                session_id: "session-1".to_owned(),
            },
        )),
        Err(ProcessError::Write(ref message)) if message == "driver stdin is closed"
    ));
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
fn oversized_driver_records_are_bounded_before_buffering() {
    let mut launch = DriverLaunch::new(env!("CARGO_BIN_EXE_agent-lab-driver-fixture"));
    launch
        .env
        .push(("AGENT_LAB_FIXTURE_OVERSIZED_STDOUT".into(), "1".into()));
    let mut process = DriverProcess::spawn_with(launch).unwrap();

    assert!(matches!(
        process.receive(TIMEOUT),
        Err(ProcessError::OutputLimitExceeded { limit }) if limit == MAX_DRIVER_RECORD_BYTES
    ));
}

#[test]
fn oversized_driver_stderr_is_bounded_before_buffering() {
    let mut launch = DriverLaunch::new(env!("CARGO_BIN_EXE_agent-lab-driver-fixture"));
    launch
        .env
        .push(("AGENT_LAB_FIXTURE_OVERSIZED_STDERR".into(), "1".into()));
    let mut process = DriverProcess::spawn_with(launch).unwrap();

    assert!(matches!(
        process.receive(TIMEOUT),
        Err(ProcessError::StderrLimitExceeded { limit }) if limit == MAX_DRIVER_STDERR_BYTES
    ));
    assert_eq!(process.stderr().len(), MAX_DRIVER_STDERR_BYTES);
}

#[test]
fn total_driver_transcript_retention_is_bounded() {
    let mut launch = DriverLaunch::new(env!("CARGO_BIN_EXE_agent-lab-driver-fixture"));
    launch
        .env
        .push(("AGENT_LAB_FIXTURE_LARGE_TRANSCRIPT".into(), "1".into()));
    let mut process = DriverProcess::spawn_with(launch).unwrap();

    loop {
        match process.receive(TIMEOUT) {
            Ok(_) => {}
            Err(ProcessError::TranscriptLimitExceeded { limit }) => {
                assert_eq!(limit, MAX_DRIVER_TRANSCRIPT_BYTES);
                break;
            }
            Err(error) => panic!("unexpected process error: {error}"),
        }
    }
    let retained = process
        .transcript()
        .driver_records
        .iter()
        .map(Vec::len)
        .sum::<usize>();
    assert!(retained <= MAX_DRIVER_TRANSCRIPT_BYTES);
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

    let mut wrong_controller_version = bundle.transcript.clone();
    let mut controller: ControllerCommand =
        serde_json::from_slice(&wrong_controller_version.controller_records[0]).unwrap();
    controller.protocol_version += 1;
    wrong_controller_version.controller_records[0] = controller_record(&controller);
    assert!(
        DriverEvidenceBundle::new(
            bundle.controller_revision.clone(),
            bundle.driver.clone(),
            bundle.process_id,
            wrong_controller_version,
            bundle.canonical.policy.clone(),
        )
        .is_err()
    );
}

#[test]
fn evidence_rejects_a_session_closed_with_an_unfinished_turn() {
    let bundle = completed_fixture_bundle();
    let mut unfinished = bundle.transcript.clone();
    unfinished.controller_records.insert(
        1,
        controller_record(&command(
            "unfinished-turn",
            CommandBody::StartTurn {
                session_id: "evidence-session".to_owned(),
                turn_id: "unfinished-turn".to_owned(),
                task: json!({}),
                capability_sources: json!([]),
            },
        )),
    );
    let mut closed: DriverMessage =
        serde_json::from_slice(unfinished.driver_records.last().unwrap()).unwrap();
    closed.sequence = 4;
    unfinished.driver_records[2] = driver_record(&closed);
    unfinished.driver_records.insert(
        2,
        driver_record(&DriverMessage {
            protocol_version: PROTOCOL_VERSION,
            sequence: 3,
            caused_by: Some("unfinished-turn".to_owned()),
            body: DriverBody::TurnEvent {
                session_id: "evidence-session".to_owned(),
                turn_id: "unfinished-turn".to_owned(),
                event_type: "fixture.waiting".to_owned(),
                payload: json!({}),
            },
        }),
    );
    assert!(
        DriverEvidenceBundle::new(
            bundle.controller_revision.clone(),
            bundle.driver.clone(),
            bundle.process_id,
            unfinished,
            bundle.canonical.policy.clone(),
        )
        .is_err()
    );
}

#[test]
fn evidence_rejects_post_terminal_turn_and_session_activity() {
    let bundle = completed_fixture_bundle_with_turn();

    let mut post_finish = bundle.transcript.clone();
    let mut closed: DriverMessage =
        serde_json::from_slice(post_finish.driver_records.last().unwrap()).unwrap();
    closed.sequence += 1;
    *post_finish.driver_records.last_mut().unwrap() = driver_record(&closed);
    post_finish.driver_records.insert(
        post_finish.driver_records.len() - 1,
        driver_record(&DriverMessage {
            protocol_version: PROTOCOL_VERSION,
            sequence: closed.sequence - 1,
            caused_by: Some("turn-evidence".to_owned()),
            body: DriverBody::TurnEvent {
                session_id: "evidence-session".to_owned(),
                turn_id: "turn-evidence".to_owned(),
                event_type: "too-late".to_owned(),
                payload: json!({}),
            },
        }),
    );
    assert!(
        DriverEvidenceBundle::new(
            bundle.controller_revision.clone(),
            bundle.driver.clone(),
            bundle.process_id,
            post_finish,
            bundle.canonical.policy.clone(),
        )
        .is_err()
    );

    let mut abort_after_close = bundle.transcript.clone();
    abort_after_close
        .controller_records
        .push(controller_record(&command(
            "abort-after-close",
            CommandBody::AbortTurn {
                session_id: "evidence-session".to_owned(),
                turn_id: "turn-evidence".to_owned(),
                reason: None,
            },
        )));
    assert!(
        DriverEvidenceBundle::new(
            bundle.controller_revision.clone(),
            bundle.driver.clone(),
            bundle.process_id,
            abort_after_close,
            bundle.canonical.policy.clone(),
        )
        .is_err()
    );
}

#[test]
fn evidence_rejects_invalid_failure_and_causal_identities() {
    let bundle = completed_fixture_bundle_with_turn();

    let mut invalid_failure = bundle.transcript.clone();
    let mut closed: DriverMessage =
        serde_json::from_slice(invalid_failure.driver_records.last().unwrap()).unwrap();
    closed.sequence += 1;
    *invalid_failure.driver_records.last_mut().unwrap() = driver_record(&closed);
    invalid_failure.driver_records.insert(
        invalid_failure.driver_records.len() - 1,
        driver_record(&DriverMessage {
            protocol_version: PROTOCOL_VERSION,
            sequence: closed.sequence - 1,
            caused_by: Some("turn-evidence".to_owned()),
            body: DriverBody::Failed {
                scope: DriverFailureScope::Turn,
                session_id: Some("bogus-session".to_owned()),
                turn_id: Some("bogus-turn".to_owned()),
                code: "bogus".to_owned(),
                message: "bogus identity".to_owned(),
            },
        }),
    );
    assert!(
        DriverEvidenceBundle::new(
            bundle.controller_revision.clone(),
            bundle.driver.clone(),
            bundle.process_id,
            invalid_failure,
            bundle.canonical.policy.clone(),
        )
        .is_err()
    );

    let mut unknown_cause = bundle.transcript.clone();
    let mut opened: DriverMessage =
        serde_json::from_slice(&unknown_cause.driver_records[1]).unwrap();
    opened.caused_by = Some("unknown-command".to_owned());
    unknown_cause.driver_records[1] = driver_record(&opened);
    assert!(
        DriverEvidenceBundle::new(
            bundle.controller_revision.clone(),
            bundle.driver.clone(),
            bundle.process_id,
            unknown_cause,
            bundle.canonical.policy.clone(),
        )
        .is_err()
    );

    let mut duplicate_cause = bundle.transcript.clone();
    let mut close: ControllerCommand =
        serde_json::from_slice(duplicate_cause.controller_records.last().unwrap()).unwrap();
    close.message_id = "open-evidence".to_owned();
    *duplicate_cause.controller_records.last_mut().unwrap() = controller_record(&close);
    assert!(
        DriverEvidenceBundle::new(
            bundle.controller_revision.clone(),
            bundle.driver.clone(),
            bundle.process_id,
            duplicate_cause,
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
fn probe_rejects_completion_for_an_unexpected_turn() {
    let root = temporary_root("probe-wrong-turn");
    let evidence = root.join("run-1");
    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-lab-driver-probe"))
        .arg(env!("CARGO_BIN_EXE_agent-lab-driver-fixture"))
        .env("AGENT_LAB_EVIDENCE_DIR", &evidence)
        .env(
            "AGENT_LAB_DRIVER_TASK_JSON",
            r#"{"mode":"wrong-turn-finished"}"#,
        )
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected session/turn"));
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

#[test]
fn clean_exit_drains_queued_stdout_before_transcript_capture() {
    let mut launch = DriverLaunch::new(env!("CARGO_BIN_EXE_agent-lab-driver-fixture"));
    launch.env.push((
        "AGENT_LAB_FIXTURE_TRAILING_STDOUT_COUNT".into(),
        "64".into(),
    ));
    let mut process = DriverProcess::spawn_with(launch).unwrap();
    open_session(&mut process);
    process
        .send(&command(
            "close-stdout",
            CommandBody::CloseSession {
                session_id: "session-1".to_owned(),
            },
        ))
        .unwrap();
    assert!(matches!(
        process.receive(TIMEOUT).unwrap().parsed.body,
        DriverBody::SessionClosed { .. }
    ));
    assert!(matches!(
        process.wait_for_exit(TIMEOUT),
        Err(ProcessError::UnexpectedOutputAfterClose { .. })
    ));

    let transcript = process.transcript();
    assert_eq!(transcript.driver_records.len(), 67);
    let trailing: DriverMessage =
        serde_json::from_slice(transcript.driver_records.last().unwrap()).unwrap();
    assert!(matches!(
        trailing.body,
        DriverBody::TurnEvent { ref event_type, .. } if event_type == "fixture.trailing-stdout"
    ));
}

#[test]
fn clean_exit_validates_malformed_queued_stdout() {
    let mut launch = DriverLaunch::new(env!("CARGO_BIN_EXE_agent-lab-driver-fixture"));
    launch.env.push((
        "AGENT_LAB_FIXTURE_TRAILING_MALFORMED_STDOUT".into(),
        "1".into(),
    ));
    let mut process = DriverProcess::spawn_with(launch).unwrap();
    open_session(&mut process);
    process
        .send(&command(
            "close-malformed",
            CommandBody::CloseSession {
                session_id: "session-1".to_owned(),
            },
        ))
        .unwrap();
    assert!(matches!(
        process.receive(TIMEOUT).unwrap().parsed.body,
        DriverBody::SessionClosed { .. }
    ));
    assert!(matches!(
        process.wait_for_exit(TIMEOUT),
        Err(ProcessError::MalformedOutput { ref raw, .. }) if raw == b"{not-json}\n"
    ));
    assert_eq!(
        process.transcript().driver_records.last().unwrap(),
        b"{not-json}\n"
    );
}

#[cfg(unix)]
#[test]
fn clean_exit_terminates_descendants_that_hold_reader_pipes() {
    let mut process = DriverProcess::spawn("sh", ["-c", "sleep 30 & exit 0"]).unwrap();
    let timeout = Duration::from_millis(250);
    let started = Instant::now();

    assert_eq!(process.wait_for_exit(timeout).unwrap(), Some(0));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "reader cleanup blocked for {:?}",
        started.elapsed()
    );
}

#[cfg(unix)]
#[test]
fn receive_reports_a_crashed_driver_even_when_a_descendant_holds_stdout() {
    let mut process = DriverProcess::spawn("sh", ["-c", "sleep 30 & exit 17"]).unwrap();
    let timeout = Duration::from_millis(250);
    let started = Instant::now();

    assert!(matches!(
        process.receive(timeout),
        Err(ProcessError::UnexpectedExit { code: Some(17) })
    ));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "crash detection blocked for {:?}",
        started.elapsed()
    );
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

fn completed_fixture_bundle_with_turn() -> DriverEvidenceBundle {
    let bundle = completed_fixture_bundle();
    let mut transcript = bundle.transcript.clone();
    transcript.controller_records.insert(
        1,
        controller_record(&command(
            "turn-evidence",
            CommandBody::StartTurn {
                session_id: "evidence-session".to_owned(),
                turn_id: "turn-evidence".to_owned(),
                task: json!({}),
                capability_sources: json!([]),
            },
        )),
    );
    let mut closed: DriverMessage =
        serde_json::from_slice(transcript.driver_records.last().unwrap()).unwrap();
    closed.sequence = 5;
    *transcript.driver_records.last_mut().unwrap() = driver_record(&closed);
    transcript.driver_records.insert(
        2,
        driver_record(&DriverMessage {
            protocol_version: PROTOCOL_VERSION,
            sequence: 3,
            caused_by: Some("turn-evidence".to_owned()),
            body: DriverBody::TurnEvent {
                session_id: "evidence-session".to_owned(),
                turn_id: "turn-evidence".to_owned(),
                event_type: "fixture.started".to_owned(),
                payload: json!({}),
            },
        }),
    );
    transcript.driver_records.insert(
        3,
        driver_record(&DriverMessage {
            protocol_version: PROTOCOL_VERSION,
            sequence: 4,
            caused_by: Some("turn-evidence".to_owned()),
            body: DriverBody::TurnFinished {
                session_id: "evidence-session".to_owned(),
                turn_id: "turn-evidence".to_owned(),
                outcome: "completed".to_owned(),
                evidence: json!({}),
            },
        }),
    );
    DriverEvidenceBundle::new(
        bundle.controller_revision.clone(),
        bundle.driver.clone(),
        bundle.process_id,
        transcript,
        bundle.canonical.policy.clone(),
    )
    .unwrap()
}

fn driver_record(message: &DriverMessage) -> Vec<u8> {
    let mut raw = serde_json::to_vec(message).unwrap();
    raw.push(b'\n');
    raw
}

fn controller_record(command: &ControllerCommand) -> Vec<u8> {
    let mut raw = serde_json::to_vec(command).unwrap();
    raw.push(b'\n');
    raw
}
