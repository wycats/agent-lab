use agent_lab_driver_protocol::{
    ASSISTANT_COMPLETED_EVENT, ASSISTANT_DELTA_EVENT, AssistantCompletedObservation,
    AssistantDeltaObservation, DriverBody, NATIVE_ACTION_EVENT, NativeActionObservation,
    NativeActionStatus, PROGRESS_EVENT, ProgressObservation, ProgressPhase, TurnObservation,
    USAGE_EVENT, UsageObservation,
};
use serde_json::json;

#[test]
fn portable_observations_round_trip_through_opaque_turn_events() {
    let observations = [
        TurnObservation::AssistantDelta(AssistantDeltaObservation {
            message_id: "message-1".to_owned(),
            text: "Alpha and gamma ".to_owned(),
        }),
        TurnObservation::AssistantCompleted(AssistantCompletedObservation {
            message_id: "message-1".to_owned(),
            text: "Alpha and gamma are active.".to_owned(),
        }),
        TurnObservation::NativeAction(NativeActionObservation {
            action_id: "action-1".to_owned(),
            name: "write_file".to_owned(),
            status: NativeActionStatus::Completed,
            summary: Some("Wrote result.json".to_owned()),
        }),
        TurnObservation::Progress(ProgressObservation {
            phase: ProgressPhase::Reasoning,
            detail: Some("Inspecting the catalog".to_owned()),
            source: Some("fixture".to_owned()),
        }),
        TurnObservation::Usage(UsageObservation {
            input_tokens: Some(10),
            output_tokens: Some(20),
            total_tokens: Some(30),
            cache_read_input_tokens: Some(4),
            cache_creation_input_tokens: None,
        }),
    ];

    for observation in observations {
        let expected = observation.clone();
        let DriverBody::TurnEvent {
            session_id,
            turn_id,
            event_type,
            payload,
        } = observation.into_driver_body("session-1", "turn-1")
        else {
            panic!("portable observation should encode as turn.event")
        };
        assert_eq!(session_id, "session-1");
        assert_eq!(turn_id, "turn-1");
        assert_eq!(
            TurnObservation::parse(&event_type, &payload).unwrap(),
            Some(expected)
        );
    }
}

#[test]
fn completed_text_is_authoritative_and_unknown_events_stay_opaque() {
    let completed = TurnObservation::parse(
        ASSISTANT_COMPLETED_EVENT,
        &json!({ "messageId": "message-1", "text": "authoritative full text" }),
    )
    .unwrap();
    assert_eq!(
        completed,
        Some(TurnObservation::AssistantCompleted(
            AssistantCompletedObservation {
                message_id: "message-1".to_owned(),
                text: "authoritative full text".to_owned(),
            }
        ))
    );
    assert_eq!(
        TurnObservation::parse("v0.native-event", &json!({ "anything": true })).unwrap(),
        None
    );
}

#[test]
fn recognized_observations_reject_ambiguous_or_empty_payloads() {
    for (event_type, payload) in [
        (
            ASSISTANT_DELTA_EVENT,
            json!({ "messageId": "", "text": "delta" }),
        ),
        (
            ASSISTANT_COMPLETED_EVENT,
            json!({ "messageId": "message-1", "text": " " }),
        ),
        (
            NATIVE_ACTION_EVENT,
            json!({
                "actionId": "action-1",
                "name": "write_file",
                "status": "completed",
                "unexpected": true
            }),
        ),
        (
            PROGRESS_EVENT,
            json!({
                "phase": "reasoning",
                "detail": " ",
                "source": "fixture"
            }),
        ),
        (
            PROGRESS_EVENT,
            json!({
                "phase": "thinking",
                "detail": "Inspecting the catalog"
            }),
        ),
        (
            PROGRESS_EVENT,
            json!({
                "phase": "reasoning",
                "source": ""
            }),
        ),
        (USAGE_EVENT, json!({})),
    ] {
        assert!(TurnObservation::parse(event_type, &payload).is_err());
    }
}
