#![allow(clippy::wildcard_imports)]

use std::collections::BTreeSet;

use super::*;

const PROMOTION_SCHEMA_VERSION: u32 = 1;
const CATALOG_EVALUATOR_ID: &str = "catalog-to-file";
const CATALOG_EVALUATOR_VERSION: u32 = 1;
const DEFINITION_PUBLICATION_TRANSACTION: &str = "publication.pending.json";
pub(super) const MAX_EVALUATION_DRAFT_REVISIONS: usize = 32;
const MANUAL_AUTHORING_BLOCKER: &str =
    "review and confirm the suggested task, assertions, and measurements";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EvaluationExecutionStatus {
    Queued,
    Running,
    Complete,
    Inconclusive,
    Cancelled,
    Intervened,
}

impl EvaluationExecutionStatus {
    fn is_finished(self) -> bool {
        matches!(
            self,
            Self::Complete | Self::Inconclusive | Self::Cancelled | Self::Intervened
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ValidationAssertionStatus {
    Passed,
    Failed,
    NotEvaluated,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CatalogEvaluatorParameters {
    pub active_names: Vec<String>,
    pub total_score: i64,
    pub required_capability_sources: Vec<String>,
    pub output_path: PathBuf,
    pub require_schema: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationEvaluator {
    pub id: String,
    pub version: u32,
    pub parameters: CatalogEvaluatorParameters,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationSourceProvenance {
    pub workspace_id: String,
    pub session_id: String,
    pub turn_ids: Vec<String>,
    pub source_revision: String,
    #[serde(default)]
    pub source_digest: String,
    pub capability_revisions: BTreeMap<String, String>,
    pub source_event_sequences: Vec<u64>,
    pub scenario_id: String,
    pub harness_id: String,
    pub model_profile_id: String,
    pub model_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationRevision {
    pub schema_version: u32,
    pub id: String,
    pub draft_id: String,
    pub previous_revision_id: Option<String>,
    pub created_at_ms: u128,
    pub task: String,
    pub source: EvaluationSourceProvenance,
    #[serde(default)]
    pub capability_recipe: Vec<CapabilityAssembly>,
    pub limits: ScenarioLimits,
    pub evaluator: EvaluationEvaluator,
    pub measurements: Vec<String>,
    pub blocking_issues: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationRevisionUpdate {
    #[serde(default)]
    pub task: Option<String>,
    #[serde(default)]
    pub limits: Option<ScenarioLimits>,
    #[serde(default)]
    pub evaluator: Option<EvaluationEvaluator>,
    #[serde(default)]
    pub measurements: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationDraftSummary {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub current_revision_id: String,
    pub status: String,
    pub saved: bool,
    pub definition_id: Option<String>,
    #[serde(default)]
    pub promoted_revision_id: Option<String>,
    pub created_at_ms: u128,
    pub updated_at_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationValidationAttempt {
    pub id: String,
    pub draft_id: String,
    pub revision_id: String,
    pub execution_status: EvaluationExecutionStatus,
    pub assertion_status: ValidationAssertionStatus,
    pub harness_id: String,
    pub model_profile_id: String,
    pub run_id: Option<String>,
    pub started_at_ms: u128,
    pub finished_at_ms: Option<u128>,
    pub error: Option<String>,
    pub score: Option<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationDraftDetail {
    pub summary: EvaluationDraftSummary,
    pub revisions: Vec<EvaluationRevision>,
    pub validations: Vec<EvaluationValidationAttempt>,
    #[serde(default)]
    pub events: Vec<RunEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationDefinitionSummary {
    pub id: String,
    pub name: String,
    pub draft_id: String,
    pub revision_id: String,
    pub created_at_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationDefinitionDetail {
    pub summary: EvaluationDefinitionSummary,
    pub revision: EvaluationRevision,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateEvaluationDraftRequest {
    #[serde(default)]
    pub session_id: Option<String>,
    pub from_turn_id: String,
    pub through_turn_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateEvaluationDraftRequest {
    pub base_revision_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub revision: EvaluationRevisionUpdate,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct StartDefinitionEvaluationRequest {
    #[serde(default)]
    pub harness_ids: Option<Vec<String>>,
    #[serde(default)]
    pub model_profile_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SaveEvaluationDraftRequest {
    #[serde(default)]
    pub revision_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}

pub(super) struct PromotionStore {
    root: PathBuf,
    drafts: Mutex<HashMap<String, Arc<PromotionDraftState>>>,
    definitions: Mutex<HashMap<String, Arc<PromotionDefinitionState>>>,
    secret_values: Mutex<Vec<Vec<u8>>>,
    evidence_lifecycle: Mutex<()>,
    #[cfg(test)]
    fail_next_validation_assembly_persist: AtomicBool,
    #[cfg(test)]
    validation_before_start_hook: Mutex<Option<ValidationBeforeStartHook>>,
}

#[cfg(test)]
struct ValidationBeforeStartHook {
    reached: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
}

struct PromotionDraftState {
    detail: Mutex<EvaluationDraftDetail>,
    anchor: Arc<AgentSessionDirectoryAnchor>,
    sender: broadcast::Sender<RunEvent>,
    event_commit: Mutex<()>,
    validation_cancels: Mutex<HashMap<String, CancellationToken>>,
    evidence_quarantined: AtomicBool,
}

struct PromotionDefinitionState {
    detail: EvaluationDefinitionDetail,
    anchor: Arc<AgentSessionDirectoryAnchor>,
}

impl PromotionStore {
    pub(super) fn load(data_dir: &Path) -> Result<Self, RunError> {
        let root = data_dir.join("evaluation-library");
        fs::create_dir_all(root.join("drafts"))?;
        fs::create_dir_all(root.join("definitions"))?;
        let root = fs::canonicalize(root)?;
        let drafts = load_drafts(&root.join("drafts"))?;
        let definitions = load_definitions(&root.join("definitions"), &drafts)?;
        Ok(Self {
            root,
            drafts: Mutex::new(drafts),
            definitions: Mutex::new(definitions),
            secret_values: Mutex::new(Vec::new()),
            evidence_lifecycle: Mutex::new(()),
            #[cfg(test)]
            fail_next_validation_assembly_persist: AtomicBool::new(false),
            #[cfg(test)]
            validation_before_start_hook: Mutex::new(None),
        })
    }

    fn draft_root(&self) -> PathBuf {
        self.root.join("drafts")
    }

    fn definition_root(&self) -> PathBuf {
        self.root.join("definitions")
    }

    pub(super) fn quarantine_contaminated_evidence(
        &self,
        runs: &Mutex<HashMap<String, Arc<RunState>>>,
        secrets: &[Vec<u8>],
    ) -> bool {
        let _evidence_lifecycle = lock(&self.evidence_lifecycle);
        extend_secret_values(&self.secret_values, secrets.iter().cloned());
        let all_secrets = lock(&self.secret_values).clone();
        let drafts = lock(&self.drafts).values().cloned().collect::<Vec<_>>();
        let definitions = lock(&self.definitions)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut contaminated = false;
        for state in drafts {
            if confined_bundle_contains_protected_data(&state.anchor, &all_secrets).unwrap_or(true)
            {
                contaminated = true;
                state.evidence_quarantined.store(true, Ordering::Release);
                for cancel in lock(&state.validation_cancels).values() {
                    cancel.cancel();
                }
                let (id, validation_run_ids) = {
                    let detail = lock(&state.detail);
                    (
                        detail.summary.id.clone(),
                        detail
                            .validations
                            .iter()
                            .filter_map(|attempt| attempt.run_id.clone())
                            .collect::<Vec<_>>(),
                    )
                };
                if !quarantine_run_bundle(&state.anchor, &id) {
                    let _ = remove_confined_run_entry(&state.anchor, Path::new("manifest.json"));
                }
                for run_id in validation_run_ids {
                    if let Some(run) = lock(runs).get(&run_id).cloned() {
                        mark_workspace_evidence_unavailable(&run, false);
                    }
                }
                lock(&self.drafts).remove(&id);
            }
        }
        for state in definitions {
            if confined_bundle_contains_protected_data(&state.anchor, &all_secrets).unwrap_or(true)
            {
                contaminated = true;
                let id = state.detail.summary.id.clone();
                if !quarantine_run_bundle(&state.anchor, &id) {
                    let _ = remove_confined_run_entry(&state.anchor, Path::new("manifest.json"));
                }
                lock(&self.definitions).remove(&id);
            }
        }
        contaminated
    }
}

impl RunController {
    /// List evaluation drafts in most-recently-updated order.
    #[must_use]
    pub fn list_evaluation_drafts(&self) -> Vec<EvaluationDraftSummary> {
        let mut drafts = lock(&self.inner.promotion.drafts)
            .values()
            .map(|state| lock(&state.detail).summary.clone())
            .collect::<Vec<_>>();
        drafts.sort_by_key(|draft| std::cmp::Reverse(draft.updated_at_ms));
        drafts
    }

    /// Return one durable evaluation draft.
    ///
    /// # Errors
    ///
    /// Returns an error when the draft is unknown.
    pub fn evaluation_draft(&self, id: &str) -> Result<EvaluationDraftDetail, RunError> {
        Ok(lock(&self.promotion_draft_state(id)?.detail).clone())
    }

    /// List promoted evaluation definitions in most-recently-created order.
    #[must_use]
    pub fn list_evaluation_definitions(&self) -> Vec<EvaluationDefinitionSummary> {
        let mut definitions = lock(&self.inner.promotion.definitions)
            .values()
            .map(|state| state.detail.summary.clone())
            .collect::<Vec<_>>();
        definitions.sort_by_key(|definition| std::cmp::Reverse(definition.created_at_ms));
        definitions
    }

    /// Return one promoted evaluation definition.
    ///
    /// # Errors
    ///
    /// Returns an error when the definition is unknown.
    pub fn evaluation_definition(&self, id: &str) -> Result<EvaluationDefinitionDetail, RunError> {
        lock(&self.inner.promotion.definitions)
            .get(id)
            .map(|state| state.detail.clone())
            .ok_or_else(|| RunError::InvalidRequest(format!("unknown evaluation definition: {id}")))
    }

    /// Create a draft from a contiguous span of terminal agent turns.
    ///
    /// # Errors
    ///
    /// Returns an error when the workspace, session, turn span, or durable evidence is invalid.
    #[allow(clippy::too_many_lines)]
    pub fn create_evaluation_draft(
        &self,
        workspace_id: &str,
        request: CreateEvaluationDraftRequest,
        origin: WorkbenchOrigin,
    ) -> Result<EvaluationDraftDetail, RunError> {
        let workspace = self.state(workspace_id)?;
        if lock(&workspace.summary).status != RunStatus::Exploring {
            return Err(RunError::RunUnavailable(workspace_id.to_owned()));
        }
        if lock(&workspace.active_agent_turn).is_some() {
            return Err(RunError::InvalidRequest(
                "finish or cancel the active agent turn before creating an evaluation draft"
                    .to_owned(),
            ));
        }
        let session_id = request
            .session_id
            .or_else(|| lock(&workspace.active_agent_session_id).clone())
            .ok_or_else(|| {
                RunError::InvalidRequest(
                    "provide a session id or activate an agent session first".to_owned(),
                )
            })?;
        let session = self.agent_session_state(&session_id)?;
        let session_summary = lock(&session.summary).clone();
        if session_summary.workspace_id != workspace_id {
            return Err(RunError::UnknownAgentSession(session_id));
        }
        let turns = lock(&session.turns).clone();
        let from = turns
            .iter()
            .position(|turn| turn.id == request.from_turn_id)
            .ok_or_else(|| {
                RunError::InvalidRequest(format!("unknown source turn: {}", request.from_turn_id))
            })?;
        let through = turns
            .iter()
            .position(|turn| turn.id == request.through_turn_id)
            .ok_or_else(|| {
                RunError::InvalidRequest(format!(
                    "unknown source turn: {}",
                    request.through_turn_id
                ))
            })?;
        if through < from {
            return Err(RunError::InvalidRequest(
                "--through must not precede --from".to_owned(),
            ));
        }
        let selected = &turns[from..=through];
        if selected.iter().any(|turn| !turn.status.is_finished()) {
            return Err(RunError::InvalidRequest(
                "evaluation drafts require completed or otherwise terminal turns".to_owned(),
            ));
        }

        let first = selected.first().ok_or_else(|| {
            RunError::InvalidRequest("evaluation turn span must not be empty".to_owned())
        })?;
        let snapshot = capture_confined_tree(
            &session.evidence_root,
            &PathBuf::from("turns").join(&first.id).join("initial"),
        )?;
        if captured_tree_digest(&snapshot) != first.source_revision {
            return Err(RunError::EvidencePersistence(format!(
                "turn {} initial snapshot no longer matches its source revision",
                first.id
            )));
        }
        let scenario_id = lock(&workspace.summary).scenario_id.clone();
        let scenario = self
            .inner
            .scenarios
            .get(&scenario_id)
            .ok_or_else(|| RunError::UnknownScenario(scenario_id.clone()))?;
        let mut blocking_issues = Vec::new();
        if selected
            .iter()
            .any(|turn| turn.source_revision != first.source_revision)
        {
            blocking_issues.push(
                "selected turns cross workspace revisions; narrow the source span".to_owned(),
            );
        }
        if selected
            .iter()
            .any(|turn| turn.capability_revisions != first.capability_revisions)
        {
            blocking_issues.push(
                "selected turns cross capability revisions; narrow the source span".to_owned(),
            );
        }
        blocking_issues.push(MANUAL_AUTHORING_BLOCKER.to_owned());
        let presentations = selected
            .iter()
            .map(|turn| {
                load_or_build_agent_turn_presentation(&session, &workspace, turn).map_err(|error| {
                    RunError::EvidencePersistence(format!(
                        "could not project source turn {}: {error}",
                        turn.id
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let source_event_sequences = presentations
            .iter()
            .flat_map(|presentation| presentation.source_event_sequences.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let source_digest = selected_turns_digest(selected, &presentations)?;
        let capability_recipe = lock(&workspace.assembly).capability_sources.clone();
        let id = format!("draft-{}-{}", now_ms(), random_suffix());
        let revision_id = format!("revision-{}-{}", now_ms(), random_suffix());
        let created_at_ms = now_ms();
        let revision = EvaluationRevision {
            schema_version: PROMOTION_SCHEMA_VERSION,
            id: revision_id.clone(),
            draft_id: id.clone(),
            previous_revision_id: None,
            created_at_ms,
            task: first.prompt.clone(),
            source: EvaluationSourceProvenance {
                workspace_id: workspace_id.to_owned(),
                session_id: session_summary.id,
                turn_ids: selected.iter().map(|turn| turn.id.clone()).collect(),
                source_revision: first.source_revision.clone(),
                source_digest,
                capability_revisions: first.capability_revisions.clone(),
                source_event_sequences,
                scenario_id: scenario.id.clone(),
                harness_id: session_summary.harness_id,
                model_profile_id: session_summary.model_profile_id,
                model_id: session_summary.model_id,
            },
            capability_recipe,
            limits: scenario.limits.clone(),
            evaluator: catalog_evaluator(scenario),
            measurements: vec![
                "duration".to_owned(),
                "model-turns".to_owned(),
                "capability-calls".to_owned(),
                "workspace-effects".to_owned(),
                "reported-usage".to_owned(),
            ],
            blocking_issues,
        };
        validate_revision(&revision)?;
        let summary = EvaluationDraftSummary {
            id: id.clone(),
            workspace_id: workspace_id.to_owned(),
            name: format!("{} evaluation", scenario.title),
            current_revision_id: revision_id.clone(),
            status: revision_status(&revision).to_owned(),
            saved: false,
            definition_id: None,
            promoted_revision_id: None,
            created_at_ms,
            updated_at_ms: created_at_ms,
        };
        let detail = EvaluationDraftDetail {
            summary,
            revisions: vec![revision],
            validations: Vec::new(),
            events: Vec::new(),
        };
        let directory = confined_child(&self.inner.promotion.draft_root(), &id)?;
        fs::create_dir(&directory)?;
        let anchor = Arc::new(AgentSessionDirectoryAnchor::open(directory)?);
        write_confined_run_captured_tree(
            &anchor,
            &PathBuf::from("revisions").join(&revision_id).join("source"),
            &snapshot,
        )?;
        let known_secrets = lock(&self.inner.promotion.secret_values);
        if !known_secrets.is_empty()
            && confined_bundle_contains_protected_data(&anchor, &known_secrets).unwrap_or(true)
        {
            let _ = quarantine_run_bundle(&anchor, &id);
            return Err(RunError::EvidencePersistence(
                PROTECTED_WORKSPACE_PATH_ERROR.to_owned(),
            ));
        }
        let (sender, _) = broadcast::channel(128);
        let state = Arc::new(PromotionDraftState {
            detail: Mutex::new(detail),
            anchor,
            sender,
            event_commit: Mutex::new(()),
            validation_cancels: Mutex::new(HashMap::new()),
            evidence_quarantined: AtomicBool::new(false),
        });
        persist_draft(&state)?;
        record_draft_event(
            &state,
            "evaluation-draft.created",
            json!({
                "draftId": id,
                "revisionId": revision_id,
                "workspaceId": workspace_id,
                "origin": origin,
            }),
        )?;
        lock(&self.inner.promotion.drafts).insert(id.clone(), state);
        drop(known_secrets);
        record_event(
            &workspace,
            "workbench.evaluation-draft.created",
            json!({
                "origin": origin,
                "draftId": id,
                "revisionId": revision_id,
            }),
        )?;
        self.evaluation_draft(&id)
    }

    /// Create an immutable revision from an optimistic draft edit.
    ///
    /// # Errors
    ///
    /// Returns an error when the base revision is stale or the edit is invalid.
    pub fn update_evaluation_draft(
        &self,
        id: &str,
        request: UpdateEvaluationDraftRequest,
        origin: WorkbenchOrigin,
    ) -> Result<EvaluationDraftDetail, RunError> {
        let state = self.promotion_draft_state(id)?;
        let event_commit = lock(&state.event_commit);
        let mut detail = lock(&state.detail);
        let previous_detail = detail.clone();
        if previous_detail.summary.current_revision_id != request.base_revision_id {
            return Err(RunError::Conflict(format!(
                "stale evaluation draft revision: expected {}, received {}",
                previous_detail.summary.current_revision_id, request.base_revision_id
            )));
        }
        let current = current_draft_revision(&previous_detail, id)?;
        let mut next = current.clone();
        let confirms_manual_authoring = request.revision.task.is_some()
            && request.revision.evaluator.is_some()
            && request.revision.measurements.is_some();
        if let Some(task) = request.revision.task {
            next.task = task;
        }
        if let Some(limits) = request.revision.limits {
            next.limits = limits;
        }
        if let Some(evaluator) = request.revision.evaluator {
            next.evaluator = evaluator;
        }
        if let Some(measurements) = request.revision.measurements {
            next.measurements = measurements;
        }
        if confirms_manual_authoring {
            next.blocking_issues
                .retain(|issue| issue != MANUAL_AUTHORING_BLOCKER);
        }
        if let Some(name) = &request.name {
            validate_display_name(name)?;
        }
        let material_change = next.task != current.task
            || next.limits != current.limits
            || next.evaluator != current.evaluator
            || next.measurements != current.measurements
            || next.blocking_issues != current.blocking_issues;
        let mut next_detail = previous_detail.clone();
        let mut created_revision_path = None;
        if material_change {
            if previous_detail.revisions.len() >= MAX_EVALUATION_DRAFT_REVISIONS {
                return Err(RunError::InvalidRequest(format!(
                    "evaluation drafts retain at most {MAX_EVALUATION_DRAFT_REVISIONS} revisions"
                )));
            }
            let revision_id = format!("revision-{}-{}", now_ms(), random_suffix());
            let source = capture_confined_run_tree(
                &state.anchor,
                &PathBuf::from("revisions").join(&current.id).join("source"),
            )?;
            next.id.clone_from(&revision_id);
            next.previous_revision_id = Some(current.id);
            next.created_at_ms = now_ms();
            validate_revision(&next)?;
            let revision_path = PathBuf::from("revisions").join(&revision_id);
            write_confined_run_captured_tree(
                &state.anchor,
                &revision_path.join("source"),
                &source,
            )?;
            created_revision_path = Some(revision_path);
            next_detail.summary.current_revision_id = revision_id;
            revision_status(&next).clone_into(&mut next_detail.summary.status);
            next_detail.summary.saved = false;
            next_detail.summary.definition_id = None;
            next_detail.summary.promoted_revision_id = None;
            next_detail.revisions.push(next);
        }
        if let Some(name) = request.name {
            next_detail.summary.name = name;
        }
        next_detail.summary.updated_at_ms = now_ms();
        let event = RunEvent {
            sequence: next_detail.events.len() as u64 + 1,
            at_ms: now_ms(),
            kind: "evaluation-draft.revised".to_owned(),
            payload: json!({
                "draftId": id,
                "revisionId": next_detail.summary.current_revision_id,
                "origin": origin,
                "materialChange": material_change,
            }),
            progress: None,
        };
        next_detail.events.push(event.clone());
        persist_draft_transition(
            &state,
            &previous_detail,
            &next_detail,
            &event,
            created_revision_path.as_deref(),
        )?;
        *detail = next_detail.clone();
        drop(detail);
        drop(event_commit);
        let _ = state.sender.send(event);
        self.notify_evaluation_library_changed(&state, "revised");
        Ok(next_detail)
    }

    /// Queue one validation replay for an immutable draft revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the draft or revision is unknown, incomplete, or already validating.
    pub fn start_evaluation_validation(
        &self,
        draft_id: &str,
        revision_id: Option<&str>,
    ) -> Result<EvaluationValidationAttempt, RunError> {
        let state = self.promotion_draft_state(draft_id)?;
        let event_commit = lock(&state.event_commit);
        let mut detail = lock(&state.detail);
        let previous_detail = detail.clone();
        let revision_id = revision_id
            .unwrap_or(&previous_detail.summary.current_revision_id)
            .to_owned();
        let revision = previous_detail
            .revisions
            .iter()
            .find(|revision| revision.id == revision_id)
            .cloned()
            .ok_or_else(|| {
                RunError::InvalidRequest(format!("unknown evaluation revision: {revision_id}"))
            })?;
        if !revision.blocking_issues.is_empty() {
            return Err(RunError::InvalidRequest(format!(
                "evaluation revision is incomplete: {}",
                revision.blocking_issues.join("; ")
            )));
        }
        if previous_detail.validations.iter().any(|attempt| {
            attempt.revision_id == revision_id && !attempt.execution_status.is_finished()
        }) {
            return Err(RunError::InvalidRequest(
                "this evaluation revision already has an active validation".to_owned(),
            ));
        }
        let attempt = EvaluationValidationAttempt {
            id: format!("validation-{}-{}", now_ms(), random_suffix()),
            draft_id: draft_id.to_owned(),
            revision_id: revision_id.clone(),
            execution_status: EvaluationExecutionStatus::Queued,
            assertion_status: ValidationAssertionStatus::NotEvaluated,
            harness_id: revision.source.harness_id.clone(),
            model_profile_id: revision.source.model_profile_id.clone(),
            run_id: None,
            started_at_ms: now_ms(),
            finished_at_ms: None,
            error: None,
            score: None,
        };
        let cancel = CancellationToken::new();
        lock(&state.validation_cancels).insert(attempt.id.clone(), cancel.clone());
        let mut next_detail = previous_detail.clone();
        next_detail.validations.push(attempt.clone());
        next_detail.summary.updated_at_ms = now_ms();
        let event = RunEvent {
            sequence: next_detail.events.len() as u64 + 1,
            at_ms: now_ms(),
            kind: "evaluation-validation.created".to_owned(),
            payload: json!({
                "draftId": draft_id,
                "revisionId": revision_id,
                "validationId": attempt.id,
            }),
            progress: None,
        };
        next_detail.events.push(event.clone());
        if let Err(error) =
            persist_draft_transition(&state, &previous_detail, &next_detail, &event, None)
        {
            lock(&state.validation_cancels).remove(&attempt.id);
            return Err(error);
        }
        *detail = next_detail;
        drop(detail);
        drop(event_commit);
        let _ = state.sender.send(event);
        let controller = self.clone();
        let state_for_task = state.clone();
        let attempt_id = attempt.id.clone();
        tokio::spawn(async move {
            controller
                .execute_evaluation_validation(state_for_task, revision, attempt_id, cancel)
                .await;
        });
        self.notify_evaluation_library_changed(&state, "validation-started");
        Ok(attempt)
    }

    /// Cancel an active validation while retaining its durable attempt.
    ///
    /// # Errors
    ///
    /// Returns an error when the draft or active validation is unknown.
    pub fn cancel_evaluation_validation(
        &self,
        draft_id: &str,
        validation_id: &str,
    ) -> Result<(), RunError> {
        let state = self.promotion_draft_state(draft_id)?;
        let cancel = lock(&state.validation_cancels)
            .get(validation_id)
            .cloned()
            .ok_or_else(|| {
                RunError::InvalidRequest(format!(
                    "unknown or completed validation: {validation_id}"
                ))
            })?;
        cancel.cancel();
        if let Some(run_id) = lock(&state.detail)
            .validations
            .iter()
            .find(|attempt| attempt.id == validation_id)
            .and_then(|attempt| attempt.run_id.clone())
        {
            let _ = self.cancel(&run_id);
        }
        Ok(())
    }

    /// Retain a draft revision and promote it when that exact revision has passed.
    ///
    /// # Errors
    ///
    /// Returns an error when the draft, revision, name, or durable evidence is invalid.
    pub fn save_evaluation_draft(
        &self,
        draft_id: &str,
        request: SaveEvaluationDraftRequest,
    ) -> Result<EvaluationDraftDetail, RunError> {
        let evidence_lifecycle = lock(&self.inner.promotion.evidence_lifecycle);
        let state = self.promotion_draft_state(draft_id)?;
        let event_commit = lock(&state.event_commit);
        let mut detail = lock(&state.detail);
        let previous_detail = detail.clone();
        let mut next_detail = previous_detail.clone();
        let revision_id = request
            .revision_id
            .unwrap_or_else(|| next_detail.summary.current_revision_id.clone());
        let revision = next_detail
            .revisions
            .iter()
            .find(|revision| revision.id == revision_id)
            .cloned()
            .ok_or_else(|| {
                RunError::InvalidRequest(format!("unknown evaluation revision: {revision_id}"))
            })?;
        if let Some(name) = request.name {
            validate_display_name(&name)?;
            next_detail.summary.name = name;
        }
        let passing = next_detail.validations.iter().any(|attempt| {
            attempt.revision_id == revision_id
                && attempt.execution_status == EvaluationExecutionStatus::Complete
                && attempt.assertion_status == ValidationAssertionStatus::Passed
        });
        let mut publication = None;
        if passing {
            let existing = lock(&self.inner.promotion.definitions)
                .values()
                .find(|definition| {
                    definition.detail.summary.draft_id == draft_id
                        && definition.detail.summary.revision_id == revision_id
                })
                .cloned();
            let prepared = prepare_definition_publication(
                &state,
                draft_id,
                revision,
                existing,
                &next_detail.summary.name,
            )?;
            let definition_id = prepared.detail.summary.id.clone();
            publication = Some(begin_definition_publication(
                &self.inner.promotion,
                prepared,
            )?);
            apply_saved_revision_status(
                &mut next_detail,
                draft_id,
                &revision_id,
                Some(definition_id),
            )?;
        } else if next_detail.summary.current_revision_id == revision_id {
            apply_saved_revision_status(&mut next_detail, draft_id, &revision_id, None)?;
        }
        next_detail.summary.updated_at_ms = now_ms();
        let event = RunEvent {
            sequence: next_detail.events.len() as u64 + 1,
            at_ms: now_ms(),
            kind: "evaluation-draft.saved".to_owned(),
            payload: json!({
                "draftId": draft_id,
                "revisionId": revision_id,
                "definitionId": next_detail.summary.definition_id,
            }),
            progress: None,
        };
        next_detail.events.push(event.clone());
        if let Err(error) =
            persist_draft_transition(&state, &previous_detail, &next_detail, &event, None)
        {
            return rollback_definition_after_draft_failure(
                &self.inner.promotion,
                publication,
                error,
            );
        }
        if let Some(publication) = publication
            && let Err(error) = commit_definition_publication(&self.inner.promotion, &publication)
        {
            let draft_rollback = rollback_draft_transition(&state, &previous_detail);
            let definition_rollback =
                rollback_definition_publication(&self.inner.promotion, publication);
            return combine_publication_rollback_errors(
                &error,
                draft_rollback,
                definition_rollback,
            );
        }
        let response = next_detail.clone();
        *detail = next_detail;
        drop(detail);
        let _ = state.sender.send(event);
        drop(event_commit);
        drop(evidence_lifecycle);
        self.notify_evaluation_library_changed(&state, "saved");
        Ok(response)
    }

    /// Run a promoted definition through a compatible harness pair.
    ///
    /// # Errors
    ///
    /// Returns an error when the definition, harnesses, model, access, or snapshot is invalid.
    pub fn start_definition_evaluation(
        &self,
        definition_id: &str,
        request: StartDefinitionEvaluationRequest,
    ) -> Result<EvaluationSummary, RunError> {
        let definition = lock(&self.inner.promotion.definitions)
            .get(definition_id)
            .cloned()
            .ok_or_else(|| {
                RunError::InvalidRequest(format!("unknown evaluation definition: {definition_id}"))
            })?;
        let selection = self
            .state(&definition.detail.revision.source.workspace_id)
            .map_or_else(
                |_| default_workbench_selection(&self.inner.harnesses, &self.inner.model_profiles),
                |workspace| lock(&workspace.selection).clone(),
            );
        let harness_ids = request
            .harness_ids
            .unwrap_or(selection.comparison_harness_ids);
        validate_comparison_harnesses(&self.inner.harnesses, &harness_ids)?;
        let model_profile_id = request
            .model_profile_id
            .or(selection.model_profile_id)
            .ok_or_else(|| RunError::InvalidRequest("choose a model profile first".to_owned()))?;
        let snapshot = capture_confined_run_tree(&definition.anchor, Path::new("source"))?;
        verify_captured_source_revision(
            &snapshot,
            &definition.detail.revision.source.source_revision,
            "evaluation definition",
        )?;
        self.start_evaluation_from_snapshot(
            &definition.detail.revision.source.scenario_id,
            &definition.detail.revision.task,
            &definition.detail.revision.limits,
            &definition.detail.revision.evaluator,
            &definition.detail.revision.capability_recipe,
            model_profile_id,
            definition.detail.revision.source.workspace_id.clone(),
            definition.detail.revision.source.source_revision.clone(),
            &harness_ids,
            &snapshot,
            Some((
                definition.detail.summary.id.clone(),
                definition.detail.summary.revision_id.clone(),
            )),
        )
    }

    /// Run a promoted definition from one workbench and publish the shared
    /// evaluation transition to every attached projection.
    ///
    /// # Errors
    ///
    /// Returns an error when the definition does not belong to the workbench
    /// or the evaluation cannot be started.
    pub fn start_workbench_definition_evaluation(
        &self,
        workspace_id: &str,
        definition_id: &str,
        request: StartDefinitionEvaluationRequest,
        origin: WorkbenchOrigin,
    ) -> Result<EvaluationSummary, RunError> {
        let definition = self.evaluation_definition(definition_id)?;
        if definition.revision.source.workspace_id != workspace_id {
            return Err(RunError::InvalidRequest(format!(
                "evaluation definition {definition_id} does not belong to workspace {workspace_id}"
            )));
        }
        let state = self.state(workspace_id)?;
        let evaluation = self.start_definition_evaluation(definition_id, request)?;
        record_event(
            &state,
            "workbench.evaluation.started",
            json!({
                "origin": origin,
                "evaluationId": evaluation.id,
                "definitionId": definition_id,
                "modelProfileId": evaluation.model_profile_id,
                "harnessIds": evaluation.harness_ids,
            }),
        )?;
        Ok(evaluation)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn start_evaluation_from_snapshot(
        &self,
        scenario_id: &str,
        task: &str,
        limits: &ScenarioLimits,
        evaluator: &EvaluationEvaluator,
        capability_recipe: &[CapabilityAssembly],
        model_profile_id: String,
        source_workspace_id: String,
        source_revision: String,
        harness_ids: &[String],
        snapshot: &CapturedTree,
        definition: Option<(String, String)>,
    ) -> Result<EvaluationSummary, RunError> {
        validate_comparison_harnesses(&self.inner.harnesses, harness_ids)?;
        for harness_id in harness_ids {
            let harness = self.inner.harnesses.get(harness_id).ok_or_else(|| {
                RunError::InvalidRequest(format!("unknown harness: {harness_id}"))
            })?;
            if !harness.models.contains_key(&model_profile_id) {
                return Err(RunError::InvalidRequest(format!(
                    "model profile {model_profile_id} is unavailable for harness {harness_id}"
                )));
            }
            self.ensure_harness_model_access_ready(harness)?;
        }
        let revision = EvaluationRevision {
            schema_version: PROMOTION_SCHEMA_VERSION,
            id: definition.as_ref().map_or_else(
                || "definition-revision".to_owned(),
                |(_, revision)| revision.clone(),
            ),
            draft_id: "definition-source".to_owned(),
            previous_revision_id: None,
            created_at_ms: now_ms(),
            task: task.to_owned(),
            source: EvaluationSourceProvenance {
                workspace_id: source_workspace_id.clone(),
                session_id: String::new(),
                turn_ids: Vec::new(),
                source_revision: source_revision.clone(),
                source_digest: String::new(),
                capability_revisions: capability_recipe
                    .iter()
                    .map(|capability| (capability.id.clone(), capability.revision.clone()))
                    .collect(),
                source_event_sequences: Vec::new(),
                scenario_id: scenario_id.to_owned(),
                harness_id: String::new(),
                model_profile_id: model_profile_id.clone(),
                model_id: String::new(),
            },
            capability_recipe: capability_recipe.to_vec(),
            limits: limits.clone(),
            evaluator: evaluator.clone(),
            measurements: Vec::new(),
            blocking_issues: Vec::new(),
        };
        let scenario = scenario_for_revision(&revision)?;
        let id = format!("evaluation-{}", run_id());
        let bundle_dir = confined_child(&self.inner.evaluations_dir, &id)?;
        fs::create_dir(&bundle_dir)?;
        let bundle_directories = Arc::new(AgentSessionDirectoryAnchor::open(bundle_dir.clone())?);
        let pending_bundle = PendingEvaluationBundle::new(id.clone(), bundle_directories.clone());
        write_confined_run_captured_tree(&bundle_directories, Path::new("source"), snapshot)?;
        let (definition_id, definition_revision_id) = definition
            .map_or((None, None), |(definition_id, revision_id)| {
                (Some(definition_id), Some(revision_id))
            });
        write_confined_run_json_atomic(
            &bundle_directories,
            Path::new("source.json"),
            &json!({
                "workspaceId": source_workspace_id,
                "revision": source_revision,
                "definitionId": definition_id,
                "definitionRevisionId": definition_revision_id,
                "task": task,
                "evaluator": evaluator,
                "capabilityRecipe": capability_recipe,
                "limits": limits,
            }),
        )?;
        let summary = EvaluationSummary {
            id: id.clone(),
            scenario_id: scenario_id.to_owned(),
            model_profile_id,
            source_workspace_id,
            source_revision,
            definition_id,
            definition_revision_id,
            harness_ids: harness_ids.to_owned(),
            arms: harness_ids
                .iter()
                .map(|harness_id| EvaluationArmSummary {
                    harness_id: harness_id.clone(),
                    run_id: None,
                    status: "queued".to_owned(),
                })
                .collect(),
            status: EvaluationStatus::Queued,
            started_at_ms: now_ms(),
            finished_at_ms: None,
        };
        let (sender, _) = broadcast::channel(256);
        let state = Arc::new(EvaluationState {
            summary: Mutex::new(summary.clone()),
            events: Mutex::new(Vec::new()),
            producer_lifecycle: Mutex::new(()),
            event_commit: Mutex::new(()),
            sender,
            cancel: CancellationToken::new(),
            bundle_directories,
            evidence_quarantined: AtomicBool::new(false),
            replay_failed: false,
            scenario_override: Some(scenario),
            capability_recipe: Some(capability_recipe.to_vec()),
        });
        write_confined_run_json_atomic(
            &state.bundle_directories,
            Path::new("manifest.json"),
            &serde_json::to_value(&summary)?,
        )?;
        record_evaluation_event(
            &state,
            "evaluation.created",
            json!({
                "sourceRevision": summary.source_revision,
                "definitionId": summary.definition_id,
                "definitionRevisionId": summary.definition_revision_id,
                "harnessIds": summary.harness_ids,
            }),
        )?;
        lock(&self.inner.evaluations).insert(id, state.clone());
        pending_bundle.commit();
        let controller = self.clone();
        tokio::spawn(async move {
            if let Err(error) = controller.execute_evaluation(state.clone()).await {
                let message = error.to_string();
                finish_evaluation(&state, EvaluationStatus::Failed, Some(&message));
            }
        });
        Ok(summary)
    }

    /// Subscribe to the durable event history and live events for one draft.
    ///
    /// # Errors
    ///
    /// Returns an error when the draft is unknown.
    pub fn subscribe_evaluation_draft(
        &self,
        id: &str,
    ) -> Result<(Vec<RunEvent>, broadcast::Receiver<RunEvent>), RunError> {
        let state = self.promotion_draft_state(id)?;
        let _commit = lock(&state.event_commit);
        let detail = lock(&state.detail);
        let receiver = state.sender.subscribe();
        Ok((detail.events.clone(), receiver))
    }

    /// Return draft events after an acknowledged durable sequence.
    ///
    /// # Errors
    ///
    /// Returns an error when the draft is unknown.
    pub fn evaluation_draft_events_after(
        &self,
        id: &str,
        sequence: u64,
    ) -> Result<Vec<RunEvent>, RunError> {
        let state = self.promotion_draft_state(id)?;
        Ok(lock(&state.detail)
            .events
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect())
    }

    fn promotion_draft_state(&self, id: &str) -> Result<Arc<PromotionDraftState>, RunError> {
        let state = lock(&self.inner.promotion.drafts)
            .get(id)
            .cloned()
            .ok_or_else(|| RunError::InvalidRequest(format!("unknown evaluation draft: {id}")))?;
        if state.evidence_quarantined.load(Ordering::Acquire) {
            return Err(RunError::InvalidRequest(format!(
                "unknown evaluation draft: {id}"
            )));
        }
        Ok(state)
    }

    #[cfg(test)]
    pub(super) fn install_validation_before_start_hook(
        &self,
    ) -> (Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>) {
        let reached = Arc::new(tokio::sync::Barrier::new(2));
        let resume = Arc::new(tokio::sync::Barrier::new(2));
        *lock(&self.inner.promotion.validation_before_start_hook) =
            Some(ValidationBeforeStartHook {
                reached: reached.clone(),
                resume: resume.clone(),
            });
        (reached, resume)
    }

    #[cfg(test)]
    pub(super) fn fail_next_validation_assembly_persist(&self) {
        self.inner
            .promotion
            .fail_next_validation_assembly_persist
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn inject_stale_validation_manifest_for_recovery_test(
        &self,
        draft_id: &str,
        validation: EvaluationValidationAttempt,
    ) -> Result<(), RunError> {
        let state = self.promotion_draft_state(draft_id)?;
        lock(&state.detail).validations.push(validation.clone());
        persist_draft(&state)?;
        let stale_manifest = serde_json::to_value(lock(&state.detail).clone())?;
        record_draft_event(
            &state,
            "evaluation-validation.created",
            serde_json::to_value(validation)?,
        )?;
        write_confined_run_json_atomic(&state.anchor, Path::new("manifest.json"), &stale_manifest)
    }

    #[cfg(test)]
    pub(super) fn inject_definition_publication_after_draft_commit_for_recovery_test(
        &self,
        draft_id: &str,
        revision_id: &str,
        name: &str,
    ) -> Result<String, RunError> {
        let state = self.promotion_draft_state(draft_id)?;
        let detail = lock(&state.detail).clone();
        let revision = detail
            .revisions
            .iter()
            .find(|revision| revision.id == revision_id)
            .cloned()
            .ok_or_else(|| {
                RunError::InvalidRequest(format!("unknown evaluation revision: {revision_id}"))
            })?;
        let existing = lock(&self.inner.promotion.definitions)
            .values()
            .find(|definition| {
                definition.detail.summary.draft_id == draft_id
                    && definition.detail.summary.revision_id == revision_id
            })
            .cloned();
        let prepared = prepare_definition_publication(&state, draft_id, revision, existing, name)?;
        let definition_id = prepared.detail.summary.id.clone();
        let _publication = begin_definition_publication(&self.inner.promotion, prepared)?;
        let mut committed_draft = detail;
        committed_draft.summary.name = name.to_owned();
        committed_draft.summary.saved = true;
        committed_draft.summary.status = "promoted".to_owned();
        committed_draft.summary.definition_id = Some(definition_id.clone());
        committed_draft.summary.promoted_revision_id = Some(revision_id.to_owned());
        committed_draft.summary.updated_at_ms = now_ms();
        persist_draft_detail(&state, &committed_draft)?;
        Ok(definition_id)
    }

    async fn execute_evaluation_validation(
        &self,
        state: Arc<PromotionDraftState>,
        revision: EvaluationRevision,
        attempt_id: String,
        cancel: CancellationToken,
    ) {
        let result = self
            .execute_evaluation_validation_inner(&state, &revision, &attempt_id, cancel.clone())
            .await;
        if let Err(error) = result {
            let _ = update_validation_attempt(&state, &attempt_id, |attempt| {
                attempt.execution_status = if cancel.is_cancelled() {
                    EvaluationExecutionStatus::Cancelled
                } else {
                    EvaluationExecutionStatus::Inconclusive
                };
                attempt.assertion_status = ValidationAssertionStatus::NotEvaluated;
                attempt.finished_at_ms = Some(now_ms());
                attempt.error = Some(error.to_string());
            });
        }
        if let Some(run_id) = lock(&state.detail)
            .validations
            .iter()
            .find(|attempt| attempt.id == attempt_id)
            .and_then(|attempt| attempt.run_id.clone())
        {
            lock(&self.inner.scenario_overrides).remove(&run_id);
        }
        lock(&state.validation_cancels).remove(&attempt_id);
        let _ = persist_draft(&state);
        let attempt = lock(&state.detail)
            .validations
            .iter()
            .find(|attempt| attempt.id == attempt_id)
            .cloned();
        let _ = record_draft_event(
            &state,
            "evaluation-validation.finished",
            serde_json::to_value(attempt).unwrap_or_else(|_| {
                json!({
                    "validationId": attempt_id,
                    "executionStatus": "inconclusive",
                })
            }),
        );
        self.notify_evaluation_library_changed(&state, "validation-finished");
    }

    fn notify_evaluation_library_changed(&self, state: &PromotionDraftState, change: &str) {
        let summary = lock(&state.detail).summary.clone();
        if let Ok(workspace) = self.state(&summary.workspace_id) {
            let _ = record_event(
                &workspace,
                "workbench.evaluation-library.changed",
                json!({
                    "draftId": summary.id,
                    "change": change,
                }),
            );
        }
    }

    async fn execute_evaluation_validation_inner(
        &self,
        state: &PromotionDraftState,
        revision: &EvaluationRevision,
        attempt_id: &str,
        cancel: CancellationToken,
    ) -> Result<(), RunError> {
        update_validation_attempt(state, attempt_id, |attempt| {
            attempt.execution_status = EvaluationExecutionStatus::Running;
        })?;
        persist_draft(state)?;
        record_draft_event(
            state,
            "evaluation-validation.status",
            json!({ "validationId": attempt_id, "status": "running" }),
        )?;
        let Some(prepared) = self
            .prepare_validation_replay(state, revision, attempt_id, &cancel)
            .await?
        else {
            return Ok(());
        };
        update_validation_attempt(state, attempt_id, |attempt| {
            attempt.run_id = Some(prepared.id.clone());
        })?;
        if let Err(error) = persist_draft(state) {
            lock(&self.inner.scenario_overrides).remove(&prepared.id);
            let _ = self.cancel(&prepared.id);
            return Err(error);
        }
        #[cfg(test)]
        self.wait_for_validation_before_start_test_hook().await;
        if self.cancel_validation_before_start_if_requested(
            state,
            attempt_id,
            &prepared.id,
            &cancel,
        )? {
            return Ok(());
        }
        if let Err(error) = self.start_prepared(
            &prepared.id,
            &StartPreparedRunRequest {
                model_id: None,
                harness_id: Some(revision.source.harness_id.clone()),
                model_profile_id: Some(revision.source.model_profile_id.clone()),
            },
        ) {
            lock(&self.inner.scenario_overrides).remove(&prepared.id);
            return Err(error);
        }
        loop {
            if cancel.is_cancelled() {
                let _ = self.cancel(&prepared.id);
            }
            let run = self.get(&prepared.id)?;
            if run.summary.status.is_finished() {
                lock(&self.inner.scenario_overrides).remove(&prepared.id);
                let score = run.score.clone();
                let (execution_status, assertion_status, error) = match run.summary.status {
                    RunStatus::Passed => (
                        EvaluationExecutionStatus::Complete,
                        ValidationAssertionStatus::Passed,
                        None,
                    ),
                    RunStatus::Cancelled => (
                        EvaluationExecutionStatus::Cancelled,
                        ValidationAssertionStatus::NotEvaluated,
                        run.summary.error.clone(),
                    ),
                    RunStatus::Failed
                        if run.summary.error.is_none()
                            && score.as_ref().is_some_and(catalog_score_is_complete) =>
                    {
                        (
                            EvaluationExecutionStatus::Complete,
                            ValidationAssertionStatus::Failed,
                            None,
                        )
                    }
                    _ => (
                        EvaluationExecutionStatus::Inconclusive,
                        ValidationAssertionStatus::NotEvaluated,
                        run.summary.error.clone(),
                    ),
                };
                update_validation_attempt(state, attempt_id, |attempt| {
                    attempt.execution_status = execution_status;
                    attempt.assertion_status = assertion_status;
                    attempt.finished_at_ms = Some(now_ms());
                    attempt.error = error;
                    attempt.score = score;
                })?;
                persist_draft(state)?;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[cfg(test)]
    async fn wait_for_validation_before_start_test_hook(&self) {
        let validation_before_start_hook =
            { lock(&self.inner.promotion.validation_before_start_hook).take() };
        if let Some(hook) = validation_before_start_hook {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
    }

    async fn prepare_validation_replay(
        &self,
        state: &PromotionDraftState,
        revision: &EvaluationRevision,
        attempt_id: &str,
        cancel: &CancellationToken,
    ) -> Result<Option<RunSummary>, RunError> {
        if finish_cancelled_validation_if_requested(state, attempt_id, cancel)? {
            return Ok(None);
        }
        self.verify_validation_model_identity(revision)?;
        let snapshot = capture_confined_run_tree(
            &state.anchor,
            &PathBuf::from("revisions").join(&revision.id).join("source"),
        )?;
        verify_captured_source_revision(
            &snapshot,
            &revision.source.source_revision,
            "evaluation revision",
        )?;
        if finish_cancelled_validation_if_requested(state, attempt_id, cancel)? {
            return Ok(None);
        }
        self.prepare_promotion_run(revision, &snapshot, &revision.source.source_revision)
            .await
            .map(Some)
    }

    fn cancel_validation_before_start_if_requested(
        &self,
        state: &PromotionDraftState,
        attempt_id: &str,
        prepared_id: &str,
        cancel: &CancellationToken,
    ) -> Result<bool, RunError> {
        if !cancel.is_cancelled() {
            return Ok(false);
        }
        let cancellation = self.cancel(prepared_id);
        lock(&self.inner.scenario_overrides).remove(prepared_id);
        cancellation?;
        mark_validation_cancelled(state, attempt_id)?;
        Ok(true)
    }

    fn verify_validation_model_identity(
        &self,
        revision: &EvaluationRevision,
    ) -> Result<(), RunError> {
        let current_model_id = self
            .inner
            .harnesses
            .get(&revision.source.harness_id)
            .and_then(|harness| harness.models.get(&revision.source.model_profile_id))
            .ok_or_else(|| {
                RunError::InvalidRequest(format!(
                    "captured validation configuration is unavailable: harness={}, profile={}",
                    revision.source.harness_id, revision.source.model_profile_id
                ))
            })?;
        if current_model_id != &revision.source.model_id {
            return Err(RunError::InvalidRequest(format!(
                "captured validation model changed: expected {}, found {}",
                revision.source.model_id, current_model_id
            )));
        }
        Ok(())
    }

    async fn prepare_promotion_run(
        &self,
        revision: &EvaluationRevision,
        snapshot: &CapturedTree,
        source_revision: &str,
    ) -> Result<RunSummary, RunError> {
        let prepared = self
            .prepare_captured_snapshot_run(&revision.source.scenario_id, snapshot, source_revision)
            .await?;
        let scenario = scenario_for_revision(revision)?;
        lock(&self.inner.scenario_overrides).insert(prepared.id.clone(), scenario.clone());
        let state = self.state(&prepared.id)?;
        if let Err(error) = verify_capability_recipe(&state, &revision.capability_recipe) {
            lock(&self.inner.scenario_overrides).remove(&prepared.id);
            let _ = self.cancel(&prepared.id);
            return Err(error);
        }
        {
            let mut assembly = lock(&state.assembly);
            assembly.question.clone_from(&revision.task);
            assembly.limits = revision.limits.clone();
            assembly.scenario.output.clone_from(&scenario.output);
        }
        #[cfg(test)]
        let persist_result = if self
            .inner
            .promotion
            .fail_next_validation_assembly_persist
            .swap(false, Ordering::AcqRel)
        {
            Err(RunError::EvidencePersistence(
                "injected validation assembly persistence failure".to_owned(),
            ))
        } else {
            persist_assembly(&state)
        };
        #[cfg(not(test))]
        let persist_result = persist_assembly(&state);
        if let Err(error) = persist_result {
            lock(&self.inner.scenario_overrides).remove(&prepared.id);
            let cleanup = self.cancel(&prepared.id);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(RunError::EvidencePersistence(format!(
                    "{error}; prepared validation cleanup also failed: {cleanup}"
                ))),
            };
        }
        Ok(prepared)
    }
}

fn selected_turns_digest(
    turns: &[AgentTurnSummary],
    presentations: &[AgentTurnPresentation],
) -> Result<String, RunError> {
    let source = turns
        .iter()
        .zip(presentations)
        .map(|(turn, presentation)| {
            json!({
                "turnId": turn.id,
                "sourceDigest": presentation.source_digest,
            })
        })
        .collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&source)?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

fn catalog_score_is_complete(score: &JsonValue) -> bool {
    let Some(score) = score.as_object() else {
        return false;
    };
    [
        "passed",
        "outputPresent",
        "schemaValid",
        "expectedActiveNames",
        "expectedTotalScore",
        "expectedCapabilitySources",
        "capabilityEvidenceComplete",
        "catalogAnalysisComposed",
        "analysisResultMatches",
    ]
    .iter()
    .all(|field| score.contains_key(*field))
}

fn catalog_evaluator(scenario: &ScenarioManifest) -> EvaluationEvaluator {
    EvaluationEvaluator {
        id: CATALOG_EVALUATOR_ID.to_owned(),
        version: CATALOG_EVALUATOR_VERSION,
        parameters: CatalogEvaluatorParameters {
            active_names: scenario.assertions.active_names.clone(),
            total_score: scenario.assertions.total_score,
            required_capability_sources: scenario.assertions.required_capability_sources.clone(),
            output_path: scenario.output.clone(),
            require_schema: scenario.assertions.require_schema,
        },
    }
}

fn validate_revision(revision: &EvaluationRevision) -> Result<(), RunError> {
    if revision.schema_version != PROMOTION_SCHEMA_VERSION {
        return Err(RunError::InvalidRequest(format!(
            "unsupported evaluation revision schema: {}",
            revision.schema_version
        )));
    }
    if revision.task.trim().is_empty() {
        return Err(RunError::InvalidRequest(
            "evaluation task must not be empty".to_owned(),
        ));
    }
    if !revision.capability_recipe.is_empty() {
        let recipe = revision
            .capability_recipe
            .iter()
            .map(|capability| (capability.id.clone(), capability.revision.clone()))
            .collect::<BTreeMap<_, _>>();
        if recipe.len() != revision.capability_recipe.len()
            || recipe != revision.source.capability_revisions
        {
            return Err(RunError::InvalidRequest(format!(
                "evaluation capability recipe must match its captured capability revisions: recipe={recipe:?}, revisions={:?}",
                revision.source.capability_revisions
            )));
        }
    }
    if revision.evaluator.id != CATALOG_EVALUATOR_ID
        || revision.evaluator.version != CATALOG_EVALUATOR_VERSION
    {
        return Err(RunError::InvalidRequest(
            "this version supports only catalog-to-file@1".to_owned(),
        ));
    }
    let required_sources = &revision.evaluator.parameters.required_capability_sources;
    let unique_sources = required_sources.iter().collect::<BTreeSet<_>>();
    if required_sources.is_empty()
        || unique_sources.len() != required_sources.len()
        || required_sources
            .iter()
            .any(|source| !CATALOG_REQUIRED_SOURCES.contains(&source.as_str()))
    {
        return Err(RunError::InvalidRequest(
            "catalog-to-file@1 capability requirements must be a unique non-empty subset of catalog and analysis".to_owned(),
        ));
    }
    validate_workspace_relative_path(&revision.evaluator.parameters.output_path)?;
    if revision.limits.max_duration_ms == 0 {
        return Err(RunError::InvalidRequest(
            "evaluation duration limit must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

fn validate_workspace_relative_path(path: &Path) -> Result<(), RunError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(RunError::InvalidRequest(format!(
            "evaluation output path must be a confined relative path: {}",
            path.display()
        )));
    }
    Ok(())
}

fn validate_display_name(name: &str) -> Result<(), RunError> {
    let trimmed = name.trim();
    if trimmed.is_empty() || trimmed.len() > 120 {
        return Err(RunError::InvalidRequest(
            "evaluation name must contain 1 to 120 characters".to_owned(),
        ));
    }
    Ok(())
}

fn revision_status(revision: &EvaluationRevision) -> &'static str {
    if revision.blocking_issues.is_empty() {
        "ready"
    } else {
        "incomplete"
    }
}

fn current_draft_revision(
    detail: &EvaluationDraftDetail,
    draft_id: &str,
) -> Result<EvaluationRevision, RunError> {
    detail
        .revisions
        .iter()
        .find(|revision| revision.id == detail.summary.current_revision_id)
        .cloned()
        .ok_or_else(|| {
            RunError::EvidencePersistence(format!(
                "draft {draft_id} has no current evaluation revision"
            ))
        })
}

fn apply_saved_revision_status(
    detail: &mut EvaluationDraftDetail,
    draft_id: &str,
    revision_id: &str,
    definition_id: Option<String>,
) -> Result<(), RunError> {
    let is_current = detail.summary.current_revision_id == revision_id;
    detail.summary.saved = is_current;
    detail.summary.definition_id = definition_id;
    detail.summary.promoted_revision_id = detail
        .summary
        .definition_id
        .as_ref()
        .map(|_| revision_id.to_owned());
    if detail.summary.definition_id.is_some() && is_current {
        "promoted".clone_into(&mut detail.summary.status);
    } else if detail.summary.definition_id.is_none() && is_current {
        "saved-draft".clone_into(&mut detail.summary.status);
    } else {
        let current = detail
            .revisions
            .iter()
            .find(|revision| revision.id == detail.summary.current_revision_id)
            .ok_or_else(|| {
                RunError::EvidencePersistence(format!(
                    "draft {draft_id} has no current evaluation revision"
                ))
            })?;
        detail.summary.status = revision_status(current).to_owned();
    }
    Ok(())
}

fn scenario_for_revision(revision: &EvaluationRevision) -> Result<ScenarioManifest, RunError> {
    validate_revision(revision)?;
    Ok(ScenarioManifest {
        version: PROMOTION_SCHEMA_VERSION,
        id: revision.source.scenario_id.clone(),
        title: "Promoted evaluation".to_owned(),
        description: "Replayed from an interactive Agent Lab turn".to_owned(),
        question: revision.task.clone(),
        seed: PathBuf::new(),
        prompt: revision.task.clone(),
        output: revision.evaluator.parameters.output_path.clone(),
        limits: revision.limits.clone(),
        assertions: CatalogAssertions {
            active_names: revision.evaluator.parameters.active_names.clone(),
            total_score: revision.evaluator.parameters.total_score,
            required_capability_sources: revision
                .evaluator
                .parameters
                .required_capability_sources
                .clone(),
            require_schema: revision.evaluator.parameters.require_schema,
        },
    })
}

fn verify_captured_source_revision(
    snapshot: &CapturedTree,
    expected: &str,
    resource: &str,
) -> Result<(), RunError> {
    let actual = captured_tree_digest(snapshot);
    if actual != expected {
        return Err(RunError::EvidencePersistence(format!(
            "{resource} source snapshot digest mismatch: expected {expected}, found {actual}"
        )));
    }
    Ok(())
}

fn verify_capability_recipe(
    state: &RunState,
    expected: &[CapabilityAssembly],
) -> Result<(), RunError> {
    if expected.is_empty() {
        return Ok(());
    }
    let actual = lock(&state.assembly).capability_sources.clone();
    if actual != expected {
        return Err(RunError::EvidencePersistence(format!(
            "capability recipe mismatch: expected {}, found {}",
            serde_json::to_string(expected)?,
            serde_json::to_string(&actual)?
        )));
    }
    Ok(())
}

fn persist_draft(state: &PromotionDraftState) -> Result<(), RunError> {
    let detail = lock(&state.detail).clone();
    persist_draft_detail(state, &detail)
}

fn persist_draft_detail(
    state: &PromotionDraftState,
    detail: &EvaluationDraftDetail,
) -> Result<(), RunError> {
    if state.evidence_quarantined.load(Ordering::Acquire) {
        return Err(RunError::InvalidRequest(format!(
            "unknown evaluation draft: {}",
            detail.summary.id
        )));
    }
    write_confined_run_json_atomic(
        &state.anchor,
        Path::new("manifest.json"),
        &serde_json::to_value(detail)?,
    )
}

fn persist_draft_transition(
    state: &PromotionDraftState,
    previous_detail: &EvaluationDraftDetail,
    next_detail: &EvaluationDraftDetail,
    event: &RunEvent,
    created_revision_path: Option<&Path>,
) -> Result<(), RunError> {
    let mut event_line = serde_json::to_vec(event)?;
    event_line.push(b'\n');
    let cleanup_revision = || {
        if let Some(path) = created_revision_path {
            let _ = remove_confined_run_entry(&state.anchor, path);
        }
    };
    if let Err(error) = persist_draft_detail(state, next_detail) {
        cleanup_revision();
        return Err(error);
    }
    if let Err(error) =
        append_confined_run_bytes(&state.anchor, Path::new("events.jsonl"), &event_line)
    {
        let rollback = persist_draft_detail(state, previous_detail);
        cleanup_revision();
        return match rollback {
            Ok(()) => Err(error),
            Err(rollback) => Err(RunError::EvidencePersistence(format!(
                "{error}; restoring the previous draft also failed: {rollback}"
            ))),
        };
    }
    Ok(())
}

fn rollback_draft_transition(
    state: &PromotionDraftState,
    previous_detail: &EvaluationDraftDetail,
) -> Result<(), RunError> {
    persist_draft_detail(state, previous_detail)?;
    let mut events = Vec::new();
    for event in &previous_detail.events {
        serde_json::to_writer(&mut events, event)?;
        events.push(b'\n');
    }
    write_confined_run_bytes_atomic(&state.anchor, Path::new("events.jsonl"), &events)
}

struct PendingDefinitionPublication {
    detail: EvaluationDefinitionDetail,
    existing: Option<Arc<PromotionDefinitionState>>,
    source: Option<CapturedTree>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DefinitionPublicationTransaction {
    definition_id: String,
    draft_id: String,
    revision_id: String,
    committed: bool,
    previous_detail: Option<EvaluationDefinitionDetail>,
}

struct BegunDefinitionPublication {
    detail: EvaluationDefinitionDetail,
    anchor: Arc<AgentSessionDirectoryAnchor>,
    transaction: DefinitionPublicationTransaction,
}

fn prepare_definition_publication(
    draft: &PromotionDraftState,
    draft_id: &str,
    revision: EvaluationRevision,
    existing: Option<Arc<PromotionDefinitionState>>,
    name: &str,
) -> Result<PendingDefinitionPublication, RunError> {
    if let Some(existing) = existing {
        let mut detail = existing.detail.clone();
        name.clone_into(&mut detail.summary.name);
        return Ok(PendingDefinitionPublication {
            detail,
            existing: Some(existing),
            source: None,
        });
    }
    let definition_id = format!("definition-{}-{}", now_ms(), random_suffix());
    let source = capture_confined_run_tree(
        &draft.anchor,
        &PathBuf::from("revisions").join(&revision.id).join("source"),
    )?;
    Ok(PendingDefinitionPublication {
        detail: EvaluationDefinitionDetail {
            summary: EvaluationDefinitionSummary {
                id: definition_id,
                name: name.to_owned(),
                draft_id: draft_id.to_owned(),
                revision_id: revision.id.clone(),
                created_at_ms: now_ms(),
            },
            revision,
        },
        existing: None,
        source: Some(source),
    })
}

fn begin_definition_publication(
    store: &PromotionStore,
    publication: PendingDefinitionPublication,
) -> Result<BegunDefinitionPublication, RunError> {
    let definition_id = publication.detail.summary.id.clone();
    let previous_detail = publication
        .existing
        .as_ref()
        .map(|existing| existing.detail.clone());
    let anchor = if let Some(existing) = publication.existing {
        existing.anchor.clone()
    } else {
        let directory = confined_child(&store.definition_root(), &definition_id)?;
        fs::create_dir(&directory)?;
        let anchor = Arc::new(AgentSessionDirectoryAnchor::open(directory)?);
        write_confined_run_captured_tree(
            &anchor,
            Path::new("source"),
            &publication.source.ok_or_else(|| {
                RunError::EvidencePersistence(
                    "new evaluation definition has no source snapshot".to_owned(),
                )
            })?,
        )?;
        anchor
    };
    let secrets = lock(&store.secret_values).clone();
    if !secrets.is_empty()
        && confined_bundle_contains_protected_data(&anchor, &secrets).unwrap_or(true)
    {
        let _ = quarantine_run_bundle(&anchor, &definition_id);
        return Err(RunError::EvidencePersistence(
            PROTECTED_WORKSPACE_PATH_ERROR.to_owned(),
        ));
    }
    let transaction = DefinitionPublicationTransaction {
        definition_id,
        draft_id: publication.detail.summary.draft_id.clone(),
        revision_id: publication.detail.summary.revision_id.clone(),
        committed: false,
        previous_detail,
    };
    write_confined_run_json_atomic(
        &anchor,
        Path::new(DEFINITION_PUBLICATION_TRANSACTION),
        &serde_json::to_value(&transaction)?,
    )?;
    write_confined_run_json_atomic(
        &anchor,
        Path::new("manifest.json"),
        &serde_json::to_value(&publication.detail)?,
    )?;
    Ok(BegunDefinitionPublication {
        detail: publication.detail,
        anchor,
        transaction,
    })
}

fn commit_definition_publication(
    store: &PromotionStore,
    publication: &BegunDefinitionPublication,
) -> Result<(), RunError> {
    let known_secrets = lock(&store.secret_values);
    if !known_secrets.is_empty()
        && confined_bundle_contains_protected_data(&publication.anchor, &known_secrets)
            .unwrap_or(true)
    {
        return Err(RunError::EvidencePersistence(
            PROTECTED_WORKSPACE_PATH_ERROR.to_owned(),
        ));
    }
    let mut transaction = publication.transaction.clone();
    transaction.committed = true;
    write_confined_run_json_atomic(
        &publication.anchor,
        Path::new(DEFINITION_PUBLICATION_TRANSACTION),
        &serde_json::to_value(&transaction)?,
    )?;
    let _ = remove_confined_run_entry(
        &publication.anchor,
        Path::new(DEFINITION_PUBLICATION_TRANSACTION),
    );
    lock(&store.definitions).insert(
        publication.detail.summary.id.clone(),
        Arc::new(PromotionDefinitionState {
            detail: publication.detail.clone(),
            anchor: publication.anchor.clone(),
        }),
    );
    drop(known_secrets);
    Ok(())
}

fn rollback_definition_publication(
    store: &PromotionStore,
    publication: BegunDefinitionPublication,
) -> Result<(), RunError> {
    if let Some(previous_detail) = publication.transaction.previous_detail {
        write_confined_run_json_atomic(
            &publication.anchor,
            Path::new("manifest.json"),
            &serde_json::to_value(&previous_detail)?,
        )?;
        remove_confined_run_entry(
            &publication.anchor,
            Path::new(DEFINITION_PUBLICATION_TRANSACTION),
        )?;
    } else {
        lock(&store.definitions).remove(&publication.detail.summary.id);
        if !quarantine_run_bundle(&publication.anchor, &publication.detail.summary.id) {
            let _ = remove_confined_run_entry(&publication.anchor, Path::new("manifest.json"));
        }
    }
    Ok(())
}

fn rollback_definition_after_draft_failure(
    store: &PromotionStore,
    publication: Option<BegunDefinitionPublication>,
    error: RunError,
) -> Result<EvaluationDraftDetail, RunError> {
    let Some(publication) = publication else {
        return Err(error);
    };
    match rollback_definition_publication(store, publication) {
        Ok(()) => Err(error),
        Err(rollback) => Err(RunError::EvidencePersistence(format!(
            "{error}; definition rollback also failed: {rollback}"
        ))),
    }
}

fn combine_publication_rollback_errors(
    error: &RunError,
    draft_rollback: Result<(), RunError>,
    definition_rollback: Result<(), RunError>,
) -> Result<EvaluationDraftDetail, RunError> {
    let mut failures = vec![error.to_string()];
    if let Err(error) = draft_rollback {
        failures.push(format!("draft rollback failed: {error}"));
    }
    if let Err(error) = definition_rollback {
        failures.push(format!("definition rollback failed: {error}"));
    }
    Err(RunError::EvidencePersistence(failures.join("; ")))
}

fn record_draft_event(
    state: &PromotionDraftState,
    kind: &str,
    payload: JsonValue,
) -> Result<(), RunError> {
    if state.evidence_quarantined.load(Ordering::Acquire) {
        return Err(RunError::InvalidRequest(format!(
            "unknown evaluation draft: {}",
            lock(&state.detail).summary.id
        )));
    }
    let _commit = lock(&state.event_commit);
    let event = {
        let mut detail = lock(&state.detail);
        let event = RunEvent {
            sequence: detail.events.len() as u64 + 1,
            at_ms: now_ms(),
            kind: kind.to_owned(),
            payload,
            progress: None,
        };
        let mut line = serde_json::to_vec(&event)?;
        line.push(b'\n');
        append_confined_run_bytes(&state.anchor, Path::new("events.jsonl"), &line)?;
        detail.events.push(event.clone());
        event
    };
    persist_draft(state)?;
    let _ = state.sender.send(event);
    Ok(())
}

fn update_validation_attempt(
    state: &PromotionDraftState,
    attempt_id: &str,
    update: impl FnOnce(&mut EvaluationValidationAttempt),
) -> Result<(), RunError> {
    let mut detail = lock(&state.detail);
    let attempt = detail
        .validations
        .iter_mut()
        .find(|attempt| attempt.id == attempt_id)
        .ok_or_else(|| {
            RunError::InvalidRequest(format!("unknown evaluation validation: {attempt_id}"))
        })?;
    update(attempt);
    detail.summary.updated_at_ms = now_ms();
    Ok(())
}

fn mark_validation_cancelled(
    state: &PromotionDraftState,
    attempt_id: &str,
) -> Result<(), RunError> {
    update_validation_attempt(state, attempt_id, |attempt| {
        attempt.execution_status = EvaluationExecutionStatus::Cancelled;
        attempt.assertion_status = ValidationAssertionStatus::NotEvaluated;
        attempt.finished_at_ms = Some(now_ms());
        attempt.error = None;
        attempt.score = None;
    })?;
    persist_draft(state)
}

fn finish_cancelled_validation_if_requested(
    state: &PromotionDraftState,
    attempt_id: &str,
    cancel: &CancellationToken,
) -> Result<bool, RunError> {
    if !cancel.is_cancelled() {
        return Ok(false);
    }
    mark_validation_cancelled(state, attempt_id)?;
    Ok(true)
}

fn load_draft_event_evidence(
    anchor: &AgentSessionDirectoryAnchor,
    bundle: &Path,
) -> Result<Option<Vec<RunEvent>>, RunError> {
    let Some(source) = read_optional_confined_run_file(anchor, Path::new("events.jsonl"))? else {
        return Ok(None);
    };
    let events = match parse_events(&source) {
        Ok(events)
            if events
                .iter()
                .enumerate()
                .all(|(index, event)| event.sequence == index as u64 + 1) =>
        {
            events
        }
        Ok(_) => {
            tracing::warn!(
                bundle = %bundle.display(),
                "skipping evaluation draft with non-sequential event evidence"
            );
            return Err(RunError::EvidencePersistence(
                "evaluation draft event evidence is non-sequential".to_owned(),
            ));
        }
        Err(error) => {
            tracing::warn!(
                bundle = %bundle.display(),
                %error,
                "skipping evaluation draft with malformed event evidence"
            );
            return Err(RunError::EvidencePersistence(format!(
                "evaluation draft event evidence is malformed: {error}"
            )));
        }
    };
    Ok(Some(events))
}

fn load_drafts(root: &Path) -> Result<HashMap<String, Arc<PromotionDraftState>>, RunError> {
    let mut drafts = HashMap::new();
    for entry in fs::read_dir(root)? {
        let Ok(entry) = entry else {
            continue;
        };
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let anchor = match AgentSessionDirectoryAnchor::open(entry.path()) {
            Ok(anchor) => Arc::new(anchor),
            Err(error) => {
                tracing::warn!(bundle = %entry.path().display(), %error, "skipping unreadable evaluation draft");
                continue;
            }
        };
        if confined_run_quarantine_marker_exists(&anchor).unwrap_or(true)
            || confined_external_quarantine_tombstone_exists(&anchor).unwrap_or(true)
        {
            continue;
        }
        let Some(manifest) = read_optional_confined_run_file(&anchor, Path::new("manifest.json"))?
        else {
            continue;
        };
        let mut detail: EvaluationDraftDetail = match serde_json::from_slice(&manifest) {
            Ok(detail) => detail,
            Err(error) => {
                tracing::warn!(bundle = %entry.path().display(), %error, "skipping malformed evaluation draft");
                continue;
            }
        };
        if detail.summary.id != entry.file_name().to_string_lossy() {
            continue;
        }
        if detail.summary.promoted_revision_id.is_none()
            && detail.summary.status == "promoted"
            && detail.summary.definition_id.is_some()
        {
            detail.summary.promoted_revision_id = Some(detail.summary.current_revision_id.clone());
        }
        match load_draft_event_evidence(&anchor, &entry.path()) {
            Ok(Some(events)) => detail.events = events,
            Ok(None) => {}
            Err(_) => continue,
        }
        let snapshots_are_valid = detail.revisions.iter().all(|revision| {
            capture_confined_run_tree(
                &anchor,
                &PathBuf::from("revisions").join(&revision.id).join("source"),
            )
            .and_then(|snapshot| {
                verify_captured_source_revision(
                    &snapshot,
                    &revision.source.source_revision,
                    "evaluation revision",
                )
            })
            .is_ok()
        });
        if !snapshots_are_valid {
            tracing::warn!(bundle = %entry.path().display(), "skipping evaluation draft with missing or mismatched revision source");
            continue;
        }
        let mut recovered_validations = Vec::new();
        for validation in &mut detail.validations {
            if !validation.execution_status.is_finished() {
                validation.execution_status = EvaluationExecutionStatus::Inconclusive;
                validation.assertion_status = ValidationAssertionStatus::NotEvaluated;
                validation.finished_at_ms = Some(now_ms());
                validation.error =
                    Some("controller stopped before validation finalized".to_owned());
                recovered_validations.push(validation.clone());
            }
        }
        let (sender, _) = broadcast::channel(128);
        let state = Arc::new(PromotionDraftState {
            detail: Mutex::new(detail),
            anchor,
            sender,
            event_commit: Mutex::new(()),
            validation_cancels: Mutex::new(HashMap::new()),
            evidence_quarantined: AtomicBool::new(false),
        });
        if recovered_validations.is_empty() {
            persist_draft(&state)?;
        } else {
            for validation in recovered_validations {
                record_draft_event(
                    &state,
                    "evaluation-validation.finished",
                    serde_json::to_value(validation)?,
                )?;
            }
        }
        let id = lock(&state.detail).summary.id.clone();
        drafts.insert(id, state);
    }
    Ok(drafts)
}

fn load_definitions(
    root: &Path,
    drafts: &HashMap<String, Arc<PromotionDraftState>>,
) -> Result<HashMap<String, Arc<PromotionDefinitionState>>, RunError> {
    let mut definitions = HashMap::new();
    for entry in fs::read_dir(root)? {
        let Ok(entry) = entry else {
            continue;
        };
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let anchor = match AgentSessionDirectoryAnchor::open(entry.path()) {
            Ok(anchor) => Arc::new(anchor),
            Err(error) => {
                tracing::warn!(bundle = %entry.path().display(), %error, "skipping unreadable evaluation definition");
                continue;
            }
        };
        if confined_run_quarantine_marker_exists(&anchor).unwrap_or(true)
            || confined_external_quarantine_tombstone_exists(&anchor).unwrap_or(true)
        {
            continue;
        }
        let transaction = match read_optional_confined_run_file(
            &anchor,
            Path::new(DEFINITION_PUBLICATION_TRANSACTION),
        )? {
            Some(bytes) => match serde_json::from_slice::<DefinitionPublicationTransaction>(&bytes)
            {
                Ok(transaction) => Some(transaction),
                Err(error) => {
                    tracing::warn!(
                        bundle = %entry.path().display(),
                        %error,
                        "quarantining definition with malformed publication transaction"
                    );
                    let _ = quarantine_run_bundle(&anchor, &entry.file_name().to_string_lossy());
                    continue;
                }
            },
            None => None,
        };
        let Some(manifest) = read_optional_confined_run_file(&anchor, Path::new("manifest.json"))?
        else {
            continue;
        };
        let mut detail: EvaluationDefinitionDetail = match serde_json::from_slice(&manifest) {
            Ok(detail) => detail,
            Err(error) => {
                tracing::warn!(bundle = %entry.path().display(), %error, "skipping malformed evaluation definition");
                continue;
            }
        };
        if let Some(transaction) = transaction {
            let draft_committed = drafts
                .get(&transaction.draft_id)
                .map(|draft| lock(&draft.detail).summary.clone())
                .is_some_and(|draft| {
                    draft.definition_id.as_deref() == Some(&transaction.definition_id)
                        && draft.promoted_revision_id.as_deref() == Some(&transaction.revision_id)
                });
            if transaction.committed || draft_committed {
                let _ = remove_confined_run_entry(
                    &anchor,
                    Path::new(DEFINITION_PUBLICATION_TRANSACTION),
                );
            } else if let Some(previous_detail) = transaction.previous_detail {
                detail = previous_detail;
                write_confined_run_json_atomic(
                    &anchor,
                    Path::new("manifest.json"),
                    &serde_json::to_value(&detail)?,
                )?;
                remove_confined_run_entry(&anchor, Path::new(DEFINITION_PUBLICATION_TRANSACTION))?;
            } else {
                let _ = quarantine_run_bundle(&anchor, &entry.file_name().to_string_lossy());
                continue;
            }
        }
        let source_is_valid = capture_confined_run_tree(&anchor, Path::new("source"))
            .and_then(|snapshot| {
                verify_captured_source_revision(
                    &snapshot,
                    &detail.revision.source.source_revision,
                    "evaluation definition",
                )
            })
            .is_ok();
        if detail.summary.id != entry.file_name().to_string_lossy() || !source_is_valid {
            continue;
        }
        definitions.insert(
            detail.summary.id.clone(),
            Arc::new(PromotionDefinitionState { detail, anchor }),
        );
    }
    Ok(definitions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_evaluator_preserves_the_scenario_schema_requirement() {
        let scenario = ScenarioManifest {
            version: 1,
            id: "catalog".to_owned(),
            title: "Catalog".to_owned(),
            description: "test".to_owned(),
            question: "How does the harness produce the expected catalog artifact?".to_owned(),
            seed: "catalog/workspace".into(),
            prompt: "Write the catalog artifact".to_owned(),
            output: "result.json".into(),
            limits: ScenarioLimits {
                max_duration_ms: 1,
                max_command_count: 1,
                max_orchestrator_invocations: 1,
                max_tool_invocations: 1,
            },
            assertions: CatalogAssertions {
                active_names: vec!["alpha".to_owned(), "gamma".to_owned()],
                total_score: 11,
                required_capability_sources: vec!["catalog".to_owned()],
                require_schema: false,
            },
        };

        let evaluator = catalog_evaluator(&scenario);
        assert!(!evaluator.parameters.require_schema);
        assert_eq!(
            evaluator.parameters.required_capability_sources,
            vec!["catalog"]
        );
    }

    #[test]
    fn catalog_revision_rejects_unreviewed_evaluator_code() {
        let mut revision = EvaluationRevision {
            schema_version: PROMOTION_SCHEMA_VERSION,
            id: "revision-1".to_owned(),
            draft_id: "draft-1".to_owned(),
            previous_revision_id: None,
            created_at_ms: 1,
            task: "Write result.json".to_owned(),
            source: EvaluationSourceProvenance {
                workspace_id: "workspace-1".to_owned(),
                session_id: "session-1".to_owned(),
                turn_ids: vec!["turn-1".to_owned()],
                source_revision: "sha256:test".to_owned(),
                source_digest: "sha256:evidence".to_owned(),
                capability_revisions: BTreeMap::new(),
                source_event_sequences: Vec::new(),
                scenario_id: "catalog".to_owned(),
                harness_id: "v0".to_owned(),
                model_profile_id: "haiku".to_owned(),
                model_id: "provider/model".to_owned(),
            },
            capability_recipe: Vec::new(),
            limits: ScenarioLimits {
                max_duration_ms: 1,
                max_command_count: 1,
                max_orchestrator_invocations: 1,
                max_tool_invocations: 1,
            },
            evaluator: EvaluationEvaluator {
                id: "generated-code".to_owned(),
                version: 1,
                parameters: CatalogEvaluatorParameters {
                    active_names: Vec::new(),
                    total_score: 0,
                    required_capability_sources: vec!["catalog".to_owned(), "analysis".to_owned()],
                    output_path: "result.json".into(),
                    require_schema: true,
                },
            },
            measurements: Vec::new(),
            blocking_issues: Vec::new(),
        };
        assert!(validate_revision(&revision).is_err());
        revision.evaluator.id = CATALOG_EVALUATOR_ID.to_owned();
        assert!(validate_revision(&revision).is_ok());
        revision.limits.max_command_count = 0;
        revision.limits.max_orchestrator_invocations = 0;
        revision.limits.max_tool_invocations = 0;
        assert!(validate_revision(&revision).is_ok());
        revision.limits.max_duration_ms = 0;
        assert!(validate_revision(&revision).is_err());
    }

    #[test]
    fn catalog_revision_requires_capability_recipe_to_match_provenance() {
        let revision = EvaluationRevision {
            schema_version: PROMOTION_SCHEMA_VERSION,
            id: "revision-1".to_owned(),
            draft_id: "draft-1".to_owned(),
            previous_revision_id: None,
            created_at_ms: 1,
            task: "Write result.json".to_owned(),
            source: EvaluationSourceProvenance {
                workspace_id: "workspace-1".to_owned(),
                session_id: "session-1".to_owned(),
                turn_ids: vec!["turn-1".to_owned()],
                source_revision: "sha256:test".to_owned(),
                source_digest: "sha256:evidence".to_owned(),
                capability_revisions: BTreeMap::from([(
                    "catalog".to_owned(),
                    "catalog-v1".to_owned(),
                )]),
                source_event_sequences: Vec::new(),
                scenario_id: "catalog".to_owned(),
                harness_id: "v0".to_owned(),
                model_profile_id: "haiku".to_owned(),
                model_id: "provider/model".to_owned(),
            },
            capability_recipe: vec![CapabilityAssembly {
                id: "catalog".to_owned(),
                revision: "catalog-v2".to_owned(),
                protocol: "mcp-streamable-http".to_owned(),
                projections: vec!["nushell".to_owned(), "agent-mcp".to_owned()],
            }],
            limits: ScenarioLimits {
                max_duration_ms: 1,
                max_command_count: 1,
                max_orchestrator_invocations: 1,
                max_tool_invocations: 1,
            },
            evaluator: EvaluationEvaluator {
                id: CATALOG_EVALUATOR_ID.to_owned(),
                version: CATALOG_EVALUATOR_VERSION,
                parameters: CatalogEvaluatorParameters {
                    active_names: Vec::new(),
                    total_score: 0,
                    required_capability_sources: vec!["catalog".to_owned()],
                    output_path: "result.json".into(),
                    require_schema: true,
                },
            },
            measurements: Vec::new(),
            blocking_issues: Vec::new(),
        };

        assert!(
            validate_revision(&revision)
                .unwrap_err()
                .to_string()
                .contains("capability recipe")
        );
    }
}
