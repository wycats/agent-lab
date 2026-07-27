#![allow(clippy::wildcard_imports)]

use std::collections::BTreeSet;

use super::*;

const PROMOTION_SCHEMA_VERSION: u32 = 1;
const CATALOG_EVALUATOR_ID: &str = "catalog-to-file";
const CATALOG_EVALUATOR_VERSION: u32 = 1;
const DEFINITION_PUBLICATION_TRANSACTION: &str = "publication.pending.json";
pub(super) const MAX_EVALUATION_DRAFT_REVISIONS: usize = 32;
pub(super) const MAX_EVALUATION_VALIDATION_ATTEMPTS: usize = 32;
const MANUAL_AUTHORING_BLOCKER: &str =
    "review and confirm the suggested task, assertions, and measurements";
const PROPOSAL_SCHEMA_VERSION: u32 = 1;
const PROPOSAL_PROMPT_CONTRACT: &str = "agent-lab/evaluation-proposal@1";
const PROPOSAL_MIN_DURATION_MS: u64 = 30_000;
const PROPOSAL_MEASUREMENTS: &[&str] = &[
    "duration",
    "model-turns",
    "capability-calls",
    "workspace-effects",
    "reported-usage",
];

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver: Option<EvaluationSourceDriverIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal: Option<EvaluationProposalProvenance>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationSourceDriverIdentity {
    pub descriptor: DriverDescriptor,
    pub launch_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationProposalProvenance {
    pub proposal_id: String,
    pub harness_id: String,
    pub model_profile_id: String,
    pub model_id: String,
    pub prompt_contract: String,
    #[serde(default)]
    pub rationale: String,
    pub source_event_sequences: Vec<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver: Option<EvaluationSourceDriverIdentity>,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EvaluationProposalStatus {
    Queued,
    Running,
    Complete,
    Failed,
    Cancelled,
}

impl EvaluationProposalStatus {
    #[must_use]
    pub fn is_finished(self) -> bool {
        matches!(self, Self::Complete | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct StartEvaluationProposalRequest {
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub from_turn_id: Option<String>,
    #[serde(default)]
    pub through_turn_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationProposalCandidate {
    pub schema_version: u32,
    pub from_turn_id: String,
    pub through_turn_id: String,
    pub task: String,
    pub evaluator: EvaluationEvaluator,
    pub measurements: Vec<String>,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationProposalSummary {
    pub id: String,
    pub workspace_id: String,
    pub session_id: String,
    pub harness_id: String,
    pub model_profile_id: String,
    pub model_id: String,
    pub status: EvaluationProposalStatus,
    pub draft_id: Option<String>,
    pub created_at_ms: u128,
    pub finished_at_ms: Option<u128>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationProposalDetail {
    pub summary: EvaluationProposalSummary,
    pub requested_from_turn_id: Option<String>,
    pub requested_through_turn_id: Option<String>,
    pub candidate: Option<EvaluationProposalCandidate>,
    pub events: Vec<RunEvent>,
}

pub(super) struct PromotionStore {
    root: PathBuf,
    drafts: Mutex<HashMap<String, Arc<PromotionDraftState>>>,
    definitions: Mutex<HashMap<String, Arc<PromotionDefinitionState>>>,
    proposals: Mutex<HashMap<String, Arc<PromotionProposalState>>>,
    secret_values: Mutex<Vec<Vec<u8>>>,
    evidence_lifecycle: Mutex<()>,
    #[cfg(test)]
    fail_next_validation_assembly_persist: AtomicBool,
    #[cfg(test)]
    fail_next_validation_finalization_persist: AtomicBool,
    #[cfg(test)]
    fail_next_validation_fallback_persist: AtomicBool,
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

struct PromotionProposalState {
    detail: Mutex<EvaluationProposalDetail>,
    anchor: Arc<AgentSessionDirectoryAnchor>,
    sender: broadcast::Sender<RunEvent>,
    event_commit: Mutex<()>,
    completion_commit: Mutex<()>,
    cancel: CancellationToken,
    evidence_quarantined: AtomicBool,
    #[cfg(test)]
    fail_next_terminal_event_persist: AtomicBool,
}

struct ProposalExecution {
    workspace: Arc<RunState>,
    session: Arc<AgentSessionState>,
    harness: HarnessProfile,
    model_access_provider: Option<ModelAccessProvider>,
    limits: ScenarioLimits,
    evaluator: EvaluationEvaluator,
    source_turns: Vec<AgentTurnSummary>,
    turn_task: JsonValue,
    origin: WorkbenchOrigin,
}

impl PromotionStore {
    pub(super) fn load(data_dir: &Path) -> Result<Self, RunError> {
        let root = data_dir.join("evaluation-library");
        fs::create_dir_all(root.join("drafts"))?;
        fs::create_dir_all(root.join("definitions"))?;
        fs::create_dir_all(root.join("proposals"))?;
        let root = fs::canonicalize(root)?;
        let drafts = load_drafts(&root.join("drafts"))?;
        let definitions = load_definitions(&root.join("definitions"), &drafts)?;
        let proposals = load_proposals(&root.join("proposals"), &drafts)?;
        Ok(Self {
            root,
            drafts: Mutex::new(drafts),
            definitions: Mutex::new(definitions),
            proposals: Mutex::new(proposals),
            secret_values: Mutex::new(Vec::new()),
            evidence_lifecycle: Mutex::new(()),
            #[cfg(test)]
            fail_next_validation_assembly_persist: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_validation_finalization_persist: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_validation_fallback_persist: AtomicBool::new(false),
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

    fn proposal_root(&self) -> PathBuf {
        self.root.join("proposals")
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
        let proposals = lock(&self.proposals).values().cloned().collect::<Vec<_>>();
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
        for state in proposals {
            if confined_bundle_contains_protected_data(&state.anchor, &all_secrets).unwrap_or(true)
            {
                contaminated = true;
                state.evidence_quarantined.store(true, Ordering::Release);
                state.cancel.cancel();
                let id = lock(&state.detail).summary.id.clone();
                if !quarantine_run_bundle(&state.anchor, &id) {
                    let _ = remove_confined_run_entry(&state.anchor, Path::new("manifest.json"));
                }
                lock(&self.proposals).remove(&id);
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

    /// List proposal sessions in most-recently-created order.
    #[must_use]
    pub fn list_evaluation_proposals(&self) -> Vec<EvaluationProposalSummary> {
        let mut proposals = lock(&self.inner.promotion.proposals)
            .values()
            .map(|state| lock(&state.detail).summary.clone())
            .collect::<Vec<_>>();
        proposals.sort_by_key(|proposal| std::cmp::Reverse(proposal.created_at_ms));
        proposals
    }

    /// Return one durable proposal session.
    ///
    /// # Errors
    ///
    /// Returns an error when the proposal is unknown.
    pub fn evaluation_proposal(&self, id: &str) -> Result<EvaluationProposalDetail, RunError> {
        Ok(lock(&self.promotion_proposal_state(id)?.detail).clone())
    }

    /// Start a read-only, operation-scoped agent session that suggests an evaluation draft.
    ///
    /// # Errors
    ///
    /// Returns an error when the workspace, source session, requested turn span, or harness
    /// configuration is unavailable.
    #[allow(clippy::too_many_lines)]
    pub fn start_evaluation_proposal(
        &self,
        workspace_id: &str,
        request: StartEvaluationProposalRequest,
        origin: WorkbenchOrigin,
    ) -> Result<EvaluationProposalSummary, RunError> {
        if request.from_turn_id.is_some() != request.through_turn_id.is_some() {
            return Err(RunError::InvalidRequest(
                "--from and --through must be supplied together".to_owned(),
            ));
        }
        let workspace = self.state(workspace_id)?;
        // Proposal startup can discover new credential material and reserves the workspace against
        // overlapping turns. Hold the same gates used by session and turn startup until the
        // proposal is registered as both an active producer and a pending secret resolution.
        let mut pending_secret_resolutions = lock(&workspace.pending_secret_resolutions);
        let _producer_lifecycle = lock(&workspace.producer_lifecycle);
        if !pending_secret_resolutions.is_empty() {
            return Err(RunError::RunUnavailable(
                "wait for model access resolution to finish".to_owned(),
            ));
        }
        if lock(&workspace.summary).status != RunStatus::Exploring {
            return Err(RunError::RunUnavailable(workspace_id.to_owned()));
        }
        if lock(&workspace.active_agent_turn).is_some() {
            return Err(RunError::InvalidRequest(
                "finish or cancel the active agent turn before proposing an evaluation".to_owned(),
            ));
        }
        let _evidence_lifecycle = lock(&self.inner.promotion.evidence_lifecycle);
        if lock(&self.inner.promotion.proposals)
            .values()
            .any(|proposal| {
                let summary = lock(&proposal.detail).summary.clone();
                summary.workspace_id == workspace_id && !summary.status.is_finished()
            })
        {
            return Err(RunError::InvalidRequest(
                "finish or cancel the active evaluation proposal before starting another"
                    .to_owned(),
            ));
        }
        let session_id = request
            .session_id
            .clone()
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
        let scenario_id = lock(&workspace.summary).scenario_id.clone();
        let scenario = self
            .inner
            .scenarios
            .get(&scenario_id)
            .cloned()
            .ok_or_else(|| RunError::UnknownScenario(scenario_id.clone()))?;
        let turns = lock(&session.turns).clone();
        let terminal_turns = turns
            .iter()
            .filter(|turn| turn.status.is_finished())
            .cloned()
            .collect::<Vec<_>>();
        if terminal_turns.is_empty() {
            return Err(RunError::InvalidRequest(
                "proposal sessions require at least one terminal agent turn".to_owned(),
            ));
        }
        let evidence_turns = if let (Some(from), Some(through)) = (
            request.from_turn_id.as_deref(),
            request.through_turn_id.as_deref(),
        ) {
            validate_requested_proposal_span(&turns, from, through)?;
            let from = turns
                .iter()
                .position(|turn| turn.id == from)
                .ok_or_else(|| RunError::InvalidRequest(format!("unknown source turn: {from}")))?;
            let through = turns
                .iter()
                .position(|turn| turn.id == through)
                .ok_or_else(|| {
                    RunError::InvalidRequest(format!("unknown source turn: {through}"))
                })?;
            let selected = turns[from..=through].to_vec();
            validate_coherent_proposal_span(&selected).map_err(RunError::InvalidRequest)?;
            selected
        } else {
            terminal_turns
        };
        let source_turns = evidence_turns
            .iter()
            .map(|turn| {
                let presentation =
                    load_or_build_agent_turn_presentation(&session, &workspace, turn)?;
                Ok(json!({
                    "id": turn.id,
                    "prompt": turn.prompt,
                    "input": turn.input,
                    "sourceRevision": turn.source_revision,
                    "capabilityRevisions": turn.capability_revisions,
                    "status": turn.status,
                    "outcome": turn.outcome,
                    "response": presentation.response,
                    "activity": presentation.activity,
                    "usage": presentation.usage,
                    "sourceEventSequences": presentation.source_event_sequences,
                    "sourceDigest": presentation.source_digest,
                }))
            })
            .collect::<Result<Vec<_>, RunError>>()?;
        let evaluator = catalog_evaluator(&scenario);
        let source_input = json!({
            "schemaVersion": PROPOSAL_SCHEMA_VERSION,
            "promptContract": PROPOSAL_PROMPT_CONTRACT,
            "requestedSpan": {
                "fromTurnId": request.from_turn_id,
                "throughTurnId": request.through_turn_id,
            },
            "evaluator": evaluator,
            "turns": source_turns,
        });
        if serde_json::to_vec(&source_input)?.len() > MAX_AGENT_TURN_INPUT_BYTES {
            return Err(RunError::InvalidRequest(format!(
                "proposal source evidence exceeds the {MAX_AGENT_TURN_INPUT_BYTES} byte input limit"
            )));
        }
        let id = format!("proposal-{}-{}", now_ms(), random_suffix());
        let turn_task = json!({
            "mode": "evaluation-proposal",
            "promptContract": PROPOSAL_PROMPT_CONTRACT,
            "prompt": proposal_prompt(),
            "input": source_input,
        });
        validate_proposal_turn_command_size(&id, &turn_task)?;
        let harness = self
            .inner
            .harnesses
            .get(&session_summary.harness_id)
            .cloned()
            .ok_or_else(|| {
                RunError::InvalidRequest(format!(
                    "unknown source harness: {}",
                    session_summary.harness_id
                ))
            })?;
        let model_access_provider = self.model_access_provider_for_harness(&harness)?.cloned();
        let mut limits = scenario.limits;
        limits.max_duration_ms = limits.max_duration_ms.max(PROPOSAL_MIN_DURATION_MS);
        let directory = confined_child(&self.inner.promotion.proposal_root(), &id)?;
        fs::create_dir(&directory)?;
        fs::create_dir(directory.join("workspace"))?;
        let anchor = match AgentSessionDirectoryAnchor::open(directory.clone()) {
            Ok(anchor) => Arc::new(anchor),
            Err(error) => {
                let _ = fs::remove_dir_all(&directory);
                return Err(error);
            }
        };
        let pending_bundle = PendingEvaluationBundle::new(id.clone(), anchor.clone());
        let summary = EvaluationProposalSummary {
            id: id.clone(),
            workspace_id: workspace_id.to_owned(),
            session_id: session_summary.id.clone(),
            harness_id: session_summary.harness_id.clone(),
            model_profile_id: session_summary.model_profile_id.clone(),
            model_id: session_summary.model_id.clone(),
            status: EvaluationProposalStatus::Queued,
            draft_id: None,
            created_at_ms: now_ms(),
            finished_at_ms: None,
            error: None,
        };
        let detail = EvaluationProposalDetail {
            summary: summary.clone(),
            requested_from_turn_id: request.from_turn_id,
            requested_through_turn_id: request.through_turn_id,
            candidate: None,
            events: Vec::new(),
        };
        let known_secrets = lock(&self.inner.promotion.secret_values);
        reject_serialized_protected_data(&detail, &known_secrets)?;
        reject_serialized_protected_data(&source_input, &known_secrets)?;
        let (sender, _) = broadcast::channel(128);
        let state = Arc::new(PromotionProposalState {
            detail: Mutex::new(detail),
            anchor,
            sender,
            event_commit: Mutex::new(()),
            completion_commit: Mutex::new(()),
            cancel: CancellationToken::new(),
            evidence_quarantined: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_terminal_event_persist: AtomicBool::new(false),
        });
        persist_proposal(&state)?;
        write_confined_run_json_atomic(&state.anchor, Path::new("source.json"), &source_input)?;
        record_proposal_event(
            &state,
            "evaluation-proposal.created",
            json!({
                "proposalId": id,
                "workspaceId": workspace_id,
                "sessionId": session_summary.id,
                "origin": origin,
            }),
        )?;
        lock(&self.inner.promotion.proposals).insert(id.clone(), state.clone());
        drop(known_secrets);
        if let Err(error) = record_event(
            &workspace,
            "workbench.evaluation-proposal.started",
            json!({
                "origin": origin,
                "proposalId": id,
                "proposal": summary,
            }),
        ) {
            lock(&self.inner.promotion.proposals).remove(&id);
            return Err(error);
        }
        let event_workspace = workspace.clone();
        let event_origin = origin;
        let execution = ProposalExecution {
            workspace: workspace.clone(),
            session,
            harness,
            model_access_provider,
            limits,
            evaluator,
            source_turns: evidence_turns,
            turn_task,
            origin,
        };
        let controller = self.clone();
        let actor_state = state.clone();
        pending_secret_resolutions.insert(id.clone());
        let spawn = thread::Builder::new()
            .name(format!("agent-lab-proposal-{id}"))
            .spawn(move || controller.execute_evaluation_proposal(&actor_state, &execution));
        if let Err(error) = spawn {
            pending_secret_resolutions.remove(&id);
            let message = format!("proposal agent could not start: {error}");
            finish_proposal(
                &state,
                EvaluationProposalStatus::Failed,
                None,
                None,
                Some(&message),
            )?;
            let _ = record_event(
                &event_workspace,
                "workbench.evaluation-proposal.finished",
                json!({
                    "origin": event_origin,
                    "proposalId": id,
                    "draftId": JsonValue::Null,
                    "status": EvaluationProposalStatus::Failed,
                    "error": message,
                }),
            );
            pending_bundle.commit();
            return Err(RunError::Io(error));
        }
        pending_bundle.commit();
        drop(pending_secret_resolutions);
        Ok(summary)
    }

    /// Cancel an active proposal session while retaining its evidence.
    ///
    /// # Errors
    ///
    /// Returns an error when the proposal is unknown or already terminal.
    pub fn cancel_evaluation_proposal(&self, id: &str) -> Result<(), RunError> {
        let state = self.promotion_proposal_state(id)?;
        let _completion = lock(&state.completion_commit);
        if lock(&state.detail).summary.status.is_finished() {
            return Err(RunError::InvalidRequest(format!(
                "evaluation proposal is already complete: {id}"
            )));
        }
        state.cancel.cancel();
        Ok(())
    }

    /// Subscribe to durable and live events for one proposal session.
    ///
    /// # Errors
    ///
    /// Returns an error when the proposal is unknown.
    pub fn subscribe_evaluation_proposal(
        &self,
        id: &str,
    ) -> Result<(Vec<RunEvent>, broadcast::Receiver<RunEvent>), RunError> {
        let state = self.promotion_proposal_state(id)?;
        let _commit = lock(&state.event_commit);
        let detail = lock(&state.detail);
        let receiver = state.sender.subscribe();
        Ok((detail.events.clone(), receiver))
    }

    /// Return proposal events after an acknowledged durable sequence.
    ///
    /// # Errors
    ///
    /// Returns an error when the proposal is unknown.
    pub fn evaluation_proposal_events_after(
        &self,
        id: &str,
        sequence: u64,
    ) -> Result<Vec<RunEvent>, RunError> {
        let state = self.promotion_proposal_state(id)?;
        Ok(lock(&state.detail)
            .events
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect())
    }

    #[allow(clippy::too_many_lines)]
    fn execute_evaluation_proposal(
        &self,
        state: &Arc<PromotionProposalState>,
        execution: &ProposalExecution,
    ) {
        let proposal_id = lock(&state.detail).summary.id.clone();
        let session_id = format!("proposal-session-{proposal_id}");
        let turn_id = format!("proposal-turn-{proposal_id}");
        let mut pending_resolution =
            PendingSecretResolutionGuard::new(&execution.workspace, &proposal_id);
        let result = (|| -> Result<(EvaluationProposalCandidate, EvaluationSourceDriverIdentity), RunError> {
            update_proposal_status(state, EvaluationProposalStatus::Running, None)?;
            record_proposal_event(
                state,
                "evaluation-proposal.started",
                json!({ "proposalId": proposal_id }),
            )?;
            let driver_launch = resolve_harness_driver_with_cancellation(
                &execution.harness,
                execution.model_access_provider.as_ref(),
                &state.cancel,
            )?;
            if state.cancel.is_cancelled() {
                return Err(RunError::RunUnavailable(
                    "evaluation proposal was cancelled".to_owned(),
                ));
            }
            let resolved_secrets = driver_secret_values(&driver_launch);
            let secrets =
                extend_workspace_secret_values(&execution.workspace, resolved_secrets);
            extend_secret_values(
                &self.inner.promotion.secret_values,
                secrets.iter().cloned(),
            );
            quarantine_protected_bundle_paths(&execution.workspace, &secrets)?;
            invalidate_contaminated_secret_evidence(
                &self.inner.runs,
                &self.inner.evaluations,
                &self.inner.promotion,
                &execution.workspace,
                &secrets,
            )?;
            pending_resolution.complete();

            let launch_digest = driver_launch_digest(&driver_launch)?;
            let initial_scratch =
                capture_confined_run_tree(&state.anchor, Path::new("workspace"))?;
            let mut driver = DriverProcess::spawn_with(driver_launch)?;
            let driver_result = (|| -> Result<
                (EvaluationProposalCandidate, EvaluationSourceDriverIdentity),
                RunError,
            > {
                let ready_deadline = Instant::now() + DRIVER_READY_TIMEOUT;
                let descriptor = loop {
                    let message =
                        receive_until_deadline(&mut driver, ready_deadline, &state.cancel)?
                            .ok_or_else(|| {
                                RunError::RunUnavailable(
                                    "evaluation proposal was cancelled".to_owned(),
                                )
                            })?;
                    match message.parsed.body {
                        DriverBody::StartupEvent {
                            phase,
                            status,
                            detail,
                        } => {
                            record_proposal_event(
                                state,
                                "startup.event",
                                redact_value(
                                    json!({ "phase": phase, "status": status, "detail": detail }),
                                    &secrets,
                                ),
                            )?;
                        }
                        DriverBody::Ready { driver } => {
                            break redact_driver_descriptor(driver, &secrets);
                        }
                        DriverBody::Failed { code, message, .. } => {
                            return Err(RunError::Protocol(format!(
                                "proposal driver failed during startup: {code}: {message}"
                            )));
                        }
                        _ => {
                            return Err(RunError::Protocol(
                                "expected startup.event or driver.ready for evaluation proposal"
                                    .to_owned(),
                            ));
                        }
                    }
                };
                let supports_turn_observations = descriptor
                    .features
                    .iter()
                    .any(|feature| feature == TURN_OBSERVATIONS_FEATURE);
                let scratch = state.anchor.display_path.join("workspace");
                driver.send(&command(
                    "proposal-open",
                    CommandBody::OpenSession {
                        session_id: session_id.clone(),
                        config: json!({
                            "files": {},
                            "modelId": lock(&state.detail).summary.model_id,
                            "workspaceRoot": scratch,
                            "capabilitySources": [],
                            "readOnly": true,
                        }),
                        limits: serde_json::to_value(&execution.limits)?,
                    },
                ))?;
                let open_deadline = Instant::now() + DRIVER_RESPONSE_TIMEOUT;
                loop {
                    let message =
                        receive_until_deadline(&mut driver, open_deadline, &state.cancel)?
                            .ok_or_else(|| {
                                RunError::RunUnavailable(
                                    "evaluation proposal was cancelled".to_owned(),
                                )
                            })?;
                    match message.parsed.body {
                        DriverBody::StartupEvent {
                            phase,
                            status,
                            detail,
                        } => {
                            record_proposal_event(
                                state,
                                "startup.event",
                                redact_value(
                                    json!({ "phase": phase, "status": status, "detail": detail }),
                                    &secrets,
                                ),
                            )?;
                        }
                        DriverBody::SessionOpened {
                            session_id: opened,
                            process_id,
                        } if opened == session_id => {
                            record_proposal_event(
                                state,
                                "evaluation-proposal.session.ready",
                                json!({
                                    "proposalId": proposal_id,
                                    "processId": process_id,
                                    "driver": descriptor,
                                }),
                            )?;
                            break;
                        }
                        DriverBody::Failed { code, message, .. } => {
                            return Err(RunError::Protocol(format!(
                                "proposal driver failed while opening: {code}: {message}"
                            )));
                        }
                        _ => {
                            return Err(RunError::Protocol(
                                "expected startup.event or session.opened for evaluation proposal"
                                    .to_owned(),
                            ));
                        }
                    }
                }
                driver.send(&command(
                    "proposal-start",
                    CommandBody::StartTurn {
                        session_id: session_id.clone(),
                        turn_id: turn_id.clone(),
                        task: execution.turn_task.clone(),
                        capability_sources: json!([]),
                    },
                ))?;
                record_proposal_event(
                    state,
                    "evaluation-proposal.turn.started",
                    json!({ "proposalId": proposal_id, "turnId": turn_id }),
                )?;
                let started = Instant::now();
                let mut abort_sent_at = None;
                let mut assistant_redactor = AssistantObservationRedactor::new(&secrets);
                let mut response = None;
                loop {
                    if state.cancel.is_cancelled() && abort_sent_at.is_none() {
                        driver.send(&command(
                            "proposal-abort",
                            CommandBody::AbortTurn {
                                session_id: session_id.clone(),
                                turn_id: turn_id.clone(),
                                reason: Some("cancelled from Agent Lab".to_owned()),
                            },
                        ))?;
                        abort_sent_at = Some(Instant::now());
                    }
                    if started.elapsed() >= Duration::from_millis(execution.limits.max_duration_ms)
                        && abort_sent_at.is_none()
                    {
                        driver.send(&command(
                            "proposal-timeout",
                            CommandBody::AbortTurn {
                                session_id: session_id.clone(),
                                turn_id: turn_id.clone(),
                                reason: Some(
                                    "evaluation proposal duration limit exceeded".to_owned(),
                                ),
                            },
                        ))?;
                        abort_sent_at = Some(Instant::now());
                    }
                    if abort_sent_at.is_some_and(|sent| sent.elapsed() >= Duration::from_secs(10)) {
                        return Err(RunError::Protocol(
                            "proposal driver did not finish within 10 seconds of abort".to_owned(),
                        ));
                    }
                    match driver.receive(DRIVER_POLL) {
                        Ok(message) => match message.parsed.body {
                            DriverBody::StartupEvent {
                                phase,
                                status,
                                detail,
                            } => {
                                record_proposal_event(
                                    state,
                                    "startup.event",
                                    redact_value(
                                        json!({ "phase": phase, "status": status, "detail": detail }),
                                        &secrets,
                                    ),
                                )?;
                            }
                            DriverBody::TurnEvent {
                                session_id: observed_session,
                                turn_id: observed_turn,
                                event_type,
                                payload,
                            } => {
                                validate_turn_identity(
                                    &observed_session,
                                    &observed_turn,
                                    &session_id,
                                    &turn_id,
                                    "turn.event",
                                )?;
                                let observation = TurnObservation::parse(&event_type, &payload)
                                    .map_err(|error| RunError::Protocol(error.to_string()))?;
                                if event_type.starts_with("observation.") {
                                    if !supports_turn_observations {
                                        return Err(RunError::Protocol(format!(
                                            "proposal driver emitted {event_type} without advertising {TURN_OBSERVATIONS_FEATURE}"
                                        )));
                                    }
                                    if observation.is_none() {
                                        return Err(RunError::Protocol(format!(
                                            "unknown reserved proposal observation: {event_type}"
                                        )));
                                    }
                                }
                                if let Some(observation) = observation {
                                    for observation in assistant_redactor.redact(observation)? {
                                        if let TurnObservation::AssistantCompleted(completed) =
                                            &observation
                                        {
                                            response = Some(completed.text.clone());
                                        }
                                        record_proposal_event(
                                            state,
                                            observation.event_type(),
                                            json!({
                                                "proposalId": proposal_id,
                                                "turnId": turn_id,
                                                "event": observation.payload(),
                                            }),
                                        )?;
                                    }
                                } else {
                                    record_proposal_event(
                                        state,
                                        &driver_event_kind(&event_type),
                                        redact_value(
                                            json!({
                                                "proposalId": proposal_id,
                                                "turnId": turn_id,
                                                "event": payload,
                                            }),
                                            &secrets,
                                        ),
                                    )?;
                                }
                            }
                            DriverBody::TurnFinished {
                                session_id: observed_session,
                                turn_id: observed_turn,
                                outcome,
                                evidence,
                            } => {
                                validate_turn_identity(
                                    &observed_session,
                                    &observed_turn,
                                    &session_id,
                                    &turn_id,
                                    "turn.finished",
                                )?;
                                for observation in assistant_redactor.flush_incomplete() {
                                    record_proposal_event(
                                        state,
                                        observation.event_type(),
                                        json!({
                                            "proposalId": proposal_id,
                                            "turnId": turn_id,
                                            "event": observation.payload(),
                                        }),
                                    )?;
                                }
                                record_proposal_event(
                                    state,
                                    "evaluation-proposal.turn.finished",
                                    redact_value(
                                        json!({
                                            "proposalId": proposal_id,
                                            "turnId": turn_id,
                                            "outcome": outcome,
                                            "evidence": evidence,
                                        }),
                                        &secrets,
                                    ),
                                )?;
                                if state.cancel.is_cancelled() || outcome == "aborted" {
                                    return Err(RunError::RunUnavailable(
                                        "evaluation proposal was cancelled".to_owned(),
                                    ));
                                }
                                if outcome != "completed" {
                                    return Err(RunError::Protocol(format!(
                                        "proposal driver finished with outcome {outcome}"
                                    )));
                                }
                                break;
                            }
                            DriverBody::Failed { code, message, .. } => {
                                return Err(RunError::Protocol(format!(
                                    "proposal driver failed during turn: {code}: {message}"
                                )));
                            }
                            _ => {
                                return Err(RunError::Protocol(
                                    "unexpected proposal driver message".to_owned(),
                                ));
                            }
                        },
                        Err(ProcessError::Timeout) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                driver.send(&command(
                    "proposal-close",
                    CommandBody::CloseSession {
                        session_id: session_id.clone(),
                    },
                ))?;
                let closed = driver.receive(DRIVER_RESPONSE_TIMEOUT)?;
                if !matches!(closed.parsed.body, DriverBody::SessionClosed { session_id: ref closed } if closed == &session_id)
                {
                    return Err(RunError::Protocol(
                        "expected session.closed for evaluation proposal".to_owned(),
                    ));
                }
                require_successful_driver_exit(driver.wait_for_exit(DRIVER_RESPONSE_TIMEOUT)?)?;
                let final_scratch =
                    capture_confined_run_tree(&state.anchor, Path::new("workspace"))?;
                let scratch_changes =
                    captured_tree_changes(&initial_scratch, &final_scratch, &secrets);
                if !scratch_changes.is_empty() {
                    return Err(RunError::Protocol(format!(
                        "read-only proposal agent changed its scratch workspace: {}",
                        serde_json::to_string(&scratch_changes)?
                    )));
                }
                let response = response.ok_or_else(|| {
                    RunError::Protocol(
                        "evaluation proposal completed without an authoritative response"
                            .to_owned(),
                    )
                })?;
                let candidate: EvaluationProposalCandidate = serde_json::from_str(&response)
                    .map_err(|error| {
                        RunError::Protocol(format!(
                            "evaluation proposal returned invalid JSON: {error}"
                        ))
                    })?;
                let (requested_from, requested_through) = {
                    let detail = lock(&state.detail);
                    (
                        detail.requested_from_turn_id.clone(),
                        detail.requested_through_turn_id.clone(),
                    )
                };
                validate_proposal_candidate(
                    &candidate,
                    &execution.source_turns,
                    &execution.evaluator,
                    requested_from.as_deref(),
                    requested_through.as_deref(),
                )?;
                Ok((
                    candidate,
                    EvaluationSourceDriverIdentity {
                        descriptor,
                        launch_digest,
                    },
                ))
            })();
            let transcript = redact_transcript(driver.transcript(), &secrets);
            let transcript_result = write_confined_run_json_atomic(
                &state.anchor,
                Path::new("driver.json"),
                &serde_json::to_value(transcript)?,
            );
            drop(driver);
            transcript_result?;
            driver_result
        })();
        // Cancellation and terminal publication share one commit boundary. A cancellation
        // accepted before this point prevents draft publication; once publication begins, cancel
        // waits and observes the terminal proposal instead of returning an accepted request.
        let _completion = lock(&state.completion_commit);
        match result {
            Ok((candidate, driver)) => {
                if state.cancel.is_cancelled() {
                    let message = "evaluation proposal was cancelled";
                    let _ = finish_proposal_with_terminal_fallback(
                        state,
                        EvaluationProposalStatus::Cancelled,
                        Some(candidate),
                        None,
                        Some(message),
                    );
                    let _ = record_event(
                        &execution.workspace,
                        "workbench.evaluation-proposal.finished",
                        json!({
                            "origin": execution.origin,
                            "proposalId": proposal_id,
                            "draftId": JsonValue::Null,
                            "status": EvaluationProposalStatus::Cancelled,
                            "error": message,
                        }),
                    );
                    return;
                }
                let source_sequences = proposal_source_sequences(
                    &execution.session,
                    &candidate.from_turn_id,
                    &candidate.through_turn_id,
                );
                let proposal = lock(&state.detail).summary.clone();
                let seed_draft = self.create_evaluation_draft_internal(
                    &proposal.workspace_id,
                    CreateEvaluationDraftRequest {
                        session_id: Some(proposal.session_id),
                        from_turn_id: candidate.from_turn_id.clone(),
                        through_turn_id: candidate.through_turn_id.clone(),
                    },
                    execution.origin,
                    false,
                );
                let retained_draft_id = seed_draft
                    .as_ref()
                    .ok()
                    .map(|draft| draft.summary.id.clone());
                let draft_result = seed_draft.and_then(|draft| {
                    self.apply_evaluation_proposal(
                        &draft.summary.id,
                        &proposal_id,
                        &candidate,
                        driver,
                        source_sequences,
                        execution.origin,
                    )
                });
                match draft_result {
                    Ok(draft) => {
                        let _ = finish_proposal_with_terminal_fallback(
                            state,
                            EvaluationProposalStatus::Complete,
                            Some(candidate),
                            Some(&draft.summary.id),
                            None,
                        );
                        let _ = record_event(
                            &execution.workspace,
                            "workbench.evaluation-proposal.finished",
                            json!({
                                "origin": execution.origin,
                                "proposalId": proposal_id,
                                "draftId": draft.summary.id,
                                "status": EvaluationProposalStatus::Complete,
                            }),
                        );
                    }
                    Err(error) => {
                        let secrets = lock(&self.inner.promotion.secret_values).clone();
                        let message = redact_string(&error.to_string(), &secrets);
                        let _ = finish_proposal_with_terminal_fallback(
                            state,
                            EvaluationProposalStatus::Failed,
                            Some(candidate),
                            retained_draft_id.as_deref(),
                            Some(&message),
                        );
                        let _ = record_event(
                            &execution.workspace,
                            "workbench.evaluation-proposal.finished",
                            json!({
                                "origin": execution.origin,
                                "proposalId": proposal_id,
                                "draftId": retained_draft_id,
                                "status": EvaluationProposalStatus::Failed,
                                "error": message,
                            }),
                        );
                    }
                }
            }
            Err(error) => {
                let status = if state.cancel.is_cancelled() {
                    EvaluationProposalStatus::Cancelled
                } else {
                    EvaluationProposalStatus::Failed
                };
                let secrets = lock(&self.inner.promotion.secret_values).clone();
                let message = redact_string(&error.to_string(), &secrets);
                let _ = finish_proposal_with_terminal_fallback(
                    state,
                    status,
                    None,
                    None,
                    Some(&message),
                );
                let _ = record_event(
                    &execution.workspace,
                    "workbench.evaluation-proposal.finished",
                    json!({
                        "origin": execution.origin,
                        "proposalId": proposal_id,
                        "draftId": JsonValue::Null,
                        "status": status,
                        "error": message,
                    }),
                );
            }
        }
    }

    pub(super) fn has_active_evaluation_proposal(&self, workspace_id: &str) -> bool {
        lock(&self.inner.promotion.proposals)
            .values()
            .any(|proposal| {
                let summary = lock(&proposal.detail).summary.clone();
                summary.workspace_id == workspace_id && !summary.status.is_finished()
            })
    }

    #[cfg(test)]
    pub(super) fn fail_next_proposal_terminal_event_persist(
        &self,
        proposal_id: &str,
    ) -> Result<(), RunError> {
        self.promotion_proposal_state(proposal_id)?
            .fail_next_terminal_event_persist
            .store(true, Ordering::Release);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_evaluation_proposal(
        &self,
        draft_id: &str,
        proposal_id: &str,
        candidate: &EvaluationProposalCandidate,
        driver: EvaluationSourceDriverIdentity,
        source_event_sequences: Vec<u64>,
        origin: WorkbenchOrigin,
    ) -> Result<EvaluationDraftDetail, RunError> {
        let _evidence_lifecycle = lock(&self.inner.promotion.evidence_lifecycle);
        let state = self.promotion_draft_state(draft_id)?;
        let event_commit = lock(&state.event_commit);
        let mut detail = lock(&state.detail);
        let previous_detail = detail.clone();
        let current = current_draft_revision(&previous_detail, draft_id)?;
        if previous_detail.revisions.len() >= MAX_EVALUATION_DRAFT_REVISIONS {
            return Err(RunError::InvalidRequest(format!(
                "evaluation drafts retain at most {MAX_EVALUATION_DRAFT_REVISIONS} revisions"
            )));
        }
        let source = capture_confined_run_tree(
            &state.anchor,
            &PathBuf::from("revisions").join(&current.id).join("source"),
        )?;
        let revision_id = format!("revision-{}-{}", now_ms(), random_suffix());
        let mut next = current.clone();
        next.id.clone_from(&revision_id);
        next.previous_revision_id = Some(current.id);
        next.created_at_ms = now_ms();
        next.task.clone_from(&candidate.task);
        next.evaluator = candidate.evaluator.clone();
        next.measurements.clone_from(&candidate.measurements);
        next.blocking_issues
            .retain(|issue| issue != MANUAL_AUTHORING_BLOCKER);
        next.source.proposal = Some(EvaluationProposalProvenance {
            proposal_id: proposal_id.to_owned(),
            harness_id: next.source.harness_id.clone(),
            model_profile_id: next.source.model_profile_id.clone(),
            model_id: next.source.model_id.clone(),
            prompt_contract: PROPOSAL_PROMPT_CONTRACT.to_owned(),
            rationale: candidate.rationale.clone(),
            source_event_sequences,
            driver: Some(driver),
        });
        validate_revision(&next)?;
        let revision_path = PathBuf::from("revisions").join(&revision_id);
        write_confined_run_captured_tree(&state.anchor, &revision_path.join("source"), &source)?;
        let mut next_detail = previous_detail.clone();
        next_detail
            .summary
            .current_revision_id
            .clone_from(&revision_id);
        revision_status(&next).clone_into(&mut next_detail.summary.status);
        next_detail.summary.updated_at_ms = now_ms();
        next_detail.revisions.push(next);
        let event = RunEvent {
            sequence: next_detail.events.len() as u64 + 1,
            at_ms: now_ms(),
            kind: "evaluation-draft.proposed".to_owned(),
            payload: json!({
                "draftId": draft_id,
                "revisionId": revision_id,
                "proposalId": proposal_id,
                "origin": origin,
            }),
            progress: None,
        };
        next_detail.events.push(event.clone());
        reject_serialized_protected_data(&next_detail, &lock(&self.inner.promotion.secret_values))?;
        persist_draft_transition(
            &state,
            &previous_detail,
            &next_detail,
            &event,
            Some(&revision_path),
        )?;
        *detail = next_detail.clone();
        drop(detail);
        drop(event_commit);
        let _ = state.sender.send(event);
        self.notify_evaluation_library_changed(&state, "proposed");
        Ok(next_detail)
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
        self.create_evaluation_draft_internal(workspace_id, request, origin, true)
    }

    #[allow(clippy::too_many_lines)]
    fn create_evaluation_draft_internal(
        &self,
        workspace_id: &str,
        request: CreateEvaluationDraftRequest,
        origin: WorkbenchOrigin,
        notify_workbench: bool,
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
        let _evidence_lifecycle = lock(&self.inner.promotion.evidence_lifecycle);
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
        let source_driver = self.evaluation_source_driver_identity(&session, &session_summary)?;
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
                driver: Some(source_driver),
                proposal: None,
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
        let anchor = match AgentSessionDirectoryAnchor::open(directory.clone()) {
            Ok(anchor) => Arc::new(anchor),
            Err(error) => {
                let _ = fs::remove_dir(&directory);
                return Err(error);
            }
        };
        let pending_bundle = PendingEvaluationBundle::new(id.clone(), anchor.clone());
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
        reject_serialized_protected_data(&detail, &known_secrets)?;
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
        lock(&self.inner.promotion.drafts).insert(id.clone(), state.clone());
        drop(known_secrets);
        if notify_workbench
            && let Err(error) = record_event(
                &workspace,
                "workbench.evaluation-draft.created",
                json!({
                    "origin": origin,
                    "draftId": id,
                    "revisionId": revision_id,
                }),
            )
        {
            lock(&self.inner.promotion.drafts).remove(&id);
            return Err(error);
        }
        pending_bundle.commit();
        Ok(lock(&state.detail).clone())
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
        let evidence_lifecycle = lock(&self.inner.promotion.evidence_lifecycle);
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
        validate_optional_display_name(request.name.as_deref())?;
        let material_change = next.task != current.task
            || next.limits != current.limits
            || next.evaluator != current.evaluator
            || next.measurements != current.measurements
            || next.blocking_issues != current.blocking_issues;
        let mut next_detail = previous_detail.clone();
        let mut created_revision = None;
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
            created_revision = Some((revision_path, source));
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
        reject_serialized_protected_data(&next_detail, &lock(&self.inner.promotion.secret_values))?;
        if let Some((revision_path, source)) = &created_revision {
            write_confined_run_captured_tree(&state.anchor, &revision_path.join("source"), source)?;
        }
        persist_draft_transition(
            &state,
            &previous_detail,
            &next_detail,
            &event,
            created_revision
                .as_ref()
                .map(|(revision_path, _)| revision_path.as_path()),
        )?;
        *detail = next_detail.clone();
        drop((detail, event_commit, evidence_lifecycle));
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
        if previous_detail.validations.len() >= MAX_EVALUATION_VALIDATION_ATTEMPTS {
            return Err(RunError::InvalidRequest(format!(
                "evaluation drafts retain at most {MAX_EVALUATION_VALIDATION_ATTEMPTS} validation attempts"
            )));
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
        reject_serialized_protected_data(&next_detail, &lock(&self.inner.promotion.secret_values))?;
        let passing = self.revalidate_passing_attempts(&mut next_detail, &revision);
        reject_serialized_protected_data(&next_detail, &lock(&self.inner.promotion.secret_values))?;
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

    fn validation_attempt_supports_promotion(
        &self,
        attempt: &EvaluationValidationAttempt,
        revision: &EvaluationRevision,
    ) -> Result<(), String> {
        let run_id = attempt
            .run_id
            .as_deref()
            .ok_or_else(|| "passing validation has no retained run evidence".to_owned())?;
        let run = self
            .get(run_id)
            .map_err(|error| format!("passing validation evidence is unavailable: {error}"))?;
        if run.summary.status != RunStatus::Passed
            || run.score != attempt.score
            || run
                .score
                .as_ref()
                .is_none_or(|score| score["passed"].as_bool() != Some(true))
            || run.summary.scenario_id != revision.source.scenario_id
            || run.summary.harness_id.as_deref() != Some(revision.source.harness_id.as_str())
            || run.summary.model_profile_id.as_deref()
                != Some(revision.source.model_profile_id.as_str())
            || run.summary.model_id != revision.source.model_id
            || run.assembly.question != revision.task
            || run.assembly.scenario.id != revision.source.scenario_id
            || run.assembly.workspace.seed_revision != revision.source.source_revision
            || run.assembly.capability_sources != revision.capability_recipe
            || run.assembly.limits != revision.limits
        {
            return Err(
                "passing validation evidence is inconsistent with the retained attempt".to_owned(),
            );
        }
        Self::verify_validation_driver_identity(revision, run.assembly.harness.driver.as_ref())
            .map_err(|error| error.to_string())
    }

    fn revalidate_passing_attempts(
        &self,
        detail: &mut EvaluationDraftDetail,
        revision: &EvaluationRevision,
    ) -> bool {
        let terminal_validation_ids = durable_terminal_validation_ids(&detail.events);
        let mut passing = false;
        for attempt in &mut detail.validations {
            if attempt.revision_id != revision.id
                || attempt.execution_status != EvaluationExecutionStatus::Complete
                || attempt.assertion_status != ValidationAssertionStatus::Passed
            {
                continue;
            }
            if !terminal_validation_ids.contains(&attempt.id) {
                attempt.execution_status = EvaluationExecutionStatus::Inconclusive;
                attempt.assertion_status = ValidationAssertionStatus::NotEvaluated;
                attempt.error = Some("validation finalization evidence is missing".to_owned());
                continue;
            }
            match self.validation_attempt_supports_promotion(attempt, revision) {
                Ok(()) => passing = true,
                Err(error) => {
                    attempt.execution_status = EvaluationExecutionStatus::Inconclusive;
                    attempt.assertion_status = ValidationAssertionStatus::NotEvaluated;
                    attempt.error = Some(error);
                }
            }
        }
        passing
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
        let _evidence_lifecycle = lock(&self.inner.promotion.evidence_lifecycle);
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
                driver: None,
                proposal: None,
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

    fn promotion_proposal_state(&self, id: &str) -> Result<Arc<PromotionProposalState>, RunError> {
        let state = lock(&self.inner.promotion.proposals)
            .get(id)
            .cloned()
            .ok_or_else(|| {
                RunError::InvalidRequest(format!("unknown evaluation proposal: {id}"))
            })?;
        if state.evidence_quarantined.load(Ordering::Acquire) {
            return Err(RunError::InvalidRequest(format!(
                "unknown evaluation proposal: {id}"
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
    pub(super) fn fail_next_validation_finalization_persist(&self) {
        self.inner
            .promotion
            .fail_next_validation_finalization_persist
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn fail_next_validation_fallback_persist(&self) {
        self.inner
            .promotion
            .fail_next_validation_fallback_persist
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
    pub(super) fn fill_validation_retention_for_test(
        &self,
        draft_id: &str,
        revision_id: &str,
    ) -> Result<(), RunError> {
        let state = self.promotion_draft_state(draft_id)?;
        let _event_commit = lock(&state.event_commit);
        let mut detail = lock(&state.detail);
        let mut next_detail = detail.clone();
        while next_detail.validations.len() < MAX_EVALUATION_VALIDATION_ATTEMPTS {
            let attempt = EvaluationValidationAttempt {
                id: format!("validation-retention-{}", next_detail.validations.len()),
                draft_id: draft_id.to_owned(),
                revision_id: revision_id.to_owned(),
                execution_status: EvaluationExecutionStatus::Complete,
                assertion_status: ValidationAssertionStatus::Failed,
                harness_id: "fixture".to_owned(),
                model_profile_id: "fixture".to_owned(),
                run_id: None,
                started_at_ms: now_ms(),
                finished_at_ms: Some(now_ms()),
                error: None,
                score: Some(json!({ "passed": false })),
            };
            let event = RunEvent {
                sequence: next_detail.events.len() as u64 + 1,
                at_ms: now_ms(),
                kind: "evaluation-validation.created".to_owned(),
                payload: serde_json::to_value(&attempt)?,
                progress: None,
            };
            next_detail.validations.push(attempt);
            next_detail.events.push(event);
        }
        persist_draft_detail(&state, &next_detail)?;
        write_draft_event_evidence(&state.anchor, &next_detail.events)?;
        *detail = next_detail;
        Ok(())
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

    #[cfg(test)]
    pub(super) fn inject_definition_publication_before_draft_commit_for_recovery_test(
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
        Ok(definition_id)
    }

    #[cfg(test)]
    pub(super) fn inject_draft_manifest_ahead_of_event_log_for_recovery_test(
        &self,
        draft_id: &str,
    ) -> Result<RunEvent, RunError> {
        let state = self.promotion_draft_state(draft_id)?;
        let _event_commit = lock(&state.event_commit);
        let mut detail = lock(&state.detail);
        let event = RunEvent {
            sequence: detail.events.len() as u64 + 1,
            at_ms: now_ms(),
            kind: "evaluation-draft.recovery-test".to_owned(),
            payload: json!({ "draftId": draft_id }),
            progress: None,
        };
        detail.events.push(event.clone());
        persist_draft_detail(&state, &detail)?;
        Ok(event)
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
            let _ = persist_validation_attempt_update(&state, &attempt_id, |attempt| {
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
        let attempt = lock(&state.detail)
            .validations
            .iter()
            .find(|attempt| attempt.id == attempt_id)
            .cloned();
        if let Err(error) = self.record_validation_finished(
            &state,
            serde_json::to_value(attempt).unwrap_or_else(|_| {
                json!({
                    "validationId": attempt_id,
                    "executionStatus": "inconclusive",
                })
            }),
        ) {
            #[cfg(test)]
            let fail_fallback_persist = self
                .inner
                .promotion
                .fail_next_validation_fallback_persist
                .swap(false, Ordering::AcqRel);
            #[cfg(not(test))]
            let fail_fallback_persist = false;
            broadcast_validation_finalization_failure(
                &state,
                &attempt_id,
                &error,
                fail_fallback_persist,
            );
        }
        self.notify_evaluation_library_changed(&state, "validation-finished");
    }

    fn record_validation_finished(
        &self,
        state: &PromotionDraftState,
        payload: JsonValue,
    ) -> Result<(), RunError> {
        #[cfg(not(test))]
        let _ = self;
        #[cfg(test)]
        if self
            .inner
            .promotion
            .fail_next_validation_finalization_persist
            .swap(false, Ordering::AcqRel)
        {
            return Err(RunError::EvidencePersistence(
                "injected validation finalization persistence failure".to_owned(),
            ));
        }
        record_draft_event(state, "evaluation-validation.finished", payload)
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
        persist_validation_attempt_update(state, attempt_id, |attempt| {
            attempt.execution_status = EvaluationExecutionStatus::Running;
        })?;
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
        if let Err(error) = self.publish_validation_run(state, attempt_id, &prepared.id) {
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
                let (execution_status, assertion_status, error) =
                    classify_validation_run(revision, &run);
                persist_validation_attempt_update(state, attempt_id, |attempt| {
                    attempt.execution_status = execution_status;
                    attempt.assertion_status = assertion_status;
                    attempt.finished_at_ms = Some(now_ms());
                    attempt.error = error;
                    attempt.score = score;
                })?;
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

    fn publish_validation_run(
        &self,
        state: &PromotionDraftState,
        attempt_id: &str,
        run_id: &str,
    ) -> Result<(), RunError> {
        let _evidence_lifecycle = lock(&self.inner.promotion.evidence_lifecycle);
        if state.evidence_quarantined.load(Ordering::Acquire) {
            return Err(RunError::InvalidRequest(format!(
                "unknown evaluation draft: {}",
                lock(&state.detail).summary.id
            )));
        }
        let run = self.state(run_id)?;
        let secrets = lock(&self.inner.promotion.secret_values).clone();
        if !secrets.is_empty()
            && confined_bundle_contains_protected_data(&run.agent_session_directories, &secrets)
                .unwrap_or(true)
        {
            mark_workspace_evidence_unavailable(&run, false);
            return Err(RunError::EvidencePersistence(
                PROTECTED_WORKSPACE_PATH_ERROR.to_owned(),
            ));
        }
        persist_validation_attempt_update(state, attempt_id, |attempt| {
            attempt.run_id = Some(run_id.to_owned());
        })
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
        let harness = self
            .inner
            .harnesses
            .get(&revision.source.harness_id)
            .ok_or_else(|| {
                RunError::InvalidRequest(format!(
                    "captured validation configuration is unavailable: harness={}, profile={}",
                    revision.source.harness_id, revision.source.model_profile_id
                ))
            })?;
        let current_model_id = harness
            .models
            .get(&revision.source.model_profile_id)
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
        let source_driver = revision.source.driver.as_ref().ok_or_else(|| {
            RunError::EvidencePersistence(
                "captured validation has no driver stack identity".to_owned(),
            )
        })?;
        let current_launch_digest = driver_launch_digest(&harness.launch)?;
        if current_launch_digest != source_driver.launch_digest {
            return Err(RunError::EvidencePersistence(
                "captured validation harness launch changed".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_validation_driver_identity(
        revision: &EvaluationRevision,
        current: Option<&DriverDescriptor>,
    ) -> Result<(), RunError> {
        let expected = revision.source.driver.as_ref().ok_or_else(|| {
            RunError::EvidencePersistence(
                "captured validation has no driver stack identity".to_owned(),
            )
        })?;
        let current = current.ok_or_else(|| {
            RunError::EvidencePersistence(
                "validation run did not retain its driver identity".to_owned(),
            )
        })?;
        if current != &expected.descriptor {
            return Err(RunError::EvidencePersistence(format!(
                "captured validation driver changed: expected {} {} {:?}, found {} {} {:?}",
                expected.descriptor.name,
                expected.descriptor.version,
                expected.descriptor.revision,
                current.name,
                current.version,
                current.revision,
            )));
        }
        Ok(())
    }

    fn evaluation_source_driver_identity(
        &self,
        session: &AgentSessionState,
        summary: &AgentSessionSummary,
    ) -> Result<EvaluationSourceDriverIdentity, RunError> {
        let descriptor = lock(&session.events)
            .iter()
            .rev()
            .find(|event| event.kind == "agent.session.ready")
            .and_then(|event| event.payload.get("driver"))
            .cloned()
            .ok_or_else(|| {
                RunError::EvidencePersistence(format!(
                    "agent session {} has no captured driver identity",
                    summary.id
                ))
            })
            .and_then(|value| serde_json::from_value(value).map_err(RunError::from))?;
        let harness = self
            .inner
            .harnesses
            .get(&summary.harness_id)
            .ok_or_else(|| {
                RunError::InvalidRequest(format!("unknown harness: {}", summary.harness_id))
            })?;
        Ok(EvaluationSourceDriverIdentity {
            descriptor,
            launch_digest: driver_launch_digest(&harness.launch)?,
        })
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

fn classify_validation_run(
    revision: &EvaluationRevision,
    run: &RunDetail,
) -> (
    EvaluationExecutionStatus,
    ValidationAssertionStatus,
    Option<String>,
) {
    let driver_outcome = run.events.iter().rev().find_map(|event| {
        (event.kind == "driver.turn-finished")
            .then(|| event.payload.get("outcome").and_then(JsonValue::as_str))
            .flatten()
    });
    let driver_identity_error = (run.summary.status != RunStatus::Cancelled)
        .then(|| {
            RunController::verify_validation_driver_identity(
                revision,
                run.assembly.harness.driver.as_ref(),
            )
            .err()
            .map(|error| error.to_string())
        })
        .flatten();
    match (run.summary.status, driver_outcome, driver_identity_error) {
        (_, _, Some(error)) => (
            EvaluationExecutionStatus::Inconclusive,
            ValidationAssertionStatus::NotEvaluated,
            Some(error),
        ),
        (RunStatus::Passed, Some("completed"), None) => (
            EvaluationExecutionStatus::Complete,
            ValidationAssertionStatus::Passed,
            None,
        ),
        (RunStatus::Cancelled, _, None) => (
            EvaluationExecutionStatus::Cancelled,
            ValidationAssertionStatus::NotEvaluated,
            run.summary.error.clone(),
        ),
        (RunStatus::Failed, Some("intervened"), None) => (
            EvaluationExecutionStatus::Intervened,
            ValidationAssertionStatus::NotEvaluated,
            Some("validation driver outcome was intervened".to_owned()),
        ),
        (RunStatus::Failed, Some("completed"), None)
            if run.summary.error.is_none()
                && run.score.as_ref().is_some_and(catalog_score_is_complete) =>
        {
            (
                EvaluationExecutionStatus::Complete,
                ValidationAssertionStatus::Failed,
                None,
            )
        }
        (_, outcome, None) => (
            EvaluationExecutionStatus::Inconclusive,
            ValidationAssertionStatus::NotEvaluated,
            run.summary.error.clone().or_else(|| {
                Some(format!(
                    "validation driver did not complete normally: {}",
                    outcome.unwrap_or("missing terminal outcome")
                ))
            }),
        ),
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

fn proposal_prompt() -> &'static str {
    r"You are proposing one portable evaluation from durable agent-turn evidence.
Return exactly one JSON object and no Markdown.
Use schemaVersion 1 with these fields:
- fromTurnId and throughTurnId: a contiguous meaningful terminal span from the supplied turns
- task: a standalone instruction that can be replayed from the first turn's starting state
- evaluator: the reviewed catalog-to-file evaluator object supplied by the source scenario
- measurements: a useful subset of duration, model-turns, capability-calls, workspace-effects, and reported-usage
- rationale: a concise explanation for the builder
If requestedSpan contains turn IDs, use that exact span.
Do not include a harness or model in the portable task."
}

fn validate_requested_proposal_span(
    turns: &[AgentTurnSummary],
    from_turn_id: &str,
    through_turn_id: &str,
) -> Result<(), RunError> {
    let from = turns
        .iter()
        .position(|turn| turn.id == from_turn_id)
        .ok_or_else(|| RunError::InvalidRequest(format!("unknown source turn: {from_turn_id}")))?;
    let through = turns
        .iter()
        .position(|turn| turn.id == through_turn_id)
        .ok_or_else(|| {
            RunError::InvalidRequest(format!("unknown source turn: {through_turn_id}"))
        })?;
    if through < from {
        return Err(RunError::InvalidRequest(
            "--through must not precede --from".to_owned(),
        ));
    }
    if turns[from..=through]
        .iter()
        .any(|turn| !turn.status.is_finished())
    {
        return Err(RunError::InvalidRequest(
            "evaluation proposals require terminal source turns".to_owned(),
        ));
    }
    Ok(())
}

fn validate_coherent_proposal_span(turns: &[AgentTurnSummary]) -> Result<(), String> {
    let Some(first) = turns.first() else {
        return Err("evaluation proposal source span must not be empty".to_owned());
    };
    if turns.iter().any(|turn| {
        turn.source_revision != first.source_revision
            || turn.capability_revisions != first.capability_revisions
    }) {
        return Err(
            "evaluation proposal source turns must share one workspace and capability revision"
                .to_owned(),
        );
    }
    Ok(())
}

fn validate_proposal_turn_command_size(
    proposal_id: &str,
    turn_task: &JsonValue,
) -> Result<(), RunError> {
    let start_record = command(
        "proposal-start",
        CommandBody::StartTurn {
            session_id: format!("proposal-session-{proposal_id}"),
            turn_id: format!("proposal-turn-{proposal_id}"),
            task: turn_task.clone(),
            capability_sources: json!([]),
        },
    );
    if serde_json::to_vec(&start_record)?.len().saturating_add(1) > MAX_DRIVER_RECORD_BYTES {
        return Err(RunError::InvalidRequest(format!(
            "proposal source evidence exceeds the {MAX_DRIVER_RECORD_BYTES}-byte driver record limit"
        )));
    }
    Ok(())
}

fn validate_proposal_candidate(
    candidate: &EvaluationProposalCandidate,
    source_turns: &[AgentTurnSummary],
    expected_evaluator: &EvaluationEvaluator,
    requested_from: Option<&str>,
    requested_through: Option<&str>,
) -> Result<(), RunError> {
    if candidate.schema_version != PROPOSAL_SCHEMA_VERSION {
        return Err(RunError::Protocol(format!(
            "unsupported evaluation proposal schema: {}",
            candidate.schema_version
        )));
    }
    let from = source_turns
        .iter()
        .position(|turn| turn.id == candidate.from_turn_id)
        .ok_or_else(|| {
            RunError::Protocol(format!(
                "evaluation proposal selected unavailable source turn: {}",
                candidate.from_turn_id
            ))
        })?;
    let through = source_turns
        .iter()
        .position(|turn| turn.id == candidate.through_turn_id)
        .ok_or_else(|| {
            RunError::Protocol(format!(
                "evaluation proposal selected unavailable source turn: {}",
                candidate.through_turn_id
            ))
        })?;
    if through < from {
        return Err(RunError::Protocol(
            "evaluation proposal selected a reversed source span".to_owned(),
        ));
    }
    validate_coherent_proposal_span(&source_turns[from..=through]).map_err(RunError::Protocol)?;
    if let (Some(from), Some(through)) = (requested_from, requested_through)
        && (candidate.from_turn_id != from || candidate.through_turn_id != through)
    {
        return Err(RunError::Protocol(
            "evaluation proposal changed the explicitly requested turn span".to_owned(),
        ));
    }
    if candidate.task.trim().is_empty() {
        return Err(RunError::Protocol(
            "evaluation proposal task must not be empty".to_owned(),
        ));
    }
    if candidate.rationale.trim().is_empty() {
        return Err(RunError::Protocol(
            "evaluation proposal rationale must not be empty".to_owned(),
        ));
    }
    let unique = candidate.measurements.iter().collect::<BTreeSet<_>>();
    if candidate.measurements.is_empty()
        || unique.len() != candidate.measurements.len()
        || candidate
            .measurements
            .iter()
            .any(|measurement| !PROPOSAL_MEASUREMENTS.contains(&measurement.as_str()))
    {
        return Err(RunError::Protocol(
            "evaluation proposal measurements must be a unique non-empty supported subset"
                .to_owned(),
        ));
    }
    if &candidate.evaluator != expected_evaluator {
        return Err(RunError::Protocol(
            "evaluation proposal must preserve the reviewed evaluator and its parameters"
                .to_owned(),
        ));
    }
    Ok(())
}

fn proposal_source_sequences(
    session: &AgentSessionState,
    from_turn_id: &str,
    through_turn_id: &str,
) -> Vec<u64> {
    let turns = lock(&session.turns);
    let Some(from) = turns.iter().position(|turn| turn.id == from_turn_id) else {
        return Vec::new();
    };
    let Some(through) = turns.iter().position(|turn| turn.id == through_turn_id) else {
        return Vec::new();
    };
    let selected = turns[from..=through]
        .iter()
        .map(|turn| turn.id.as_str())
        .collect::<HashSet<_>>();
    lock(&session.events)
        .iter()
        .filter(|event| {
            event
                .payload
                .get("turnId")
                .and_then(JsonValue::as_str)
                .is_some_and(|turn_id| selected.contains(turn_id))
        })
        .map(|event| event.sequence)
        .collect()
}

fn driver_launch_digest(launch: &DriverLaunch) -> Result<String, RunError> {
    fn update(hasher: &mut Sha256, value: &[u8]) {
        hasher.update(value.len().to_le_bytes());
        hasher.update(value);
    }

    let mut hasher = Sha256::new();
    update(
        &mut hasher,
        launch.executable.as_os_str().as_encoded_bytes(),
    );
    for argument in &launch.args {
        update(&mut hasher, argument.as_encoded_bytes());
    }
    if let Some(cwd) = &launch.cwd {
        update(&mut hasher, cwd.as_os_str().as_encoded_bytes());
    }
    for (name, value) in &launch.env {
        update(&mut hasher, name.as_encoded_bytes());
        if sensitive_name(&name.to_string_lossy()) {
            update(&mut hasher, b"[sensitive-value]");
        } else {
            update(&mut hasher, value.as_encoded_bytes());
        }
    }
    update(&mut hasher, &[u8::from(launch.clear_env)]);
    if let Some(executable) = resolve_driver_executable(launch)
        && executable.is_file()
    {
        update(&mut hasher, &fs::read(executable)?);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn resolve_driver_executable(launch: &DriverLaunch) -> Option<PathBuf> {
    if launch.executable.is_absolute() {
        return Some(launch.executable.clone());
    }
    if launch.executable.components().count() > 1 {
        return Some(
            launch
                .cwd
                .as_deref()
                .unwrap_or_else(|| Path::new("."))
                .join(&launch.executable),
        );
    }
    let configured_path = launch
        .env
        .iter()
        .find(|(name, _)| name == "PATH")
        .map(|(_, value)| value.clone());
    let path = configured_path.or_else(|| {
        (!launch.clear_env)
            .then(|| std::env::var_os("PATH"))
            .flatten()
    })?;
    std::env::split_paths(&path)
        .map(|directory| directory.join(&launch.executable))
        .find(|candidate| candidate.is_file())
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

fn validate_optional_display_name(name: Option<&str>) -> Result<(), RunError> {
    name.map_or(Ok(()), validate_display_name)
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

fn persist_proposal(state: &PromotionProposalState) -> Result<(), RunError> {
    if state.evidence_quarantined.load(Ordering::Acquire) {
        return Err(RunError::InvalidRequest(
            "unknown evaluation proposal".to_owned(),
        ));
    }
    write_confined_run_json_atomic(
        &state.anchor,
        Path::new("manifest.json"),
        &serde_json::to_value(lock(&state.detail).clone())?,
    )
}

fn record_proposal_event(
    state: &PromotionProposalState,
    kind: &str,
    payload: JsonValue,
) -> Result<RunEvent, RunError> {
    let _commit = lock(&state.event_commit);
    let mut detail = lock(&state.detail);
    let previous = detail.clone();
    let event = RunEvent {
        sequence: detail.events.len() as u64 + 1,
        at_ms: now_ms(),
        kind: kind.to_owned(),
        payload,
        progress: None,
    };
    detail.events.push(event.clone());
    if let Err(error) = persist_proposal_detail(state, &detail) {
        *detail = previous;
        return Err(error);
    }
    #[cfg(test)]
    if kind == "evaluation-proposal.finished"
        && state
            .fail_next_terminal_event_persist
            .swap(false, Ordering::AcqRel)
    {
        *detail = previous;
        persist_proposal_detail(state, &detail)?;
        return Err(RunError::EvidencePersistence(
            "injected terminal proposal event persistence failure".to_owned(),
        ));
    }
    let mut line = serde_json::to_vec(&event)?;
    line.push(b'\n');
    if let Err(error) = append_confined_run_bytes(&state.anchor, Path::new("events.jsonl"), &line) {
        *detail = previous;
        persist_proposal_detail(state, &detail)?;
        return Err(error);
    }
    drop(detail);
    let _ = state.sender.send(event.clone());
    Ok(event)
}

fn persist_proposal_detail(
    state: &PromotionProposalState,
    detail: &EvaluationProposalDetail,
) -> Result<(), RunError> {
    if state.evidence_quarantined.load(Ordering::Acquire) {
        return Err(RunError::InvalidRequest(format!(
            "unknown evaluation proposal: {}",
            detail.summary.id
        )));
    }
    write_confined_run_json_atomic(
        &state.anchor,
        Path::new("manifest.json"),
        &serde_json::to_value(detail)?,
    )
}

fn update_proposal_status(
    state: &PromotionProposalState,
    status: EvaluationProposalStatus,
    error: Option<&str>,
) -> Result<(), RunError> {
    let _commit = lock(&state.event_commit);
    let mut detail = lock(&state.detail);
    detail.summary.status = status;
    detail.summary.error = error.map(str::to_owned);
    if status.is_finished() {
        detail.summary.finished_at_ms = Some(now_ms());
    }
    persist_proposal_detail(state, &detail)
}

fn finish_proposal(
    state: &PromotionProposalState,
    status: EvaluationProposalStatus,
    candidate: Option<EvaluationProposalCandidate>,
    draft_id: Option<&str>,
    error: Option<&str>,
) -> Result<(), RunError> {
    {
        let _commit = lock(&state.event_commit);
        let mut detail = lock(&state.detail);
        detail.summary.status = status;
        detail.summary.draft_id = draft_id.map(str::to_owned);
        detail.summary.finished_at_ms = Some(now_ms());
        detail.summary.error = error.map(str::to_owned);
        detail.candidate = candidate;
        persist_proposal_detail(state, &detail)?;
    }
    let proposal_id = lock(&state.detail).summary.id.clone();
    record_proposal_event(
        state,
        "evaluation-proposal.finished",
        json!({
            "proposalId": proposal_id,
            "draftId": draft_id,
            "status": status,
            "error": error,
        }),
    )?;
    Ok(())
}

fn finish_proposal_with_terminal_fallback(
    state: &PromotionProposalState,
    status: EvaluationProposalStatus,
    candidate: Option<EvaluationProposalCandidate>,
    draft_id: Option<&str>,
    error: Option<&str>,
) -> Result<(), RunError> {
    let result = finish_proposal(state, status, candidate, draft_id, error);
    if result.is_err() {
        let detail = lock(&state.detail);
        let event = RunEvent {
            sequence: detail.events.len() as u64 + 1,
            at_ms: now_ms(),
            kind: "evaluation-proposal.finished".to_owned(),
            payload: json!({
                "proposalId": detail.summary.id,
                "draftId": draft_id,
                "status": status,
                "error": error,
                "durable": false,
                "persistenceError": "terminal proposal evidence could not be persisted",
            }),
            progress: None,
        };
        drop(detail);
        let _ = state.sender.send(event);
    }
    result
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

fn reject_serialized_protected_data(
    value: &impl Serialize,
    secrets: &[Vec<u8>],
) -> Result<(), RunError> {
    if secrets.is_empty() {
        return Ok(());
    }
    let serialized = serde_json::to_vec(value)?;
    if redact_evidence_bytes(&serialized, secrets) != serialized {
        return Err(RunError::EvidencePersistence(
            PROTECTED_WORKSPACE_PATH_ERROR.to_owned(),
        ));
    }
    Ok(())
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
    #[serde(default)]
    expected_draft_name: Option<String>,
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
        expected_draft_name: Some(publication.detail.summary.name.clone()),
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
    let mut detail = lock(&state.detail);
    let previous_detail = detail.clone();
    let mut next_detail = previous_detail.clone();
    let event = RunEvent {
        sequence: next_detail.events.len() as u64 + 1,
        at_ms: now_ms(),
        kind: kind.to_owned(),
        payload,
        progress: None,
    };
    next_detail.events.push(event.clone());
    persist_draft_transition(state, &previous_detail, &next_detail, &event, None)?;
    *detail = next_detail;
    drop(detail);
    let _ = state.sender.send(event);
    Ok(())
}

fn persist_validation_attempt_update(
    state: &PromotionDraftState,
    attempt_id: &str,
    update: impl FnOnce(&mut EvaluationValidationAttempt),
) -> Result<(), RunError> {
    let _event_commit = lock(&state.event_commit);
    let mut detail = lock(&state.detail);
    let mut next_detail = detail.clone();
    let attempt = next_detail
        .validations
        .iter_mut()
        .find(|attempt| attempt.id == attempt_id)
        .ok_or_else(|| {
            RunError::InvalidRequest(format!("unknown evaluation validation: {attempt_id}"))
        })?;
    update(attempt);
    next_detail.summary.updated_at_ms = now_ms();
    persist_draft_detail(state, &next_detail)?;
    *detail = next_detail;
    Ok(())
}

fn broadcast_validation_finalization_failure(
    state: &PromotionDraftState,
    attempt_id: &str,
    error: &RunError,
    fail_fallback_persist: bool,
) {
    let message = format!("validation finalization could not be persisted: {error}");
    let durable_attempt = !fail_fallback_persist
        && persist_validation_attempt_update(state, attempt_id, |attempt| {
            attempt.execution_status = EvaluationExecutionStatus::Inconclusive;
            attempt.assertion_status = ValidationAssertionStatus::NotEvaluated;
            attempt.finished_at_ms = Some(now_ms());
            attempt.error = Some(message.clone());
        })
        .is_ok();
    if !durable_attempt {
        let _event_commit = lock(&state.event_commit);
        let mut detail = lock(&state.detail);
        if let Some(attempt) = detail
            .validations
            .iter_mut()
            .find(|attempt| attempt.id == attempt_id)
        {
            attempt.execution_status = EvaluationExecutionStatus::Inconclusive;
            attempt.assertion_status = ValidationAssertionStatus::NotEvaluated;
            attempt.finished_at_ms = Some(now_ms());
            attempt.error = Some(message.clone());
        }
    }
    let event = {
        let detail = lock(&state.detail);
        RunEvent {
            sequence: detail.events.len() as u64,
            at_ms: now_ms(),
            kind: "evaluation-validation.finished".to_owned(),
            payload: json!({
                "validationId": attempt_id,
                "executionStatus": EvaluationExecutionStatus::Inconclusive,
                "assertionStatus": ValidationAssertionStatus::NotEvaluated,
                "error": message,
                "durable": false,
                "durableAttempt": durable_attempt,
            }),
            progress: None,
        }
    };
    let _ = state.sender.send(event);
}

fn mark_validation_cancelled(
    state: &PromotionDraftState,
    attempt_id: &str,
) -> Result<(), RunError> {
    persist_validation_attempt_update(state, attempt_id, |attempt| {
        attempt.execution_status = EvaluationExecutionStatus::Cancelled;
        attempt.assertion_status = ValidationAssertionStatus::NotEvaluated;
        attempt.finished_at_ms = Some(now_ms());
        attempt.error = None;
        attempt.score = None;
    })
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
    manifest_events: &[RunEvent],
) -> Result<Option<Vec<RunEvent>>, RunError> {
    validate_draft_event_sequence(bundle, manifest_events)?;
    let Some(source) = read_optional_confined_run_file(anchor, Path::new("events.jsonl"))? else {
        if manifest_events.is_empty() {
            return Ok(None);
        }
        write_draft_event_evidence(anchor, manifest_events)?;
        return Ok(Some(manifest_events.to_vec()));
    };
    let events = match parse_events(&source) {
        Ok(events) => events,
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
    validate_draft_event_sequence(bundle, &events)?;
    if draft_events_are_prefix(&events, manifest_events)? {
        if events.len() < manifest_events.len() {
            write_draft_event_evidence(anchor, manifest_events)?;
        }
        return Ok(Some(manifest_events.to_vec()));
    }
    if draft_events_are_prefix(manifest_events, &events)? {
        return Ok(Some(events));
    }
    tracing::warn!(
        bundle = %bundle.display(),
        "skipping evaluation draft with divergent manifest and event evidence"
    );
    Err(RunError::EvidencePersistence(
        "evaluation draft manifest and event evidence diverge".to_owned(),
    ))
}

fn validate_draft_event_sequence(bundle: &Path, events: &[RunEvent]) -> Result<(), RunError> {
    if events
        .iter()
        .enumerate()
        .all(|(index, event)| event.sequence == index as u64 + 1)
    {
        return Ok(());
    }
    tracing::warn!(
        bundle = %bundle.display(),
        "skipping evaluation draft with non-sequential event evidence"
    );
    Err(RunError::EvidencePersistence(
        "evaluation draft event evidence is non-sequential".to_owned(),
    ))
}

fn draft_events_are_prefix(prefix: &[RunEvent], events: &[RunEvent]) -> Result<bool, RunError> {
    if prefix.len() > events.len() {
        return Ok(false);
    }
    prefix
        .iter()
        .zip(events)
        .try_fold(true, |matches, (left, right)| {
            Ok(matches && serde_json::to_vec(left)? == serde_json::to_vec(right)?)
        })
}

fn durable_terminal_validation_ids(events: &[RunEvent]) -> HashSet<String> {
    events
        .iter()
        .filter(|event| {
            event.kind == "evaluation-validation.finished"
                && event.payload["durable"].as_bool() != Some(false)
        })
        .filter_map(|event| {
            event.payload["id"]
                .as_str()
                .or_else(|| event.payload["validationId"].as_str())
                .map(str::to_owned)
        })
        .collect()
}

fn write_draft_event_evidence(
    anchor: &AgentSessionDirectoryAnchor,
    events: &[RunEvent],
) -> Result<(), RunError> {
    let mut bytes = Vec::new();
    for event in events {
        serde_json::to_writer(&mut bytes, event)?;
        bytes.push(b'\n');
    }
    write_confined_run_bytes_atomic(anchor, Path::new("events.jsonl"), &bytes)
}

fn recover_unfinalized_validations(
    detail: &mut EvaluationDraftDetail,
) -> Vec<EvaluationValidationAttempt> {
    let terminal_validation_ids = durable_terminal_validation_ids(&detail.events);
    let mut recovered = Vec::new();
    for validation in &mut detail.validations {
        let missing_terminal_evidence = validation.execution_status.is_finished()
            && !terminal_validation_ids.contains(&validation.id);
        if validation.execution_status.is_finished() && !missing_terminal_evidence {
            continue;
        }
        validation.execution_status = EvaluationExecutionStatus::Inconclusive;
        validation.assertion_status = ValidationAssertionStatus::NotEvaluated;
        validation.finished_at_ms = Some(now_ms());
        validation.error = Some(if missing_terminal_evidence {
            "validation finalization evidence is missing".to_owned()
        } else {
            "controller stopped before validation finalized".to_owned()
        });
        recovered.push(validation.clone());
    }
    recovered
}

fn recover_published_proposal(
    proposal_id: &str,
    drafts: &HashMap<String, Arc<PromotionDraftState>>,
) -> Result<Option<(String, EvaluationProposalCandidate)>, RunError> {
    let mut recovered = Vec::new();
    for draft in drafts.values() {
        let detail = lock(&draft.detail);
        if let Some(revision) = detail.revisions.iter().find(|revision| {
            revision
                .source
                .proposal
                .as_ref()
                .is_some_and(|provenance| provenance.proposal_id == proposal_id)
        }) {
            let provenance = revision
                .source
                .proposal
                .as_ref()
                .expect("the matching proposal provenance was checked");
            let Some(from_turn_id) = revision.source.turn_ids.first().cloned() else {
                return Err(RunError::Protocol(format!(
                    "published proposal revision has no source turns: {proposal_id}"
                )));
            };
            let through_turn_id = revision
                .source
                .turn_ids
                .last()
                .cloned()
                .expect("source turns were checked as non-empty");
            recovered.push((
                detail.summary.id.clone(),
                EvaluationProposalCandidate {
                    schema_version: PROPOSAL_SCHEMA_VERSION,
                    from_turn_id,
                    through_turn_id,
                    task: revision.task.clone(),
                    evaluator: revision.evaluator.clone(),
                    measurements: revision.measurements.clone(),
                    rationale: provenance.rationale.clone(),
                },
            ));
        }
    }
    if recovered.len() > 1 {
        return Err(RunError::Protocol(format!(
            "proposal publication is attributed to multiple draft revisions: {proposal_id}"
        )));
    }
    Ok(recovered.pop())
}

fn reconcile_interrupted_proposal(
    detail: &mut EvaluationProposalDetail,
    drafts: &HashMap<String, Arc<PromotionDraftState>>,
) {
    match recover_published_proposal(&detail.summary.id, drafts) {
        Ok(Some((draft_id, candidate))) => {
            detail.summary.status = EvaluationProposalStatus::Complete;
            detail.summary.draft_id = Some(draft_id);
            detail.summary.finished_at_ms = Some(now_ms());
            detail.summary.error = None;
            detail.candidate = Some(candidate);
        }
        Ok(None) => {
            detail.summary.status = EvaluationProposalStatus::Failed;
            detail.summary.finished_at_ms = Some(now_ms());
            detail.summary.error =
                Some("controller stopped before the proposal session finalized".to_owned());
        }
        Err(error) => {
            detail.summary.status = EvaluationProposalStatus::Failed;
            detail.summary.finished_at_ms = Some(now_ms());
            detail.summary.error = Some(format!(
                "proposal publication could not be reconciled: {error}"
            ));
        }
    }
}

fn load_proposals(
    root: &Path,
    drafts: &HashMap<String, Arc<PromotionDraftState>>,
) -> Result<HashMap<String, Arc<PromotionProposalState>>, RunError> {
    let mut proposals = HashMap::new();
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
                tracing::warn!(bundle = %entry.path().display(), %error, "skipping unreadable evaluation proposal");
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
        let mut detail: EvaluationProposalDetail = match serde_json::from_slice(&manifest) {
            Ok(detail) => detail,
            Err(error) => {
                tracing::warn!(bundle = %entry.path().display(), %error, "skipping malformed evaluation proposal");
                continue;
            }
        };
        if detail.summary.id != entry.file_name().to_string_lossy() {
            continue;
        }
        match load_draft_event_evidence(&anchor, &entry.path(), &detail.events) {
            Ok(Some(events)) => detail.events = events,
            Ok(None) => {}
            Err(_) => continue,
        }
        let terminal_event_count = detail
            .events
            .iter()
            .filter(|event| event.kind == "evaluation-proposal.finished")
            .count();
        if terminal_event_count > 1
            || (terminal_event_count == 1 && !detail.summary.status.is_finished())
        {
            tracing::warn!(
                bundle = %entry.path().display(),
                "skipping evaluation proposal with contradictory terminal evidence"
            );
            continue;
        }
        let needs_terminal_event = terminal_event_count == 0;
        let interrupted = !detail.summary.status.is_finished();
        if interrupted {
            reconcile_interrupted_proposal(&mut detail, drafts);
        }
        let (sender, _) = broadcast::channel(128);
        let state = Arc::new(PromotionProposalState {
            detail: Mutex::new(detail),
            anchor,
            sender,
            event_commit: Mutex::new(()),
            completion_commit: Mutex::new(()),
            cancel: CancellationToken::new(),
            evidence_quarantined: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_terminal_event_persist: AtomicBool::new(false),
        });
        if needs_terminal_event {
            let summary = lock(&state.detail).summary.clone();
            record_proposal_event(
                &state,
                "evaluation-proposal.finished",
                json!({
                    "proposalId": summary.id,
                    "draftId": summary.draft_id,
                    "status": summary.status,
                    "error": summary.error,
                }),
            )?;
        } else {
            persist_proposal(&state)?;
        }
        let id = lock(&state.detail).summary.id.clone();
        proposals.insert(id, state);
    }
    Ok(proposals)
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
        match load_draft_event_evidence(&anchor, &entry.path(), &detail.events) {
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
        let recovered_validations = recover_unfinalized_validations(&mut detail);
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
                        && transaction
                            .expected_draft_name
                            .as_deref()
                            .is_none_or(|expected| draft.name == expected)
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
    fn proposal_candidate_is_confined_to_supplied_turns_and_reviewed_evaluator() {
        let evaluator = EvaluationEvaluator {
            id: CATALOG_EVALUATOR_ID.to_owned(),
            version: CATALOG_EVALUATOR_VERSION,
            parameters: CatalogEvaluatorParameters {
                active_names: vec!["alpha".to_owned()],
                total_score: 3,
                required_capability_sources: vec!["catalog".to_owned()],
                output_path: "result.json".into(),
                require_schema: true,
            },
        };
        let mut candidate = EvaluationProposalCandidate {
            schema_version: PROPOSAL_SCHEMA_VERSION,
            from_turn_id: "turn-1".to_owned(),
            through_turn_id: "turn-2".to_owned(),
            task: "Create result.json from the active catalog".to_owned(),
            evaluator: evaluator.clone(),
            measurements: vec!["duration".to_owned(), "capability-calls".to_owned()],
            rationale: "These turns contain the reusable behavior.".to_owned(),
        };
        let source_turns = ["turn-1", "turn-2"]
            .into_iter()
            .map(|id| AgentTurnSummary {
                id: id.to_owned(),
                session_id: "session-1".to_owned(),
                prompt: "prompt".to_owned(),
                input: None,
                source_revision: "revision-1".to_owned(),
                capability_revisions: BTreeMap::from([(
                    "catalog".to_owned(),
                    "catalog-revision-1".to_owned(),
                )]),
                status: AgentTurnStatus::Completed,
                started_at_ms: 1,
                finished_at_ms: Some(2),
                outcome: Some("completed".to_owned()),
                error: None,
                human_intervention_at_ms: None,
            })
            .collect::<Vec<_>>();

        validate_proposal_candidate(&candidate, &source_turns, &evaluator, None, None).unwrap();

        candidate.through_turn_id = "turn-3".to_owned();
        assert!(
            validate_proposal_candidate(&candidate, &source_turns, &evaluator, None, None,)
                .unwrap_err()
                .to_string()
                .contains("unavailable source turn")
        );

        candidate.through_turn_id = "turn-2".to_owned();
        candidate.evaluator.parameters.total_score = 99;
        assert!(
            validate_proposal_candidate(&candidate, &source_turns, &evaluator, None, None,)
                .unwrap_err()
                .to_string()
                .contains("preserve the reviewed evaluator")
        );

        candidate.evaluator = evaluator.clone();
        let mut incompatible_turns = source_turns;
        incompatible_turns[1].source_revision = "revision-2".to_owned();
        validate_requested_proposal_span(&incompatible_turns, "turn-1", "turn-2").unwrap();
        assert!(
            validate_coherent_proposal_span(&incompatible_turns)
                .unwrap_err()
                .contains("must share one workspace and capability revision")
        );
        assert!(
            validate_proposal_candidate(&candidate, &incompatible_turns, &evaluator, None, None,)
                .unwrap_err()
                .to_string()
                .contains("must share one workspace and capability revision")
        );
    }

    #[test]
    fn proposal_input_limit_accounts_for_the_fully_encoded_driver_record() {
        let source = json!({
            "turns": ["x".repeat(MAX_AGENT_TURN_INPUT_BYTES - 512)],
        });
        assert!(serde_json::to_vec(&source).unwrap().len() <= MAX_AGENT_TURN_INPUT_BYTES);
        let turn_task = json!({
            "mode": "evaluation-proposal",
            "promptContract": PROPOSAL_PROMPT_CONTRACT,
            "prompt": proposal_prompt(),
            "input": source,
        });
        assert!(
            validate_proposal_turn_command_size("proposal-1", &turn_task)
                .unwrap_err()
                .to_string()
                .contains("driver record limit")
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
                driver: None,
                proposal: None,
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
                driver: None,
                proposal: None,
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

    #[test]
    fn launch_identity_tracks_behavior_environment_without_fingerprinting_credentials() {
        let mut launch = DriverLaunch::new("driver");
        launch.env = vec![
            ("ADAPTER_MODE".into(), "one".into()),
            ("PROVIDER_TOKEN".into(), "credential-one".into()),
        ];
        let original = driver_launch_digest(&launch).unwrap();

        launch.env[0].1 = "two".into();
        assert_ne!(driver_launch_digest(&launch).unwrap(), original);

        launch.env[0].1 = "one".into();
        launch.env[1].1 = "credential-two".into();
        assert_eq!(driver_launch_digest(&launch).unwrap(), original);
    }
}
