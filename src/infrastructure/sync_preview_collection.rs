use std::marker::PhantomData;

use serde_json::Value;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::{
    application::{
        adapter_runtime::{
            AdapterFuture, AdapterKey, AdapterRetirement, AdapterRetirementFuture, AdapterSession,
            AdapterTargetedStateReadCapability, AdapterTargetedStateReadOutcome,
            ReadAdapterTargetedState,
        },
        connection::ConnectionSessionRegistry,
        error::{ApplicationError, ApplicationErrorCode},
        facade::ApplicationFuture,
        sync_preview::{ReceiverProgressCapability, TargetedReceiverPreflight},
        sync_state_evidence::MAX_SYNC_STATE_EVIDENCE_SLOTS,
        sync_workflow::{
            PreflightSyncReceivers, SyncReceiverPreflightApi, SyncReceiverPreflightResult,
            SyncReceiverPreflightTarget,
        },
    },
    domain::synchronization::{Quantizer, StateUnit},
};

pub trait SyncReceiverPreflightAdapterSession: Send {
    type Error;

    fn read_sync_receiver_preflight(
        &mut self,
        command: ReadAdapterTargetedState,
    ) -> AdapterFuture<'_, AdapterTargetedStateReadOutcome, Self::Error>;

    fn retire_sync_receiver_preflight(
        self,
    ) -> AdapterRetirementFuture<AdapterRetirement, Self::Error>;
}

impl<S> SyncReceiverPreflightAdapterSession for S
where
    S: AdapterSession,
{
    type Error = S::Error;

    fn read_sync_receiver_preflight(
        &mut self,
        command: ReadAdapterTargetedState,
    ) -> AdapterFuture<'_, AdapterTargetedStateReadOutcome, Self::Error> {
        self.read_targeted_state(command)
    }

    fn retire_sync_receiver_preflight(
        self,
    ) -> AdapterRetirementFuture<AdapterRetirement, Self::Error> {
        self.retire()
    }
}

#[derive(Clone)]
pub struct PostgresSyncReceiverPreflightService<G, S> {
    pool: PgPool,
    sessions: G,
    session: PhantomData<fn() -> S>,
}

impl<G, S> PostgresSyncReceiverPreflightService<G, S> {
    pub fn new(pool: PgPool, sessions: G) -> Self {
        Self {
            pool,
            sessions,
            session: PhantomData,
        }
    }
}

impl<G, S> SyncReceiverPreflightApi for PostgresSyncReceiverPreflightService<G, S>
where
    G: ConnectionSessionRegistry<S>,
    S: SyncReceiverPreflightAdapterSession,
    S::Error: Send,
{
    fn preflight_receivers(
        &self,
        command: PreflightSyncReceivers,
    ) -> ApplicationFuture<'_, Vec<SyncReceiverPreflightResult>> {
        Box::pin(async move {
            if command.targets.is_empty()
                || command.targets.len() > MAX_SYNC_STATE_EVIDENCE_SLOTS
                || command
                    .targets
                    .iter()
                    .enumerate()
                    .any(|(index, target)| command.targets[..index].contains(target))
            {
                return Err(invalid_provider_data(
                    "synchronization receiver preflight selection is invalid",
                ));
            }
            let mut source_ids = command
                .targets
                .iter()
                .map(|target| target.source_id)
                .collect::<Vec<_>>();
            source_ids.sort_unstable();
            source_ids.dedup();
            let rows = sqlx::query_as::<_, ReceiverSourceRow>(
                "SELECT
    source.id AS source_id,
    source.connection_id,
    source.provider_key_namespace AS source_key_namespace,
    source.provider_key_value AS source_key_value,
    source.availability AS source_availability,
    source.capabilities AS source_capabilities,
    source.capability_revision AS source_capability_revision,
    connection.status AS connection_status,
    connection.deleted_at AS connection_deleted_at,
    connection.capability_revision AS connection_capability_revision
FROM workspace_sources AS attachment
INNER JOIN sources AS source ON source.id = attachment.source_id
INNER JOIN connections AS connection ON connection.id = source.connection_id
WHERE
    attachment.workspace_id = $1
    AND source.id = ANY($2)
ORDER BY source.id
",
            )
            .bind(command.workspace_id)
            .bind(&source_ids)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| {
                ApplicationError::new(
                    ApplicationErrorCode::Internal,
                    "synchronization receiver preflight failed",
                )
            })?;
            if rows.len() != source_ids.len() {
                return Err(ApplicationError::new(
                    ApplicationErrorCode::SourceNotAttached,
                    "synchronization receiver source is not attached",
                ));
            }
            let mut results = Vec::with_capacity(command.targets.len());
            for (ordinal, target) in command.targets.into_iter().enumerate() {
                let row = rows
                    .iter()
                    .find(|row| row.source_id == target.source_id)
                    .ok_or_else(|| {
                        ApplicationError::new(
                            ApplicationErrorCode::SourceNotAttached,
                            "synchronization receiver source is not attached",
                        )
                    })?;
                validate_receiver_source(row)?;
                let (read_capability, progress_capability) =
                    parse_receiver_capabilities(&row.source_capabilities, &target)?;
                let identity = self.sessions.identity(row.connection_id).ok_or_else(|| {
                    ApplicationError::new(
                        ApplicationErrorCode::ConnectionUnavailable,
                        "synchronization receiver connection is unavailable",
                    )
                })?;
                let mut session = self.sessions.remove(row.connection_id).ok_or_else(|| {
                    ApplicationError::new(
                        ApplicationErrorCode::ConnectionUnavailable,
                        "synchronization receiver connection is unavailable",
                    )
                })?;
                let mut operation_id = command.operation_id.as_bytes().to_vec();
                operation_id.extend_from_slice(
                    &u64::try_from(ordinal)
                        .expect("bounded receiver ordinal fits u64")
                        .to_be_bytes(),
                );
                let outcome = match session
                    .read_sync_receiver_preflight(ReadAdapterTargetedState {
                        operation_id,
                        source_key: AdapterKey {
                            namespace: row.source_key_namespace.clone(),
                            value: row.source_key_value.clone(),
                        },
                        subject: target.subject.clone(),
                        fields: vec![target.field.clone()],
                        maximum_response_bytes: read_capability.maximum_response_bytes,
                        capability: read_capability.clone(),
                        reconciliation_idempotency_key: None,
                    })
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        let _ = session.retire_sync_receiver_preflight().await;
                        return Err(ApplicationError::new(
                            ApplicationErrorCode::ConnectionUnavailable,
                            "synchronization receiver targeted read failed",
                        ));
                    }
                };
                if let Some(unexpected) = self.sessions.replace(row.connection_id, session) {
                    if let Some(installed) = self.sessions.remove(row.connection_id) {
                        let _ = installed.retire_sync_receiver_preflight().await;
                    }
                    let _ = unexpected.retire_sync_receiver_preflight().await;
                    return Err(ApplicationError::new(
                        ApplicationErrorCode::ConnectionBusy,
                        "synchronization receiver connection changed during preflight",
                    ));
                }
                if self
                    .sessions
                    .identity(row.connection_id)
                    .is_none_or(|restored| restored != identity)
                {
                    return Err(ApplicationError::new(
                        ApplicationErrorCode::ConnectionUnavailable,
                        "synchronization receiver connection changed during preflight",
                    ));
                }
                results.push(SyncReceiverPreflightResult {
                    receiver: TargetedReceiverPreflight {
                        source_id: target.source_id,
                        subject: target.subject.clone(),
                        state_read_capability: read_capability,
                        progress_capability,
                        outcome,
                    },
                    target,
                    connection_id: row.connection_id,
                    source_capability_revision: row.source_capability_revision,
                    connection_capability_revision: row.connection_capability_revision,
                    process_instance_id: identity.process_instance_id,
                });
            }
            Ok(results)
        })
    }
}

fn validate_receiver_source(row: &ReceiverSourceRow) -> Result<(), ApplicationError> {
    if row.source_availability == "removed" {
        return Err(ApplicationError::new(
            ApplicationErrorCode::SourceNotAttached,
            "synchronization receiver source was removed",
        ));
    }
    if row.connection_status != "ready" || row.connection_deleted_at.is_some() {
        return Err(ApplicationError::new(
            ApplicationErrorCode::ConnectionUnavailable,
            "synchronization receiver connection is unavailable",
        ));
    }
    if row.source_capability_revision <= 0 || row.connection_capability_revision <= 0 {
        return Err(invalid_provider_data(
            "synchronization receiver capability revision is invalid",
        ));
    }
    Ok(())
}

fn parse_receiver_capabilities(
    capabilities: &Value,
    target: &SyncReceiverPreflightTarget,
) -> Result<
    (
        AdapterTargetedStateReadCapability,
        ReceiverProgressCapability,
    ),
    ApplicationError,
> {
    let read = capabilities
        .get("targetedStateRead")
        .filter(|read| read.is_object())
        .ok_or_else(capability_unavailable)?;
    let read_capability = AdapterTargetedStateReadCapability {
        maximum_fields: read["maximumFields"]
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(capability_invalid)?,
        maximum_response_bytes: read["maximumResponseBytes"]
            .as_u64()
            .ok_or_else(capability_invalid)?,
    };
    if read_capability.maximum_fields == 0 || read_capability.maximum_response_bytes == 0 {
        return Err(capability_unavailable());
    }
    let field = capabilities["stateFields"]
        .as_array()
        .and_then(|fields| {
            fields.iter().find(|field| {
                field["field"]["namespace"].as_str() == Some(target.field.field().namespace())
                    && field["field"]["name"].as_str() == Some(target.field.field().name())
                    && unit_matches(&field["unit"], target.field.unit())
            })
        })
        .ok_or_else(capability_unavailable)?;
    let range = field["numericRange"]
        .as_object()
        .ok_or_else(capability_unavailable)?;
    let minimum = parse_u64_string(range.get("minimum"))?;
    let maximum = parse_u64_string(range.get("maximum"))?;
    let step = parse_u64_string(range.get("step"))?;
    if minimum > maximum || step == 0 {
        return Err(capability_invalid());
    }
    let unit = target
        .field
        .unit()
        .and_then(|unit| parse_state_unit(unit.namespace(), unit.name()))
        .ok_or_else(capability_unavailable)?;
    let quantizer = match field["quantizer"].as_str() {
        Some("exact") => Quantizer::Exact,
        Some("nearest") => Quantizer::Nearest,
        Some("floor") => Quantizer::Floor,
        Some("ceiling") => Quantizer::Ceiling,
        _ => return Err(capability_unavailable()),
    };
    Ok((
        read_capability,
        ReceiverProgressCapability {
            unit,
            minimum,
            maximum,
            step,
            quantizer,
        },
    ))
}

fn unit_matches(stored: &Value, expected: Option<&crate::domain::acquisition::Term>) -> bool {
    match expected {
        Some(expected) => {
            stored["namespace"].as_str() == Some(expected.namespace())
                && stored["name"].as_str() == Some(expected.name())
        }
        None => stored.is_null(),
    }
}

fn parse_state_unit(namespace: &str, name: &str) -> Option<StateUnit> {
    if namespace != "trakkin.unit" {
        return None;
    }
    match name {
        "chapter" => Some(StateUnit::Chapter),
        "count" => Some(StateUnit::Count),
        "episode" => Some(StateUnit::Episode),
        "flag" => Some(StateUnit::Flag),
        "page" => Some(StateUnit::Page),
        "ratio" => Some(StateUnit::Ratio),
        "time" => Some(StateUnit::Time),
        _ => None,
    }
}

fn parse_u64_string(value: Option<&Value>) -> Result<u64, ApplicationError> {
    value
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
        .ok_or_else(capability_invalid)
}

fn capability_unavailable() -> ApplicationError {
    ApplicationError::new(
        ApplicationErrorCode::ProviderCapabilityUnavailable,
        "synchronization receiver capability is unavailable",
    )
}

fn capability_invalid() -> ApplicationError {
    invalid_provider_data("synchronization receiver capability is invalid")
}

fn invalid_provider_data(message: &'static str) -> ApplicationError {
    ApplicationError::new(ApplicationErrorCode::InvalidProviderData, message)
}

#[derive(FromRow)]
struct ReceiverSourceRow {
    source_id: Uuid,
    connection_id: Uuid,
    source_key_namespace: String,
    source_key_value: Vec<u8>,
    source_availability: String,
    source_capabilities: Value,
    source_capability_revision: i64,
    connection_status: String,
    connection_deleted_at: Option<time::OffsetDateTime>,
    connection_capability_revision: i64,
}
