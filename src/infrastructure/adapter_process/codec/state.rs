use time::OffsetDateTime;
use trakkin_provider_sdk::v1::{
    Key, ReadStateRequest, ReadStateResponse as ProtoStateReadEvent,
    SourceMembership as ProtoSourceMembership, StateField as ProtoStateField,
    StatePresence as ProtoStatePresence,
    TargetedStateFieldEffectKind as ProtoTargetedStateFieldEffectKind,
    TargetedStateMembershipEffect as ProtoTargetedStateMembershipEffect,
    TargetedStateWriteCapability as ProtoTargetedStateWriteCapability,
    TargetedStateWriteCertainty as ProtoTargetedStateWriteCertainty,
    TargetedStateWriteIdempotencyMode as ProtoTargetedStateWriteIdempotencyMode,
    TargetedStateWritePreconditionMode as ProtoTargetedStateWritePreconditionMode,
    TargetedStateWriteRetryDisposition as ProtoTargetedStateWriteRetryDisposition,
    TargetedStateWriteStatus as ProtoTargetedStateWriteStatus, Value, read_state_response,
    read_targeted_state_response, state_observation, subject_reference,
};

use crate::{
    application::adapter_runtime::{
        AdapterReadEvent, AdapterSourceMembership, AdapterStatePresence, AdapterTargetedStateField,
        AdapterTargetedStateFieldEffect, AdapterTargetedStateFieldEffectKind,
        AdapterTargetedStateMatch, AdapterTargetedStateMembershipEffect,
        AdapterTargetedStateReadOutcome, AdapterTargetedStateWriteCapability,
        AdapterTargetedStateWriteCausation, AdapterTargetedStateWriteCertainty,
        AdapterTargetedStateWriteIdempotencyMode, AdapterTargetedStateWriteOutcome,
        AdapterTargetedStateWritePreconditionMode, AdapterTargetedStateWriteRetryDisposition,
        AdapterTargetedStateWriteStatus, AdapterValue, ReadAdapterSource,
    },
    domain::acquisition::{StateBatch, StateField, StateObservation, StateSubject, StateValue},
};

use super::{
    super::AdapterProcessError,
    common::{
        map_adapter_error, map_catalog_relation_key, map_domain_term, map_milliseconds,
        map_provider_item_key, map_read_completion, map_read_failure, map_read_request,
        map_state_value, map_value,
    },
};

pub(in super::super) fn map_read_state_request(
    command: ReadAdapterSource,
) -> Result<ReadStateRequest, AdapterProcessError> {
    let request = map_read_request(command)?;
    Ok(ReadStateRequest {
        operation_id: request.operation_id,
        source_key: Some(request.source_key),
        mode: request.mode as i32,
        prior_cursor: request.prior_cursor,
        preferred_batch_size: request.preferred_batch_size,
    })
}

pub(in super::super) fn map_state_event(
    event: ProtoStateReadEvent,
    operation_id: &[u8],
) -> Result<AdapterReadEvent<StateBatch>, AdapterProcessError> {
    match event.event.ok_or(AdapterProcessError::InvalidResponse(
        "state event is missing",
    ))? {
        read_state_response::Event::Heartbeat(heartbeat) => {
            if heartbeat.operation_id != operation_id {
                return Err(AdapterProcessError::InvalidResponse(
                    "state heartbeat operation ID does not match",
                ));
            }
            Ok(AdapterReadEvent::Heartbeat {
                records_emitted: heartbeat.records_emitted,
            })
        }
        read_state_response::Event::Batch(batch) => {
            Ok(AdapterReadEvent::Batch(map_state_batch(batch)?))
        }
        read_state_response::Event::Completed(completed) => {
            Ok(AdapterReadEvent::Completed(map_read_completion(completed)?))
        }
        read_state_response::Event::Failed(failed) => {
            Ok(AdapterReadEvent::Failed(map_read_failure(failed.error)?))
        }
        read_state_response::Event::Cancelled(_) => Ok(AdapterReadEvent::Cancelled),
    }
}

fn map_state_batch(
    batch: trakkin_provider_sdk::v1::StateBatch,
) -> Result<StateBatch, AdapterProcessError> {
    Ok(StateBatch::new(
        batch.sequence,
        batch
            .observations
            .into_iter()
            .map(map_state_observation)
            .collect::<Result<_, _>>()?,
    ))
}

fn map_state_observation(
    observation: trakkin_provider_sdk::v1::StateObservation,
) -> Result<StateObservation, AdapterProcessError> {
    let subject = match observation
        .subject
        .and_then(|subject| subject.subject)
        .ok_or(AdapterProcessError::InvalidResponse(
            "state observation subject is missing",
        ))? {
        subject_reference::Subject::ProviderItemKey(key) => {
            StateSubject::ProviderItem(map_provider_item_key(Some(key))?)
        }
        subject_reference::Subject::CatalogRelationKey(key) => {
            StateSubject::CatalogRelation(map_catalog_relation_key(Some(key))?)
        }
    };
    let field = observation
        .field
        .ok_or(AdapterProcessError::InvalidResponse(
            "state observation field is missing",
        ))?;
    let field = map_domain_state_field(field)?;
    let observed_at = OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(observation.observed_time_milliseconds) * 1_000_000,
    )
    .map_err(|_| AdapterProcessError::InvalidResponse("state observation time is invalid"))?;
    match observation
        .observation
        .ok_or(AdapterProcessError::InvalidResponse(
            "state observation operation is missing",
        ))? {
        state_observation::Observation::Value(value) => StateObservation::set(
            subject,
            field,
            map_state_value(Some(value))?,
            observation.provider_revision,
            observed_at,
        ),
        state_observation::Observation::Deletion(_) => {
            StateObservation::delete(subject, field, observation.provider_revision, observed_at)
        }
    }
    .map_err(|_| AdapterProcessError::InvalidResponse("invalid state observation"))
}

pub(in super::super) fn map_state_subject(
    subject: StateSubject,
) -> trakkin_provider_sdk::v1::SubjectReference {
    let subject = match subject {
        StateSubject::ProviderItem(key) => subject_reference::Subject::ProviderItemKey(Key {
            namespace: key.namespace().to_owned(),
            value: key.value().to_vec(),
        }),
        StateSubject::CatalogRelation(key) => subject_reference::Subject::CatalogRelationKey(Key {
            namespace: key.namespace().to_owned(),
            value: key.value().to_vec(),
        }),
    };
    trakkin_provider_sdk::v1::SubjectReference {
        subject: Some(subject),
    }
}

fn map_domain_state_subject(
    subject: trakkin_provider_sdk::v1::SubjectReference,
) -> Result<StateSubject, AdapterProcessError> {
    match subject.subject.ok_or(AdapterProcessError::InvalidResponse(
        "targeted state subject is missing",
    ))? {
        subject_reference::Subject::ProviderItemKey(key) => Ok(StateSubject::ProviderItem(
            map_provider_item_key(Some(key))?,
        )),
        subject_reference::Subject::CatalogRelationKey(key) => Ok(StateSubject::CatalogRelation(
            map_catalog_relation_key(Some(key))?,
        )),
    }
}

pub(in super::super) fn map_state_field(field: StateField) -> ProtoStateField {
    ProtoStateField {
        field: Some(trakkin_provider_sdk::v1::Term {
            namespace: field.field().namespace().to_owned(),
            name: field.field().name().to_owned(),
        }),
        unit: field.unit().map(|unit| trakkin_provider_sdk::v1::Term {
            namespace: unit.namespace().to_owned(),
            name: unit.name().to_owned(),
        }),
    }
}

pub(in super::super) fn map_domain_state_value(value: StateValue) -> Value {
    map_value(match value {
        StateValue::Boolean(value) => AdapterValue::Boolean(value),
        StateValue::Integer(value) => AdapterValue::Integer(value),
        StateValue::Decimal(value) => AdapterValue::Decimal(value),
        StateValue::Text(value) => AdapterValue::Text(value),
        StateValue::Bytes(value) => AdapterValue::Bytes(value),
        StateValue::TimestampMilliseconds(value) => AdapterValue::TimestampMilliseconds(value),
    })
}

pub(in super::super) fn map_source_membership(
    membership: AdapterSourceMembership,
) -> ProtoSourceMembership {
    match membership {
        AdapterSourceMembership::Present => ProtoSourceMembership::Present,
        AdapterSourceMembership::Absent => ProtoSourceMembership::Absent,
        AdapterSourceMembership::Unknown => ProtoSourceMembership::Unknown,
    }
}

fn map_domain_state_field(field: ProtoStateField) -> Result<StateField, AdapterProcessError> {
    Ok(StateField::new(
        map_domain_term(field.field)?,
        field
            .unit
            .map(|unit| map_domain_term(Some(unit)))
            .transpose()?,
    ))
}

pub(in super::super) fn map_targeted_state_outcome(
    response: trakkin_provider_sdk::v1::ReadTargetedStateResponse,
) -> Result<AdapterTargetedStateReadOutcome, AdapterProcessError> {
    match response
        .outcome
        .ok_or(AdapterProcessError::InvalidResponse(
            "targeted state outcome is missing",
        ))? {
        read_targeted_state_response::Outcome::Matched(matched) => {
            let membership = match ProtoSourceMembership::try_from(matched.membership)
                .map_err(|_| AdapterProcessError::InvalidResponse("unknown source membership"))?
            {
                ProtoSourceMembership::Present => AdapterSourceMembership::Present,
                ProtoSourceMembership::Absent => AdapterSourceMembership::Absent,
                ProtoSourceMembership::Unknown => AdapterSourceMembership::Unknown,
                ProtoSourceMembership::Unspecified => {
                    return Err(AdapterProcessError::InvalidResponse(
                        "source membership is unspecified",
                    ));
                }
            };
            let fields = matched
                .fields
                .into_iter()
                .map(|observation| {
                    let presence = match ProtoStatePresence::try_from(observation.presence)
                        .map_err(|_| {
                            AdapterProcessError::InvalidResponse("unknown targeted state presence")
                        })? {
                        ProtoStatePresence::Present => AdapterStatePresence::Present,
                        ProtoStatePresence::Deleted => AdapterStatePresence::Deleted,
                        ProtoStatePresence::Absent => AdapterStatePresence::Absent,
                        ProtoStatePresence::Unknown => AdapterStatePresence::Unknown,
                        ProtoStatePresence::Stale => AdapterStatePresence::Stale,
                        ProtoStatePresence::Unspecified => {
                            return Err(AdapterProcessError::InvalidResponse(
                                "targeted state presence is unspecified",
                            ));
                        }
                    };
                    Ok(AdapterTargetedStateField {
                        field: map_domain_state_field(observation.field.ok_or(
                            AdapterProcessError::InvalidResponse("targeted state field is missing"),
                        )?)?,
                        presence,
                        value: observation
                            .value
                            .map(|value| map_state_value(Some(value)))
                            .transpose()?,
                    })
                })
                .collect::<Result<_, AdapterProcessError>>()?;
            Ok(AdapterTargetedStateReadOutcome::Matched(
                AdapterTargetedStateMatch {
                    membership,
                    fields,
                    provider_revision: matched.provider_revision,
                    observed_at: map_milliseconds(
                        matched.observed_time_milliseconds,
                        "targeted state observation time is invalid",
                    )?,
                    expires_at: matched
                        .expires_time_milliseconds
                        .map(|expires| {
                            map_milliseconds(expires, "targeted state expiry is invalid")
                        })
                        .transpose()?,
                    precondition: (!matched.precondition.is_empty())
                        .then_some(matched.precondition),
                    write_causation: matched.write_causation.map(|causation| {
                        AdapterTargetedStateWriteCausation {
                            idempotency_key: causation.idempotency_key,
                            receipt: causation.receipt,
                            provider_revision: causation.provider_revision,
                        }
                    }),
                },
            ))
        }
        read_targeted_state_response::Outcome::Unsupported(_) => {
            Ok(AdapterTargetedStateReadOutcome::Unsupported)
        }
        read_targeted_state_response::Outcome::NotFound(_) => {
            Ok(AdapterTargetedStateReadOutcome::NotFound)
        }
        read_targeted_state_response::Outcome::Ambiguous(ambiguous) => {
            Ok(AdapterTargetedStateReadOutcome::Ambiguous(
                ambiguous
                    .candidates
                    .into_iter()
                    .map(map_domain_state_subject)
                    .collect::<Result<_, _>>()?,
            ))
        }
        read_targeted_state_response::Outcome::Indeterminate(indeterminate) => {
            Ok(AdapterTargetedStateReadOutcome::Indeterminate(
                map_adapter_error(indeterminate.error.ok_or(
                    AdapterProcessError::InvalidResponse(
                        "targeted state indeterminate error is missing",
                    ),
                )?),
            ))
        }
    }
}

pub(in super::super) fn map_targeted_state_write_outcome(
    response: trakkin_provider_sdk::v1::WriteTargetedStateResponse,
) -> Result<AdapterTargetedStateWriteOutcome, AdapterProcessError> {
    let status = match ProtoTargetedStateWriteStatus::try_from(response.status)
        .map_err(|_| AdapterProcessError::InvalidResponse("unknown targeted state write status"))?
    {
        ProtoTargetedStateWriteStatus::Applied => AdapterTargetedStateWriteStatus::Applied,
        ProtoTargetedStateWriteStatus::NoOp => AdapterTargetedStateWriteStatus::NoOp,
        ProtoTargetedStateWriteStatus::Rejected => AdapterTargetedStateWriteStatus::Rejected,
        ProtoTargetedStateWriteStatus::Failed => AdapterTargetedStateWriteStatus::Failed,
        ProtoTargetedStateWriteStatus::Indeterminate => {
            AdapterTargetedStateWriteStatus::Indeterminate
        }
        ProtoTargetedStateWriteStatus::Unspecified => {
            return Err(AdapterProcessError::InvalidResponse(
                "targeted state write status is unspecified",
            ));
        }
    };
    let certainty =
        match ProtoTargetedStateWriteCertainty::try_from(response.certainty).map_err(|_| {
            AdapterProcessError::InvalidResponse("unknown targeted state write certainty")
        })? {
            ProtoTargetedStateWriteCertainty::ConfirmedApplied => {
                AdapterTargetedStateWriteCertainty::ConfirmedApplied
            }
            ProtoTargetedStateWriteCertainty::ConfirmedNotApplied => {
                AdapterTargetedStateWriteCertainty::ConfirmedNotApplied
            }
            ProtoTargetedStateWriteCertainty::Unknown => {
                AdapterTargetedStateWriteCertainty::Unknown
            }
            ProtoTargetedStateWriteCertainty::Unspecified => {
                return Err(AdapterProcessError::InvalidResponse(
                    "targeted state write certainty is unspecified",
                ));
            }
        };
    let retry_disposition =
        match ProtoTargetedStateWriteRetryDisposition::try_from(response.retry_disposition)
            .map_err(|_| {
                AdapterProcessError::InvalidResponse(
                    "unknown targeted state write retry disposition",
                )
            })? {
            ProtoTargetedStateWriteRetryDisposition::NotRetryable => {
                AdapterTargetedStateWriteRetryDisposition::NotRetryable
            }
            ProtoTargetedStateWriteRetryDisposition::SafeSameKey => {
                AdapterTargetedStateWriteRetryDisposition::SafeSameKey
            }
            ProtoTargetedStateWriteRetryDisposition::ReconcileFirst => {
                AdapterTargetedStateWriteRetryDisposition::ReconcileFirst
            }
            ProtoTargetedStateWriteRetryDisposition::Unspecified => {
                return Err(AdapterProcessError::InvalidResponse(
                    "targeted state write retry disposition is unspecified",
                ));
            }
        };
    let membership_effect =
        match ProtoTargetedStateMembershipEffect::try_from(response.membership_effect).map_err(
            |_| AdapterProcessError::InvalidResponse("unknown targeted state membership effect"),
        )? {
            ProtoTargetedStateMembershipEffect::Unchanged => {
                AdapterTargetedStateMembershipEffect::Unchanged
            }
            ProtoTargetedStateMembershipEffect::Created => {
                AdapterTargetedStateMembershipEffect::Created
            }
            ProtoTargetedStateMembershipEffect::Unknown => {
                AdapterTargetedStateMembershipEffect::Unknown
            }
            ProtoTargetedStateMembershipEffect::Unspecified => {
                return Err(AdapterProcessError::InvalidResponse(
                    "targeted state membership effect is unspecified",
                ));
            }
        };
    let field_effects = response
        .field_effects
        .into_iter()
        .map(|effect| {
            let effect_kind = match ProtoTargetedStateFieldEffectKind::try_from(effect.effect)
                .map_err(|_| {
                    AdapterProcessError::InvalidResponse("unknown targeted state field effect")
                })? {
                ProtoTargetedStateFieldEffectKind::Set => AdapterTargetedStateFieldEffectKind::Set,
                ProtoTargetedStateFieldEffectKind::Cleared => {
                    AdapterTargetedStateFieldEffectKind::Cleared
                }
                ProtoTargetedStateFieldEffectKind::Unchanged => {
                    AdapterTargetedStateFieldEffectKind::Unchanged
                }
                ProtoTargetedStateFieldEffectKind::Unknown => {
                    AdapterTargetedStateFieldEffectKind::Unknown
                }
                ProtoTargetedStateFieldEffectKind::Unspecified => {
                    return Err(AdapterProcessError::InvalidResponse(
                        "targeted state field effect is unspecified",
                    ));
                }
            };
            Ok(AdapterTargetedStateFieldEffect {
                field: map_domain_state_field(effect.field.ok_or(
                    AdapterProcessError::InvalidResponse(
                        "targeted state write effect field is missing",
                    ),
                )?)?,
                effect: effect_kind,
                value: effect
                    .value
                    .map(|value| map_state_value(Some(value)))
                    .transpose()?,
            })
        })
        .collect::<Result<_, AdapterProcessError>>()?;
    Ok(AdapterTargetedStateWriteOutcome {
        status,
        certainty,
        retry_disposition,
        membership_effect,
        field_effects,
        provider_revision: response.provider_revision,
        successor_precondition: (!response.successor_precondition.is_empty())
            .then_some(response.successor_precondition),
        receipt: response.receipt,
        error: response.error.map(map_adapter_error),
    })
}

pub(in super::super) fn map_targeted_state_write_capability_to_proto(
    capability: &AdapterTargetedStateWriteCapability,
) -> ProtoTargetedStateWriteCapability {
    ProtoTargetedStateWriteCapability {
        fields: capability
            .fields
            .iter()
            .map(
                |field| trakkin_provider_sdk::v1::TargetedStateFieldWriteCapability {
                    field: Some(map_state_field(field.field.clone())),
                    set_supported: field.set_supported,
                    clear_supported: field.clear_supported,
                },
            )
            .collect(),
        may_create_source_membership: capability.may_create_source_membership,
        precondition_mode: match capability.precondition_mode {
            None => ProtoTargetedStateWritePreconditionMode::Unspecified,
            Some(AdapterTargetedStateWritePreconditionMode::ProviderToken) => {
                ProtoTargetedStateWritePreconditionMode::ProviderToken
            }
            Some(AdapterTargetedStateWritePreconditionMode::HostRecheckOnly) => {
                ProtoTargetedStateWritePreconditionMode::HostRecheckOnly
            }
        } as i32,
        idempotency_mode: match capability.idempotency_mode {
            None => ProtoTargetedStateWriteIdempotencyMode::Unspecified,
            Some(AdapterTargetedStateWriteIdempotencyMode::NotSupported) => {
                ProtoTargetedStateWriteIdempotencyMode::NotSupported
            }
            Some(AdapterTargetedStateWriteIdempotencyMode::StableKey) => {
                ProtoTargetedStateWriteIdempotencyMode::StableKey
            }
        } as i32,
        maximum_fields: capability.maximum_fields,
        maximum_request_bytes: capability.maximum_request_bytes,
        maximum_response_bytes: capability.maximum_response_bytes,
        maximum_receipt_bytes: capability.maximum_receipt_bytes,
    }
}
