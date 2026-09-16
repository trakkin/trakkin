use serde_json::json;
use time::OffsetDateTime;
use trakkin_provider_sdk::{
    v1::{
        ConfigurationValue, ContinueAuthenticationResponse, DecimalValue, Key, OperationFailure,
        ReadMode as ProtoReadMode, SecretValue, StartAuthenticationResponse, TimestampValue, Value,
        continue_authentication_response, start_authentication_response, value,
    },
    validation,
};

use crate::{
    application::{
        acquisition::ReadMode,
        adapter_runtime::{
            AdapterAuthenticationStep, AdapterCallError, AdapterKey, AdapterOperationResult,
            AdapterReadCompletion, AdapterValue, ReadAdapterSource,
        },
    },
    domain::acquisition::{BinaryAssetKey, CatalogRelationKey, ProviderItemKey, StateValue, Term},
};

use super::super::AdapterProcessError;

pub(super) struct ReadRequest {
    pub(super) operation_id: Vec<u8>,
    pub(super) source_key: Key,
    pub(super) mode: ProtoReadMode,
    pub(super) prior_cursor: Vec<u8>,
    pub(super) preferred_batch_size: u32,
}

pub(super) fn map_read_request(
    command: ReadAdapterSource,
) -> Result<ReadRequest, AdapterProcessError> {
    if command.operation_id.is_empty()
        || command.source_key.namespace.is_empty()
        || command.source_key.value.is_empty()
        || command.preferred_batch_size == 0
    {
        return Err(AdapterProcessError::InvalidReadRequest);
    }
    let (mode, prior_cursor) = match (command.mode, command.prior_cursor) {
        (ReadMode::Full, None) => (ProtoReadMode::Full, Vec::new()),
        (ReadMode::Incremental, Some(cursor)) if !cursor.is_empty() => {
            (ProtoReadMode::Incremental, cursor)
        }
        (ReadMode::Full | ReadMode::Incremental, _) => {
            return Err(AdapterProcessError::InvalidReadRequest);
        }
    };
    Ok(ReadRequest {
        operation_id: command.operation_id,
        source_key: Key {
            namespace: command.source_key.namespace,
            value: command.source_key.value,
        },
        mode,
        prior_cursor,
        preferred_batch_size: command.preferred_batch_size,
    })
}

pub(super) fn map_value(value: AdapterValue) -> Value {
    let value = match value {
        AdapterValue::Boolean(value) => value::Value::Boolean(value),
        AdapterValue::Integer(value) => value::Value::Integer(value),
        AdapterValue::Decimal(value) => value::Value::Decimal(DecimalValue { value }),
        AdapterValue::Text(value) => value::Value::Text(value),
        AdapterValue::Bytes(value) => value::Value::Bytes(value),
        AdapterValue::TimestampMilliseconds(milliseconds) => {
            value::Value::Timestamp(TimestampValue { milliseconds })
        }
    };
    Value { value: Some(value) }
}

pub(super) fn map_read_completion(
    completed: trakkin_provider_sdk::v1::ReadCompleted,
) -> Result<AdapterReadCompletion, AdapterProcessError> {
    let observed_at = OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(completed.observed_time_milliseconds) * 1_000_000,
    )
    .map_err(|_| AdapterProcessError::InvalidResponse("read observation time is invalid"))?;
    Ok(AdapterReadCompletion {
        next_cursor: completed.next_cursor,
        evidence_revision: completed.evidence_revision,
        observed_at,
    })
}

pub(super) fn map_read_failure(
    error: Option<OperationFailure>,
) -> Result<AdapterCallError, AdapterProcessError> {
    let error = error.ok_or(AdapterProcessError::InvalidResponse(
        "read failure error is missing",
    ))?;
    validate_adapter_error(&error)?;
    Ok(map_adapter_error(error))
}

pub(in super::super) fn map_adapter_key(key: AdapterKey) -> Result<Key, AdapterProcessError> {
    if key.namespace.is_empty() || key.value.is_empty() {
        return Err(AdapterProcessError::InvalidAssetRequest);
    }
    Ok(Key {
        namespace: key.namespace,
        value: key.value,
    })
}

pub(super) fn map_milliseconds(
    milliseconds: i64,
    resource: &'static str,
) -> Result<OffsetDateTime, AdapterProcessError> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(milliseconds) * 1_000_000)
        .map_err(|_| AdapterProcessError::InvalidResponse(resource))
}

pub(super) fn map_attributes(
    attributes: Vec<trakkin_provider_sdk::v1::Attribute>,
) -> Result<serde_json::Value, AdapterProcessError> {
    let entries = attributes
        .into_iter()
        .map(|attribute| {
            let term = map_domain_term(attribute.term)?;
            Ok(json!({
                "term": {
                    "namespace": term.namespace(),
                    "name": term.name(),
                },
                "value": encode_adapter_value(map_contract_value(attribute.value)?),
            }))
        })
        .collect::<Result<Vec<_>, AdapterProcessError>>()?;
    Ok(json!({"entries": entries}))
}

fn encode_adapter_value(value: AdapterValue) -> serde_json::Value {
    match value {
        AdapterValue::Boolean(value) => json!({"kind": "boolean", "value": value}),
        AdapterValue::Integer(value) => json!({"kind": "integer", "value": value}),
        AdapterValue::Decimal(value) => json!({"kind": "decimal", "value": value}),
        AdapterValue::Text(value) => json!({"kind": "text", "value": value}),
        AdapterValue::Bytes(value) => json!({"kind": "bytes", "value": hex::encode(value)}),
        AdapterValue::TimestampMilliseconds(value) => {
            json!({"kind": "timestamp_milliseconds", "value": value})
        }
    }
}

pub(super) fn map_provider_item_key(
    key: Option<Key>,
) -> Result<ProviderItemKey, AdapterProcessError> {
    let key = key.ok_or(AdapterProcessError::InvalidResponse(
        "provider item key is missing",
    ))?;
    ProviderItemKey::new(key.namespace, key.value)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid provider item key"))
}

pub(super) fn map_catalog_relation_key(
    key: Option<Key>,
) -> Result<CatalogRelationKey, AdapterProcessError> {
    let key = key.ok_or(AdapterProcessError::InvalidResponse(
        "catalog relation key is missing",
    ))?;
    CatalogRelationKey::new(key.namespace, key.value)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid catalog relation key"))
}

pub(super) fn map_binary_asset_key(
    key: Option<Key>,
) -> Result<BinaryAssetKey, AdapterProcessError> {
    let key = key.ok_or(AdapterProcessError::InvalidResponse(
        "binary asset key is missing",
    ))?;
    BinaryAssetKey::new(key.namespace, key.value)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid binary asset key"))
}

pub(super) fn map_domain_term(
    term: Option<trakkin_provider_sdk::v1::Term>,
) -> Result<Term, AdapterProcessError> {
    let term = term.ok_or(AdapterProcessError::InvalidResponse("term is missing"))?;
    Term::new(term.namespace, term.name)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid term"))
}

pub(super) fn map_state_value(value: Option<Value>) -> Result<StateValue, AdapterProcessError> {
    match map_contract_value(value)? {
        AdapterValue::Boolean(value) => Ok(StateValue::Boolean(value)),
        AdapterValue::Integer(value) => Ok(StateValue::Integer(value)),
        AdapterValue::Decimal(value) => Ok(StateValue::Decimal(value)),
        AdapterValue::Text(value) => Ok(StateValue::Text(value)),
        AdapterValue::Bytes(value) => Ok(StateValue::Bytes(value)),
        AdapterValue::TimestampMilliseconds(value) => Ok(StateValue::TimestampMilliseconds(value)),
    }
}

pub(in super::super) fn map_settings(
    settings: Vec<crate::application::adapter_runtime::AdapterSetting>,
) -> Vec<ConfigurationValue> {
    settings
        .into_iter()
        .map(|setting| ConfigurationValue {
            key: setting.key,
            value: Some(map_value(setting.value)),
        })
        .collect()
}

pub(in super::super) fn map_secrets(
    secrets: Vec<crate::application::adapter_runtime::AdapterSecret>,
) -> Vec<SecretValue> {
    secrets
        .into_iter()
        .map(|secret| SecretValue {
            key: secret.key,
            value: secret.value,
        })
        .collect()
}

fn map_contract_value(value: Option<Value>) -> Result<AdapterValue, AdapterProcessError> {
    match value.and_then(|value| value.value) {
        Some(value::Value::Boolean(value)) => Ok(AdapterValue::Boolean(value)),
        Some(value::Value::Integer(value)) => Ok(AdapterValue::Integer(value)),
        Some(value::Value::Decimal(value)) => Ok(AdapterValue::Decimal(value.value)),
        Some(value::Value::Text(value)) => Ok(AdapterValue::Text(value)),
        Some(value::Value::Bytes(value)) => Ok(AdapterValue::Bytes(value)),
        Some(value::Value::Timestamp(value)) => {
            Ok(AdapterValue::TimestampMilliseconds(value.milliseconds))
        }
        None => Err(AdapterProcessError::InvalidResponse("value is missing")),
    }
}

pub(in super::super) fn map_start_authentication_response(
    response: StartAuthenticationResponse,
) -> Result<AdapterOperationResult<AdapterAuthenticationStep>, AdapterProcessError> {
    validation::start_authentication_response(&response)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid authentication step"))?;
    match response
        .outcome
        .expect("validated authentication start outcome")
    {
        start_authentication_response::Outcome::Result(progress) => Ok(Ok(progress)),
        start_authentication_response::Outcome::Error(error) => Ok(Err(map_adapter_error(error))),
    }
}

pub(in super::super) fn map_continue_authentication_response(
    response: ContinueAuthenticationResponse,
) -> Result<AdapterOperationResult<AdapterAuthenticationStep>, AdapterProcessError> {
    validation::continue_authentication_response(&response)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid authentication step"))?;
    match response
        .outcome
        .expect("validated authentication continuation outcome")
    {
        continue_authentication_response::Outcome::Result(progress) => Ok(Ok(progress)),
        continue_authentication_response::Outcome::Error(error) => {
            Ok(Err(map_adapter_error(error)))
        }
    }
}

fn validate_adapter_error(error: &OperationFailure) -> Result<(), AdapterProcessError> {
    validation::adapter_error(error)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid adapter error"))
}

pub(in super::super) fn map_adapter_error(error: OperationFailure) -> AdapterCallError {
    error.into()
}

#[cfg(test)]
mod tests {
    use trakkin_provider_sdk::v1::{OperationFailureCategory, RetryDisposition};

    use super::*;

    #[test]
    fn authentication_failures_remain_operation_outcomes() {
        let response = StartAuthenticationResponse {
            outcome: Some(start_authentication_response::Outcome::Error(
                OperationFailure {
                    category: OperationFailureCategory::Authentication as i32,
                    code: "authentication_denied".to_owned(),
                    safe_message: "Authentication was denied".to_owned(),
                    retry: Some(trakkin_provider_sdk::v1::RetryAdvice {
                        disposition: RetryDisposition::NotRetryable as i32,
                        after: None,
                    }),
                    diagnostic_id: "provider:auth:1".to_owned(),
                    ..OperationFailure::default()
                },
            )),
        };

        let outcome = map_start_authentication_response(response).unwrap();
        let error = outcome.expect_err("provider failure must not become a transport error");
        assert_eq!(error.code, "authentication_denied");
        assert_eq!(error.diagnostic_id, "provider:auth:1");
    }
}
