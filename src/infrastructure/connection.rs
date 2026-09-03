use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, Payload},
};
use serde_json::{Value, json};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::application::{
    adapter_runtime::{
        AdapterAuthenticationStatus, AdapterAuthenticationStep, AdapterConfigurationField,
        AdapterConnectionCapabilities, AdapterIdentity, AdapterKey, AdapterSecret,
        AdapterSecretPatch, AdapterSession, AdapterSetting, AdapterSourceAvailability,
        AdapterSourceCapabilities, AdapterValue,
    },
    connection::{
        BeginConnectionDeletion, ConnectionDeletionImpact, ConnectionFuture, ConnectionRepository,
        ConnectionRetirement, ConnectionSessionRegistry, ConnectionSourceSummary, ConnectionStatus,
        ConnectionSummary, CreateConnection, FinishConnectionDeletion, MarkAuthenticationCancelled,
        MarkConnectionUnavailable, PublishConnectionSnapshot, ReconfigureConnection,
        RenameConnection, SaveAuthenticationStep, StoredAuthenticationSession, StoredConnection,
        WaitForConnectionSession,
    },
    error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
};

const ENCRYPTION_VERSION: i32 = 1;

#[derive(Clone)]
pub struct ConnectionMasterKey([u8; 32]);

impl ConnectionMasterKey {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

#[derive(Clone)]
pub struct PostgresConnectionRepository {
    pool: PgPool,
    master_key: ConnectionMasterKey,
}

impl PostgresConnectionRepository {
    pub fn new(pool: PgPool, master_key: ConnectionMasterKey) -> Self {
        Self { pool, master_key }
    }

    async fn store(&self, command: CreateConnection) -> Result<(), ConnectionPersistenceError> {
        validate_connection_input(&command)?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO connections (
    id, provider_id, display_name, status, created_at, updated_at
) VALUES ($1, $2, $3, 'pending', $4, $4)
ON CONFLICT (id) DO UPDATE SET
provider_id = excluded.provider_id,
display_name = excluded.display_name,
status = 'pending',
capabilities = '{}'::JSONB,
updated_at = excluded.updated_at
",
        )
        .bind(command.connection_id)
        .bind(&command.provider_id)
        .bind(&command.display_name)
        .bind(command.created_at)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM connection_settings WHERE connection_id = $1
",
        )
        .bind(command.connection_id)
        .execute(&mut *transaction)
        .await?;
        for setting in command.settings {
            sqlx::query(
                "INSERT INTO connection_settings (
    connection_id, setting_key, value, updated_at
) VALUES ($1, $2, $3, $4)
",
            )
            .bind(command.connection_id)
            .bind(setting.key)
            .bind(encode_value(setting.value))
            .bind(command.created_at)
            .execute(&mut *transaction)
            .await?;
        }
        clear_connection_secrets(&mut transaction, command.connection_id).await?;
        for secret in command.secrets {
            set_secret(
                &mut transaction,
                &self.master_key,
                command.connection_id,
                secret.key,
                secret.value,
                command.created_at,
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn load(
        &self,
        connection_id: Uuid,
    ) -> Result<Option<StoredConnection>, ConnectionPersistenceError> {
        let mut transaction = self.pool.begin().await?;
        let Some(connection) = sqlx::query_as::<_, StoredConnectionRow>(
            "SELECT provider_id
FROM connections
WHERE id = $1 AND deleted_at IS NULL AND status <> 'disabled'
FOR SHARE
",
        )
        .bind(connection_id)
        .fetch_optional(&mut *transaction)
        .await?
        else {
            transaction.commit().await?;
            return Ok(None);
        };
        let settings = sqlx::query_as::<_, SettingRow>(
            "SELECT setting_key, value
FROM connection_settings
WHERE connection_id = $1
ORDER BY setting_key
",
        )
        .bind(connection_id)
        .fetch_all(&mut *transaction)
        .await?
        .into_iter()
        .map(|row| {
            Ok(AdapterSetting {
                key: row.setting_key,
                value: decode_value(row.value)?,
            })
        })
        .collect::<Result<_, ConnectionPersistenceError>>()?;
        let secrets = sqlx::query_as::<_, SecretRow>(
            "SELECT
    bindings.setting_key, secrets.nonce, secrets.ciphertext,
    secrets.encryption_version
FROM connection_secret_bindings AS bindings
INNER JOIN encrypted_secrets AS secrets ON secrets.id = bindings.secret_id
WHERE bindings.connection_id = $1
ORDER BY bindings.setting_key
",
        )
        .bind(connection_id)
        .fetch_all(&mut *transaction)
        .await?
        .into_iter()
        .map(|row| {
            if row.encryption_version != ENCRYPTION_VERSION {
                return Err(ConnectionPersistenceError::UnsupportedEncryptionVersion(
                    row.encryption_version,
                ));
            }
            Ok(AdapterSecret {
                value: decrypt_secret(
                    &self.master_key,
                    connection_id,
                    &row.setting_key,
                    &row.nonce,
                    &row.ciphertext,
                )?,
                key: row.setting_key,
            })
        })
        .collect::<Result<_, ConnectionPersistenceError>>()?;
        let stored = StoredConnection {
            connection_id,
            provider_id: connection.provider_id,
            settings,
            secrets,
        };
        transaction.commit().await?;
        Ok(Some(stored))
    }

    async fn publish(
        &self,
        command: PublishConnectionSnapshot,
    ) -> Result<(), ConnectionPersistenceError> {
        validate_snapshot(&command)?;
        let mut transaction = self.pool.begin().await?;
        let connection = sqlx::query_scalar::<_, Uuid>(
            "SELECT id
FROM connections
WHERE id = $1 AND deleted_at IS NULL AND status <> 'disabled'
FOR UPDATE
",
        )
        .bind(command.connection_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if connection.is_none() {
            return Err(ConnectionPersistenceError::ConnectionNotFound);
        }
        sqlx::query(
            "UPDATE connections
SET
    status = 'ready',
    capabilities = $1,
    capability_revision = capability_revision + 1,
    updated_at = $2
WHERE id = $3
",
        )
        .bind(encode_connection_capabilities(&command.opened.capabilities))
        .bind(command.published_at)
        .bind(command.connection_id)
        .execute(&mut *transaction)
        .await?;

        let existing_accounts = sqlx::query_as::<_, ExistingProviderRow>(
            "SELECT id, provider_key_namespace, provider_key_value
FROM accounts
WHERE connection_id = $1
",
        )
        .bind(command.connection_id)
        .fetch_all(&mut *transaction)
        .await?;
        let existing_account_ids = existing_provider_ids(existing_accounts);
        let mut account_ids = HashMap::new();
        for account in command.opened.accounts {
            let provider_key = provider_key(&account.key);
            let account_id = existing_account_ids
                .get(&provider_key)
                .copied()
                .unwrap_or(generate_uuid()?);
            sqlx::query(
                "INSERT INTO accounts (
    id, connection_id, provider_key_namespace, provider_key_value,
    display_name, first_seen_at, last_seen_at
) VALUES ($1, $2, $3, $4, $5, $6, $6)
ON CONFLICT (connection_id, provider_key_namespace, provider_key_value)
DO UPDATE SET
display_name = excluded.display_name,
last_seen_at = excluded.last_seen_at,
removed_at = NULL
",
            )
            .bind(account_id)
            .bind(command.connection_id)
            .bind(&account.key.namespace)
            .bind(&account.key.value)
            .bind(account.display_name)
            .bind(command.published_at)
            .execute(&mut *transaction)
            .await?;
            account_ids.insert(provider_key, account_id);
        }
        for (provider_key, account_id) in &existing_account_ids {
            if !account_ids.contains_key(provider_key) {
                sqlx::query(
                    "UPDATE accounts
SET removed_at = $1, last_seen_at = $1
WHERE id = $2
",
                )
                .bind(command.published_at)
                .bind(account_id)
                .execute(&mut *transaction)
                .await?;
            }
        }

        let existing_sources = sqlx::query_as::<_, ExistingProviderRow>(
            "SELECT id, provider_key_namespace, provider_key_value
FROM sources
WHERE connection_id = $1
",
        )
        .bind(command.connection_id)
        .fetch_all(&mut *transaction)
        .await?;
        let existing_source_ids = existing_provider_ids(existing_sources);
        let mut seen_source_keys = HashSet::new();
        for source in command.discovered.sources {
            let source_provider_key = provider_key(&source.key);
            let source_id = existing_source_ids
                .get(&source_provider_key)
                .copied()
                .unwrap_or(generate_uuid()?);
            let account_id = account_ids
                .get(&provider_key(&source.account_key))
                .copied()
                .ok_or(ConnectionPersistenceError::UnknownSourceAccount)?;
            let (availability, removed_at) = match source.availability {
                AdapterSourceAvailability::Available => ("available", None),
                AdapterSourceAvailability::Unavailable => ("unavailable", None),
                AdapterSourceAvailability::Removed => ("removed", Some(command.published_at)),
            };
            sqlx::query(
                "INSERT INTO sources (
    id, connection_id, account_id, provider_key_namespace,
    provider_key_value, display_name, kind_namespace, kind_name,
    availability, capabilities, first_seen_at, last_seen_at, removed_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11, $12
)
ON CONFLICT (connection_id, provider_key_namespace, provider_key_value)
DO UPDATE SET
account_id = excluded.account_id,
display_name = excluded.display_name,
kind_namespace = excluded.kind_namespace,
kind_name = excluded.kind_name,
availability = excluded.availability,
capabilities = excluded.capabilities,
capability_revision = sources.capability_revision + 1,
last_seen_at = excluded.last_seen_at,
removed_at = excluded.removed_at
",
            )
            .bind(source_id)
            .bind(command.connection_id)
            .bind(account_id)
            .bind(&source.key.namespace)
            .bind(&source.key.value)
            .bind(source.display_name)
            .bind(source.kind.namespace)
            .bind(source.kind.name)
            .bind(availability)
            .bind(encode_source_capabilities(&source.capabilities))
            .bind(command.published_at)
            .bind(removed_at)
            .execute(&mut *transaction)
            .await?;
            for stream_kind in ["catalog", "state"] {
                sqlx::query(
                    "INSERT INTO source_streams (
    source_id, stream_kind, status, updated_at
) VALUES ($1, $2, 'unavailable', $3)
ON CONFLICT (source_id, stream_kind) DO NOTHING
",
                )
                .bind(source_id)
                .bind(stream_kind)
                .bind(command.published_at)
                .execute(&mut *transaction)
                .await?;
            }
            seen_source_keys.insert(source_provider_key);
        }
        for (provider_key, source_id) in existing_source_ids {
            if !seen_source_keys.contains(&provider_key) {
                sqlx::query(
                    "UPDATE sources
SET availability = 'removed', removed_at = $1, last_seen_at = $1
WHERE id = $2
",
                )
                .bind(command.published_at)
                .bind(source_id)
                .execute(&mut *transaction)
                .await?;
            }
        }

        let patches = command
            .opened
            .secret_patches
            .into_iter()
            .chain(command.discovered.secret_patches)
            .collect::<Vec<_>>();
        apply_secret_patches(
            &mut transaction,
            &self.master_key,
            command.connection_id,
            patches,
            command.published_at,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn mark_unavailable(
        &self,
        command: MarkConnectionUnavailable,
    ) -> Result<(), ConnectionPersistenceError> {
        let result = sqlx::query(
            "UPDATE connections
SET status = 'unavailable', updated_at = $1
WHERE id = $2 AND deleted_at IS NULL AND status <> 'disabled'
",
        )
        .bind(command.updated_at)
        .bind(command.connection_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(ConnectionPersistenceError::ConnectionNotFound);
        }
        Ok(())
    }

    async fn save_authentication(
        &self,
        command: SaveAuthenticationStep,
    ) -> Result<(), ConnectionPersistenceError> {
        if command.method.is_empty()
            || command.step.authentication_id.is_empty()
            || command.expires_at <= command.updated_at
        {
            return Err(ConnectionPersistenceError::InvalidAuthenticationSession);
        }
        validate_named_values(
            command
                .step
                .secret_patches
                .iter()
                .map(|patch| patch.key.as_str()),
            "authentication secret patch",
        )?;
        let status = authentication_status(command.step.status);
        let step = encode_authentication_step(&command.step);
        let mut transaction = self.pool.begin().await?;
        let connection_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
    SELECT 1
    FROM connections
    WHERE id = $1 AND deleted_at IS NULL AND status <> 'disabled'
    FOR UPDATE
)
",
        )
        .bind(command.connection_id)
        .fetch_one(&mut *transaction)
        .await?;
        if !connection_exists {
            return Err(ConnectionPersistenceError::ConnectionNotFound);
        }
        let result = sqlx::query(
            "INSERT INTO authentication_sessions (
    id, connection_id, provider_authentication_id, method, status,
    step, expires_at, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
ON CONFLICT (id) DO UPDATE SET
provider_authentication_id = excluded.provider_authentication_id,
method = excluded.method,
status = excluded.status,
step = excluded.step,
expires_at = excluded.expires_at,
updated_at = excluded.updated_at
WHERE authentication_sessions.connection_id = excluded.connection_id
",
        )
        .bind(command.authentication_id)
        .bind(command.connection_id)
        .bind(&command.step.authentication_id)
        .bind(command.method)
        .bind(status)
        .bind(step)
        .bind(command.expires_at)
        .bind(command.updated_at)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(ConnectionPersistenceError::AuthenticationConnectionMismatch);
        }
        apply_secret_patches(
            &mut transaction,
            &self.master_key,
            command.connection_id,
            command.step.secret_patches,
            command.updated_at,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn load_authentication(
        &self,
        authentication_id: Uuid,
    ) -> Result<Option<StoredAuthenticationSession>, ConnectionPersistenceError> {
        let session = sqlx::query_as::<_, AuthenticationSessionRow>(
            "SELECT
    id, connection_id, provider_authentication_id, method, status, expires_at
FROM authentication_sessions
WHERE id = $1
",
        )
        .bind(authentication_id)
        .fetch_optional(&self.pool)
        .await?
        .map(
            |row| -> Result<StoredAuthenticationSession, ConnectionPersistenceError> {
                Ok(StoredAuthenticationSession {
                    authentication_id: row.id,
                    connection_id: row.connection_id,
                    provider_authentication_id: row.provider_authentication_id,
                    method: row.method,
                    status: parse_authentication_status(&row.status)?,
                    expires_at: row.expires_at,
                })
            },
        )
        .transpose()?;
        Ok(session)
    }

    async fn cancel_authentication(
        &self,
        command: MarkAuthenticationCancelled,
    ) -> Result<(), ConnectionPersistenceError> {
        let result = sqlx::query(
            "UPDATE authentication_sessions
SET
    status = 'cancelled',
    step = jsonb_set(step, '{status}', '\"cancelled\"'::JSONB),
    updated_at = $1
WHERE id = $2 AND status IN ('waiting', 'input_required')
",
        )
        .bind(command.cancelled_at)
        .bind(command.authentication_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(ConnectionPersistenceError::AuthenticationNotActive);
        }
        Ok(())
    }

    async fn list_summaries(&self) -> Result<Vec<ConnectionSummary>, ConnectionPersistenceError> {
        sqlx::query_as::<_, ConnectionSummaryRow>(
            "SELECT
    id, provider_id, display_name, status, capability_revision,
    created_at, updated_at
FROM connections
WHERE deleted_at IS NULL
ORDER BY created_at, id
",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(ConnectionSummaryRow::into_summary)
        .collect()
    }

    async fn summary(
        &self,
        connection_id: Uuid,
    ) -> Result<Option<ConnectionSummary>, ConnectionPersistenceError> {
        sqlx::query_as::<_, ConnectionSummaryRow>(
            "SELECT
    id, provider_id, display_name, status, capability_revision,
    created_at, updated_at
FROM connections
WHERE id = $1 AND deleted_at IS NULL
",
        )
        .bind(connection_id)
        .fetch_optional(&self.pool)
        .await?
        .map(ConnectionSummaryRow::into_summary)
        .transpose()
    }

    async fn rename(
        &self,
        command: RenameConnection,
    ) -> Result<ConnectionSummary, ConnectionPersistenceError> {
        let display_name = command.display_name.trim();
        if display_name.is_empty() {
            return Err(ConnectionPersistenceError::InvalidConnection);
        }
        sqlx::query_as::<_, ConnectionSummaryRow>(
            "UPDATE connections
SET display_name = $1, updated_at = $2
WHERE id = $3 AND deleted_at IS NULL
RETURNING
    id, provider_id, display_name, status, capability_revision,
    created_at, updated_at
",
        )
        .bind(display_name)
        .bind(command.renamed_at)
        .bind(command.connection_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ConnectionPersistenceError::ConnectionNotFound)?
        .into_summary()
    }

    async fn reconfigure(
        &self,
        command: ReconfigureConnection,
    ) -> Result<ConnectionSummary, ConnectionPersistenceError> {
        validate_named_values(
            command.settings.iter().map(|setting| setting.key.as_str()),
            "setting",
        )?;
        validate_named_values(
            command
                .secret_patches
                .iter()
                .map(|patch| patch.key.as_str()),
            "secret patch",
        )?;
        let mut transaction = self.pool.begin().await?;
        let exists = sqlx::query_scalar::<_, Uuid>(
            "SELECT id
FROM connections
WHERE id = $1 AND deleted_at IS NULL AND status <> 'disabled'
FOR UPDATE
",
        )
        .bind(command.connection_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if exists.is_none() {
            return Err(ConnectionPersistenceError::ConnectionNotFound);
        }
        sqlx::query(
            "DELETE FROM connection_settings WHERE connection_id = $1
",
        )
        .bind(command.connection_id)
        .execute(&mut *transaction)
        .await?;
        for setting in command.settings {
            sqlx::query(
                "INSERT INTO connection_settings (
    connection_id, setting_key, value, updated_at
) VALUES ($1, $2, $3, $4)
",
            )
            .bind(command.connection_id)
            .bind(setting.key)
            .bind(encode_value(setting.value))
            .bind(command.reconfigured_at)
            .execute(&mut *transaction)
            .await?;
        }
        apply_secret_patches(
            &mut transaction,
            &self.master_key,
            command.connection_id,
            command.secret_patches,
            command.reconfigured_at,
        )
        .await?;
        let summary = sqlx::query_as::<_, ConnectionSummaryRow>(
            "UPDATE connections
SET
    status = 'pending',
    capabilities = '{}'::JSONB,
    capability_revision = capability_revision + 1,
    updated_at = $1
WHERE id = $2
RETURNING
    id, provider_id, display_name, status, capability_revision,
    created_at, updated_at
",
        )
        .bind(command.reconfigured_at)
        .bind(command.connection_id)
        .fetch_one(&mut *transaction)
        .await?
        .into_summary()?;
        transaction.commit().await?;
        Ok(summary)
    }

    async fn list_sources(
        &self,
        connection_id: Uuid,
    ) -> Result<Vec<ConnectionSourceSummary>, ConnectionPersistenceError> {
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
    SELECT 1 FROM connections
    WHERE id = $1 AND deleted_at IS NULL
)
",
        )
        .bind(connection_id)
        .fetch_one(&self.pool)
        .await?;
        if !exists {
            return Err(ConnectionPersistenceError::ConnectionNotFound);
        }
        sqlx::query_as::<_, ConnectionSourceSummaryRow>(
            "SELECT
    sources.id AS source_id,
    sources.account_id,
    accounts.display_name AS account_display_name,
    sources.display_name,
    sources.kind_namespace,
    sources.kind_name,
    sources.availability,
    sources.capability_revision
FROM sources
INNER JOIN accounts ON accounts.id = sources.account_id
WHERE
    sources.connection_id = $1
    AND sources.removed_at IS NULL
    AND accounts.removed_at IS NULL
ORDER BY accounts.display_name, sources.display_name, sources.id
",
        )
        .bind(connection_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(ConnectionSourceSummaryRow::into_summary)
        .collect()
    }

    async fn deletion_impact(
        &self,
        connection_id: Uuid,
    ) -> Result<ConnectionDeletionImpact, ConnectionPersistenceError> {
        sqlx::query_as::<_, ConnectionDeletionImpactRow>(
            "SELECT
    connections.id AS connection_id,
    (
        SELECT COUNT(*)
        FROM sources
        WHERE connection_id = connections.id
    ) AS source_count,
    (
        SELECT COUNT(*)
        FROM workspace_sources
        INNER JOIN sources ON sources.id = workspace_sources.source_id
        WHERE sources.connection_id = connections.id
    ) AS workspace_attachment_count,
    (
        SELECT COUNT(*)
        FROM tasks
        WHERE connection_id = connections.id
    ) AS task_count,
    (
        SELECT COUNT(*) FROM tasks
        WHERE
            connection_id = connections.id
            AND status IN ('queued', 'running', 'cancelling')
    ) AS active_task_count,
    (
        SELECT COUNT(*) FROM authentication_sessions
        WHERE connection_id = connections.id
    ) AS authentication_session_count
FROM connections
WHERE connections.id = $1 AND connections.deleted_at IS NULL
",
        )
        .bind(connection_id)
        .fetch_optional(&self.pool)
        .await?
        .map(ConnectionDeletionImpactRow::into_impact)
        .ok_or(ConnectionPersistenceError::ConnectionNotFound)
    }

    async fn begin_deletion(
        &self,
        command: BeginConnectionDeletion,
    ) -> Result<ConnectionDeletionImpact, ConnectionPersistenceError> {
        let mut transaction = self.pool.begin().await?;
        let exists = sqlx::query_scalar::<_, Uuid>(
            "SELECT id
FROM connections
WHERE id = $1 AND deleted_at IS NULL
FOR UPDATE
",
        )
        .bind(command.connection_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if exists.is_none() {
            return Err(ConnectionPersistenceError::ConnectionNotFound);
        }
        let impact = connection_deletion_impact(&mut transaction, command.connection_id).await?;
        if impact.active_task_count > 0 {
            return Err(ConnectionPersistenceError::ConnectionBusy);
        }
        sqlx::query(
            "UPDATE connections
SET status = 'disabled', updated_at = $1
WHERE id = $2
",
        )
        .bind(command.disabled_at)
        .bind(command.connection_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(impact)
    }

    async fn finish_deletion(
        &self,
        command: FinishConnectionDeletion,
    ) -> Result<(), ConnectionPersistenceError> {
        let mut transaction = self.pool.begin().await?;
        let status = sqlx::query_scalar::<_, String>(
            "SELECT status
FROM connections
WHERE id = $1 AND deleted_at IS NULL
FOR UPDATE
",
        )
        .bind(command.connection_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ConnectionPersistenceError::ConnectionNotFound)?;
        if status != "disabled" {
            return Err(ConnectionPersistenceError::DeletionNotFenced);
        }
        clear_connection_secrets(&mut transaction, command.connection_id).await?;
        sqlx::query(
            "DELETE FROM connections
WHERE id = $1 AND status = 'disabled'
",
        )
        .bind(command.connection_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}

impl ConnectionRepository for PostgresConnectionRepository {
    type Error = ConnectionPersistenceError;

    fn store_connection(&self, command: CreateConnection) -> ConnectionFuture<'_, (), Self::Error> {
        Box::pin(self.store(command))
    }

    fn load_connection(
        &self,
        connection_id: Uuid,
    ) -> ConnectionFuture<'_, Option<StoredConnection>, Self::Error> {
        Box::pin(self.load(connection_id))
    }

    fn publish_connection_snapshot(
        &self,
        command: PublishConnectionSnapshot,
    ) -> ConnectionFuture<'_, (), Self::Error> {
        Box::pin(self.publish(command))
    }

    fn mark_connection_unavailable(
        &self,
        command: MarkConnectionUnavailable,
    ) -> ConnectionFuture<'_, (), Self::Error> {
        Box::pin(self.mark_unavailable(command))
    }

    fn save_authentication_step(
        &self,
        command: SaveAuthenticationStep,
    ) -> ConnectionFuture<'_, (), Self::Error> {
        Box::pin(self.save_authentication(command))
    }

    fn load_authentication_session(
        &self,
        authentication_id: Uuid,
    ) -> ConnectionFuture<'_, Option<StoredAuthenticationSession>, Self::Error> {
        Box::pin(self.load_authentication(authentication_id))
    }

    fn mark_authentication_cancelled(
        &self,
        command: MarkAuthenticationCancelled,
    ) -> ConnectionFuture<'_, (), Self::Error> {
        Box::pin(self.cancel_authentication(command))
    }

    fn list_connections(&self) -> ConnectionFuture<'_, Vec<ConnectionSummary>, Self::Error> {
        Box::pin(self.list_summaries())
    }

    fn get_connection_summary(
        &self,
        connection_id: Uuid,
    ) -> ConnectionFuture<'_, Option<ConnectionSummary>, Self::Error> {
        Box::pin(self.summary(connection_id))
    }

    fn rename_connection(
        &self,
        command: RenameConnection,
    ) -> ConnectionFuture<'_, ConnectionSummary, Self::Error> {
        Box::pin(self.rename(command))
    }

    fn reconfigure_connection(
        &self,
        command: ReconfigureConnection,
    ) -> ConnectionFuture<'_, ConnectionSummary, Self::Error> {
        Box::pin(self.reconfigure(command))
    }

    fn list_connection_sources(
        &self,
        connection_id: Uuid,
    ) -> ConnectionFuture<'_, Vec<ConnectionSourceSummary>, Self::Error> {
        Box::pin(self.list_sources(connection_id))
    }

    fn connection_deletion_impact(
        &self,
        connection_id: Uuid,
    ) -> ConnectionFuture<'_, ConnectionDeletionImpact, Self::Error> {
        Box::pin(self.deletion_impact(connection_id))
    }

    fn begin_connection_deletion(
        &self,
        command: BeginConnectionDeletion,
    ) -> ConnectionFuture<'_, ConnectionDeletionImpact, Self::Error> {
        Box::pin(self.begin_deletion(command))
    }

    fn finish_connection_deletion(
        &self,
        command: FinishConnectionDeletion,
    ) -> ConnectionFuture<'_, (), Self::Error> {
        Box::pin(self.finish_deletion(command))
    }
}

pub struct ProcessSessionRegistry<S> {
    sessions: Arc<Mutex<HashMap<Uuid, S>>>,
    session_available: Arc<Notify>,
}

impl<S> Clone for ProcessSessionRegistry<S> {
    fn clone(&self) -> Self {
        Self {
            sessions: Arc::clone(&self.sessions),
            session_available: Arc::clone(&self.session_available),
        }
    }
}

impl<S> Default for ProcessSessionRegistry<S> {
    fn default() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            session_available: Arc::new(Notify::new()),
        }
    }
}

impl<S> ProcessSessionRegistry<S> {
    pub fn contains(&self, connection_id: Uuid) -> bool {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&connection_id)
    }

    pub fn len(&self) -> usize {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<S> ProcessSessionRegistry<S>
where
    S: AdapterSession,
{
    pub async fn retire_all(&self) -> Result<(), ConnectionSessionRetirementError> {
        let sessions = {
            let mut sessions = self
                .sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            sessions
                .drain()
                .map(|(_, session)| session)
                .collect::<Vec<_>>()
        };
        let mut failed = false;
        for session in sessions {
            if session.retire().await.is_err() {
                failed = true;
            }
        }
        if failed {
            Err(ConnectionSessionRetirementError)
        } else {
            Ok(())
        }
    }
}

impl<S> ConnectionSessionRegistry<S> for ProcessSessionRegistry<S>
where
    S: AdapterSession + Send,
{
    fn replace(&self, connection_id: Uuid, session: S) -> Option<S> {
        let previous = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(connection_id, session);
        if previous.is_none() {
            self.session_available.notify_waiters();
        }
        previous
    }

    fn remove(&self, connection_id: Uuid) -> Option<S> {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&connection_id)
    }

    fn identity(&self, connection_id: Uuid) -> Option<AdapterIdentity> {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&connection_id)
            .map(|session| session.identity().clone())
    }
}

impl<S> WaitForConnectionSession<S> for ProcessSessionRegistry<S>
where
    S: AdapterSession + Send,
{
    fn wait_for_session(
        &self,
        connection_id: Uuid,
    ) -> crate::application::connection::ConnectionSessionFuture<'_, S> {
        Box::pin(async move {
            loop {
                let notification = self.session_available.notified();
                tokio::pin!(notification);
                notification.as_mut().enable();
                if let Some(session) = self.remove(connection_id) {
                    return session;
                }
                notification.await;
            }
        })
    }
}

#[derive(Clone, Copy, Debug, Error)]
#[error("connection process could not be retired")]
pub struct ConnectionSessionRetirementError;

impl<S> ConnectionRetirement for ProcessSessionRegistry<S>
where
    S: AdapterSession + 'static,
{
    type Error = ConnectionSessionRetirementError;

    fn retire_connection(&self, connection_id: Uuid) -> ConnectionFuture<'_, (), Self::Error> {
        let session = ConnectionSessionRegistry::remove(self, connection_id);
        Box::pin(async move {
            if let Some(session) = session {
                session
                    .retire()
                    .await
                    .map_err(|_| ConnectionSessionRetirementError)?;
            }
            Ok(())
        })
    }
}

impl IntoApplicationError for ConnectionSessionRetirementError {
    fn into_application_error(self) -> ApplicationError {
        ApplicationError::new(
            ApplicationErrorCode::Internal,
            "connection process could not be retired",
        )
    }
}

fn validate_connection_input(command: &CreateConnection) -> Result<(), ConnectionPersistenceError> {
    if command.provider_id.is_empty() || command.display_name.is_empty() {
        return Err(ConnectionPersistenceError::InvalidConnection);
    }
    validate_named_values(
        command.settings.iter().map(|setting| setting.key.as_str()),
        "setting",
    )?;
    validate_named_values(
        command.secrets.iter().map(|secret| secret.key.as_str()),
        "secret",
    )?;
    Ok(())
}

fn validate_snapshot(
    command: &PublishConnectionSnapshot,
) -> Result<(), ConnectionPersistenceError> {
    let account_keys = command
        .opened
        .accounts
        .iter()
        .map(|account| provider_key(&account.key))
        .collect::<HashSet<_>>();
    if account_keys.len() != command.opened.accounts.len() {
        return Err(ConnectionPersistenceError::DuplicateProviderKey("account"));
    }
    let source_keys = command
        .discovered
        .sources
        .iter()
        .map(|source| provider_key(&source.key))
        .collect::<HashSet<_>>();
    if source_keys.len() != command.discovered.sources.len() {
        return Err(ConnectionPersistenceError::DuplicateProviderKey("source"));
    }
    validate_named_values(
        command
            .opened
            .secret_patches
            .iter()
            .chain(&command.discovered.secret_patches)
            .map(|patch| patch.key.as_str()),
        "secret patch",
    )?;
    Ok(())
}

fn validate_named_values<'a>(
    values: impl IntoIterator<Item = &'a str>,
    resource: &'static str,
) -> Result<(), ConnectionPersistenceError> {
    let mut seen = HashSet::new();
    for value in values {
        if value.is_empty() {
            return Err(ConnectionPersistenceError::EmptyKey(resource));
        }
        if !seen.insert(value) {
            return Err(ConnectionPersistenceError::DuplicateKey(resource));
        }
    }
    Ok(())
}

async fn clear_connection_secrets(
    transaction: &mut Transaction<'_, Postgres>,
    connection_id: Uuid,
) -> Result<(), ConnectionPersistenceError> {
    let secret_ids = sqlx::query_scalar::<_, Uuid>(
        "DELETE FROM connection_secret_bindings
WHERE connection_id = $1
RETURNING secret_id
",
    )
    .bind(connection_id)
    .fetch_all(&mut **transaction)
    .await?;
    if !secret_ids.is_empty() {
        sqlx::query(
            "DELETE FROM encrypted_secrets WHERE id = ANY($1)
",
        )
        .bind(&secret_ids)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

async fn apply_secret_patches(
    transaction: &mut Transaction<'_, Postgres>,
    master_key: &ConnectionMasterKey,
    connection_id: Uuid,
    patches: Vec<AdapterSecretPatch>,
    updated_at: OffsetDateTime,
) -> Result<(), ConnectionPersistenceError> {
    for patch in patches {
        match patch.value {
            Some(value) => {
                set_secret(
                    transaction,
                    master_key,
                    connection_id,
                    patch.key,
                    value,
                    updated_at,
                )
                .await?;
            }
            None => {
                let secret_id = sqlx::query_scalar::<_, Uuid>(
                    "DELETE FROM connection_secret_bindings
WHERE connection_id = $1 AND setting_key = $2
RETURNING secret_id
",
                )
                .bind(connection_id)
                .bind(patch.key)
                .fetch_optional(&mut **transaction)
                .await?;
                if let Some(secret_id) = secret_id {
                    sqlx::query(
                        "DELETE FROM encrypted_secrets WHERE id = $1
",
                    )
                    .bind(secret_id)
                    .execute(&mut **transaction)
                    .await?;
                }
            }
        }
    }
    Ok(())
}

async fn set_secret(
    transaction: &mut Transaction<'_, Postgres>,
    master_key: &ConnectionMasterKey,
    connection_id: Uuid,
    key: String,
    value: Vec<u8>,
    updated_at: OffsetDateTime,
) -> Result<(), ConnectionPersistenceError> {
    let previous_secret_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT secret_id
FROM connection_secret_bindings
WHERE connection_id = $1 AND setting_key = $2
FOR UPDATE
",
    )
    .bind(connection_id)
    .bind(&key)
    .fetch_optional(&mut **transaction)
    .await?;
    let secret_id = generate_uuid()?;
    let (nonce, ciphertext) = encrypt_secret(master_key, connection_id, &key, &value)?;
    sqlx::query(
        "INSERT INTO encrypted_secrets (
    id, nonce, ciphertext, encryption_version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $5)
",
    )
    .bind(secret_id)
    .bind(nonce)
    .bind(ciphertext)
    .bind(ENCRYPTION_VERSION)
    .bind(updated_at)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO connection_secret_bindings (
    connection_id, setting_key, secret_id, updated_at
) VALUES ($1, $2, $3, $4)
ON CONFLICT (connection_id, setting_key) DO UPDATE SET
secret_id = excluded.secret_id,
updated_at = excluded.updated_at
",
    )
    .bind(connection_id)
    .bind(key)
    .bind(secret_id)
    .bind(updated_at)
    .execute(&mut **transaction)
    .await?;
    if let Some(previous_secret_id) = previous_secret_id {
        sqlx::query(
            "DELETE FROM encrypted_secrets WHERE id = $1
",
        )
        .bind(previous_secret_id)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

fn encrypt_secret(
    master_key: &ConnectionMasterKey,
    connection_id: Uuid,
    setting_key: &str,
    plaintext: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), ConnectionPersistenceError> {
    let mut nonce = [0_u8; 24];
    getrandom::fill(&mut nonce)?;
    let cipher = XChaCha20Poly1305::new((&master_key.0).into());
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &secret_associated_data(connection_id, setting_key),
            },
        )
        .map_err(|_| ConnectionPersistenceError::Encryption)?;
    Ok((nonce.to_vec(), ciphertext))
}

fn decrypt_secret(
    master_key: &ConnectionMasterKey,
    connection_id: Uuid,
    setting_key: &str,
    nonce: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, ConnectionPersistenceError> {
    let nonce =
        <&[u8; 24]>::try_from(nonce).map_err(|_| ConnectionPersistenceError::InvalidNonceLength)?;
    let cipher = XChaCha20Poly1305::new((&master_key.0).into());
    cipher
        .decrypt(
            XNonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad: &secret_associated_data(connection_id, setting_key),
            },
        )
        .map_err(|_| ConnectionPersistenceError::Decryption)
}

fn secret_associated_data(connection_id: Uuid, setting_key: &str) -> Vec<u8> {
    let mut data = Vec::with_capacity(16 + setting_key.len());
    data.extend_from_slice(connection_id.as_bytes());
    data.extend_from_slice(setting_key.as_bytes());
    data
}

fn generate_uuid() -> Result<Uuid, ConnectionPersistenceError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(bytes))
}

fn provider_key(key: &AdapterKey) -> (String, Vec<u8>) {
    (key.namespace.clone(), key.value.clone())
}

fn existing_provider_ids(rows: Vec<ExistingProviderRow>) -> HashMap<(String, Vec<u8>), Uuid> {
    rows.into_iter()
        .map(|row| ((row.provider_key_namespace, row.provider_key_value), row.id))
        .collect()
}

fn encode_value(value: AdapterValue) -> Value {
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

fn decode_value(value: Value) -> Result<AdapterValue, ConnectionPersistenceError> {
    let kind = value
        .get("kind")
        .and_then(Value::as_str)
        .ok_or(ConnectionPersistenceError::InvalidSettingValue)?;
    let value = value
        .get("value")
        .ok_or(ConnectionPersistenceError::InvalidSettingValue)?;
    match kind {
        "boolean" => value
            .as_bool()
            .map(AdapterValue::Boolean)
            .ok_or(ConnectionPersistenceError::InvalidSettingValue),
        "integer" => value
            .as_i64()
            .map(AdapterValue::Integer)
            .ok_or(ConnectionPersistenceError::InvalidSettingValue),
        "decimal" => value
            .as_str()
            .map(|value| AdapterValue::Decimal(value.to_owned()))
            .ok_or(ConnectionPersistenceError::InvalidSettingValue),
        "text" => value
            .as_str()
            .map(|value| AdapterValue::Text(value.to_owned()))
            .ok_or(ConnectionPersistenceError::InvalidSettingValue),
        "bytes" => value
            .as_str()
            .ok_or(ConnectionPersistenceError::InvalidSettingValue)
            .and_then(|value| {
                hex::decode(value)
                    .map(AdapterValue::Bytes)
                    .map_err(|_| ConnectionPersistenceError::InvalidSettingValue)
            }),
        "timestamp_milliseconds" => value
            .as_i64()
            .map(AdapterValue::TimestampMilliseconds)
            .ok_or(ConnectionPersistenceError::InvalidSettingValue),
        _ => Err(ConnectionPersistenceError::InvalidSettingValue),
    }
}

fn encode_connection_capabilities(capabilities: &AdapterConnectionCapabilities) -> Value {
    json!({
        "referenceLookup": capabilities.reference_lookup.as_ref().map(|lookup| json!({
            "referenceNamespaces": lookup.reference_namespaces,
            "maximumBatchSize": lookup.maximum_batch_size,
        })),
        "endpointLookup": capabilities.endpoint_lookup.as_ref().map(|lookup| json!({
            "referenceNamespaces": lookup.reference_namespaces,
            "coordinateIds": lookup.coordinate_ids,
            "maximumBatchSize": lookup.maximum_batch_size,
            "maximumResponseBytes": lookup.maximum_response_bytes,
        })),
    })
}

fn encode_source_capabilities(capabilities: &AdapterSourceCapabilities) -> Value {
    json!({
        "catalog": capabilities.catalog.as_ref().map(|read| json!({
            "full": read.full,
            "incremental": read.incremental,
        })),
        "state": capabilities.state.as_ref().map(|read| json!({
            "full": read.full,
            "incremental": read.incremental,
        })),
        "stateFields": capabilities.state_fields.iter().map(|field| json!({
            "field": {"namespace": field.field.namespace, "name": field.field.name},
            "unit": field.unit.as_ref().map(|unit| json!({
                "namespace": unit.namespace,
                "name": unit.name,
            })),
            "valueKind": format!("{:?}", field.value_kind).to_lowercase(),
            "ratingScale": field.rating_scale.as_ref().map(|scale| json!({
                "minimum": scale.minimum,
                "maximum": scale.maximum,
                "step": scale.step,
            })),
            "numericRange": field.numeric_range.as_ref().map(|range| json!({
                "minimum": range.minimum,
                "maximum": range.maximum,
                "step": range.step,
            })),
            "quantizer": field.quantizer.map(|quantizer| quantizer.as_str()),
        })).collect::<Vec<_>>(),
        "assets": capabilities.assets.as_ref().map(|assets| json!({
            "maximumBytes": assets.maximum_bytes,
            "contentTypes": assets.content_types,
        })),
        "coordinateIds": capabilities.coordinate_ids,
        "targetedStateRead": capabilities.targeted_state_read.as_ref().map(|read| json!({
            "maximumFields": read.maximum_fields,
            "maximumResponseBytes": read.maximum_response_bytes,
        })),
        "targetedStateWrite": capabilities.targeted_state_write.as_ref().map(|write| json!({
            "fields": write.fields.iter().map(|field| json!({
                "field": {
                    "namespace": field.field.field().namespace(),
                    "name": field.field.field().name(),
                },
                "unit": field.field.unit().map(|unit| json!({
                    "namespace": unit.namespace(),
                    "name": unit.name(),
                })),
                "setSupported": field.set_supported,
                "clearSupported": field.clear_supported,
            })).collect::<Vec<_>>(),
            "mayCreateSourceMembership": write.may_create_source_membership,
            "preconditionMode": write.precondition_mode.map(|mode| match mode {
                crate::application::adapter_runtime::AdapterTargetedStateWritePreconditionMode::ProviderToken => "provider_token",
                crate::application::adapter_runtime::AdapterTargetedStateWritePreconditionMode::HostRecheckOnly => "host_recheck_only",
            }),
            "idempotencyMode": write.idempotency_mode.map(|mode| match mode {
                crate::application::adapter_runtime::AdapterTargetedStateWriteIdempotencyMode::NotSupported => "not_supported",
                crate::application::adapter_runtime::AdapterTargetedStateWriteIdempotencyMode::StableKey => "stable_key",
            }),
            "maximumFields": write.maximum_fields,
            "maximumRequestBytes": write.maximum_request_bytes,
            "maximumResponseBytes": write.maximum_response_bytes,
            "maximumReceiptBytes": write.maximum_receipt_bytes,
        })),
    })
}

fn authentication_status(status: AdapterAuthenticationStatus) -> &'static str {
    match status {
        AdapterAuthenticationStatus::Waiting => "waiting",
        AdapterAuthenticationStatus::InputRequired => "input_required",
        AdapterAuthenticationStatus::Completed => "completed",
        AdapterAuthenticationStatus::Cancelled => "cancelled",
        AdapterAuthenticationStatus::Expired => "expired",
        AdapterAuthenticationStatus::Failed => "failed",
    }
}

fn parse_authentication_status(
    status: &str,
) -> Result<AdapterAuthenticationStatus, ConnectionPersistenceError> {
    match status {
        "waiting" => Ok(AdapterAuthenticationStatus::Waiting),
        "input_required" => Ok(AdapterAuthenticationStatus::InputRequired),
        "completed" => Ok(AdapterAuthenticationStatus::Completed),
        "cancelled" => Ok(AdapterAuthenticationStatus::Cancelled),
        "expired" => Ok(AdapterAuthenticationStatus::Expired),
        "failed" => Ok(AdapterAuthenticationStatus::Failed),
        _ => Err(ConnectionPersistenceError::InvalidAuthenticationStatus),
    }
}

fn encode_authentication_step(step: &AdapterAuthenticationStep) -> Value {
    json!({
        "providerAuthenticationId": step.authentication_id,
        "status": authentication_status(step.status),
        "prompt": step.prompt.as_ref().map(|prompt| json!({
            "message": prompt.message,
            "verificationUrl": prompt.verification_url,
            "userCode": prompt.user_code,
            "fields": prompt.fields.iter().map(encode_configuration_field).collect::<Vec<_>>(),
        })),
        "accounts": step.accounts.iter().map(|account| json!({
            "key": {
                "namespace": account.key.namespace,
                "value": hex::encode(&account.key.value),
            },
            "displayName": account.display_name,
        })).collect::<Vec<_>>(),
        "appliedSecretPatchKeys": step.secret_patches.iter().map(|patch| &patch.key).collect::<Vec<_>>(),
        "retryAfter": step.retry_after.as_ref().map(|retry_after| json!({
            "seconds": retry_after.seconds,
            "nanoseconds": retry_after.nanoseconds,
        })),
        "error": step.error.as_ref().map(|error| json!({
            "code": error.code,
            "safeMessage": error.safe_message,
            "retryable": error.retryable,
            "retryAfter": error.retry_after.as_ref().map(|retry_after| json!({
                "seconds": retry_after.seconds,
                "nanoseconds": retry_after.nanoseconds,
            })),
            "fieldProblems": error.field_problems.iter().map(|problem| json!({
                "path": problem.path,
                "code": problem.code,
                "message": problem.message,
            })).collect::<Vec<_>>(),
        })),
    })
}

fn encode_configuration_field(field: &AdapterConfigurationField) -> Value {
    json!({
        "key": field.key,
        "displayName": field.display_name,
        "valueKind": format!("{:?}", field.value_kind).to_lowercase(),
        "required": field.required,
        "secret": field.secret,
        "options": field.options.iter().map(|option| json!({
            "value": encode_value(option.value.clone()),
            "displayName": option.display_name,
        })).collect::<Vec<_>>(),
        "defaultValue": field.default_value.clone().map(encode_value),
        "placeholder": field.placeholder,
    })
}

#[derive(FromRow)]
struct StoredConnectionRow {
    provider_id: String,
}

#[derive(FromRow)]
struct SettingRow {
    setting_key: String,
    value: Value,
}

#[derive(FromRow)]
struct SecretRow {
    setting_key: String,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
    encryption_version: i32,
}

#[derive(FromRow)]
struct ExistingProviderRow {
    id: Uuid,
    provider_key_namespace: String,
    provider_key_value: Vec<u8>,
}

#[derive(FromRow)]
struct AuthenticationSessionRow {
    id: Uuid,
    connection_id: Uuid,
    provider_authentication_id: String,
    method: String,
    status: String,
    expires_at: OffsetDateTime,
}

#[derive(FromRow)]
struct ConnectionSummaryRow {
    id: Uuid,
    provider_id: String,
    display_name: String,
    status: String,
    capability_revision: i64,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl ConnectionSummaryRow {
    fn into_summary(self) -> Result<ConnectionSummary, ConnectionPersistenceError> {
        Ok(ConnectionSummary {
            connection_id: self.id,
            provider_id: self.provider_id,
            display_name: self.display_name,
            status: parse_connection_status(&self.status)?,
            capability_revision: self.capability_revision,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

#[derive(FromRow)]
struct ConnectionSourceSummaryRow {
    source_id: Uuid,
    account_id: Uuid,
    account_display_name: String,
    display_name: String,
    kind_namespace: String,
    kind_name: String,
    availability: String,
    capability_revision: i64,
}

impl ConnectionSourceSummaryRow {
    fn into_summary(self) -> Result<ConnectionSourceSummary, ConnectionPersistenceError> {
        Ok(ConnectionSourceSummary {
            source_id: self.source_id,
            account_id: self.account_id,
            account_display_name: self.account_display_name,
            display_name: self.display_name,
            kind: super::super::application::adapter_runtime::AdapterTerm {
                namespace: self.kind_namespace,
                name: self.kind_name,
            },
            availability: parse_source_availability(&self.availability)?,
            capability_revision: self.capability_revision,
        })
    }
}

#[derive(FromRow)]
struct ConnectionDeletionImpactRow {
    connection_id: Uuid,
    source_count: i64,
    workspace_attachment_count: i64,
    task_count: i64,
    active_task_count: i64,
    authentication_session_count: i64,
}

impl ConnectionDeletionImpactRow {
    fn into_impact(self) -> ConnectionDeletionImpact {
        ConnectionDeletionImpact {
            connection_id: self.connection_id,
            source_count: self.source_count,
            workspace_attachment_count: self.workspace_attachment_count,
            task_count: self.task_count,
            active_task_count: self.active_task_count,
            authentication_session_count: self.authentication_session_count,
        }
    }
}

#[derive(Debug, Error)]
pub enum ConnectionPersistenceError {
    #[error("connection does not exist")]
    ConnectionNotFound,
    #[error("connection has active durable tasks")]
    ConnectionBusy,
    #[error("connection deletion was not fenced")]
    DeletionNotFenced,
    #[error("connection configuration is invalid")]
    InvalidConnection,
    #[error("connection {0} key is empty")]
    EmptyKey(&'static str),
    #[error("connection {0} key is duplicated")]
    DuplicateKey(&'static str),
    #[error("provider {0} key is duplicated")]
    DuplicateProviderKey(&'static str),
    #[error("source references an account absent from the open snapshot")]
    UnknownSourceAccount,
    #[error("stored connection setting is invalid")]
    InvalidSettingValue,
    #[error("authentication session is invalid")]
    InvalidAuthenticationSession,
    #[error("authentication session belongs to another connection")]
    AuthenticationConnectionMismatch,
    #[error("authentication session is not active")]
    AuthenticationNotActive,
    #[error("stored authentication status is invalid")]
    InvalidAuthenticationStatus,
    #[error("secret encryption failed")]
    Encryption,
    #[error("secret decryption failed")]
    Decryption,
    #[error("stored secret nonce length is invalid")]
    InvalidNonceLength,
    #[error("stored secret encryption version {0} is unsupported")]
    UnsupportedEncryptionVersion(i32),
    #[error("secure random generation failed: {0}")]
    Random(#[from] getrandom::Error),
    #[error("connection database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}

impl IntoApplicationError for ConnectionPersistenceError {
    fn into_application_error(self) -> ApplicationError {
        match self {
            Self::ConnectionNotFound => ApplicationError::new(
                ApplicationErrorCode::ConnectionNotFound,
                "connection was not found",
            ),
            Self::ConnectionBusy => ApplicationError::new(
                ApplicationErrorCode::ConnectionBusy,
                "connection has active tasks",
            ),
            Self::InvalidConnection | Self::EmptyKey(_) | Self::DuplicateKey(_) => {
                ApplicationError::new(
                    ApplicationErrorCode::InvalidRequest,
                    "connection configuration is invalid",
                )
            }
            Self::DuplicateProviderKey(_)
            | Self::DeletionNotFenced
            | Self::UnknownSourceAccount
            | Self::InvalidSettingValue
            | Self::InvalidAuthenticationSession
            | Self::AuthenticationConnectionMismatch
            | Self::AuthenticationNotActive
            | Self::InvalidAuthenticationStatus
            | Self::Encryption
            | Self::Decryption
            | Self::InvalidNonceLength
            | Self::UnsupportedEncryptionVersion(_)
            | Self::Random(_)
            | Self::Database(_) => ApplicationError::new(
                ApplicationErrorCode::Internal,
                "connection operation failed",
            ),
        }
    }
}

async fn connection_deletion_impact(
    transaction: &mut Transaction<'_, Postgres>,
    connection_id: Uuid,
) -> Result<ConnectionDeletionImpact, ConnectionPersistenceError> {
    sqlx::query_as::<_, ConnectionDeletionImpactRow>(
        "SELECT
    connections.id AS connection_id,
    (
        SELECT COUNT(*)
        FROM sources
        WHERE connection_id = connections.id
    ) AS source_count,
    (
        SELECT COUNT(*)
        FROM workspace_sources
        INNER JOIN sources ON sources.id = workspace_sources.source_id
        WHERE sources.connection_id = connections.id
    ) AS workspace_attachment_count,
    (
        SELECT COUNT(*)
        FROM tasks
        WHERE connection_id = connections.id
    ) AS task_count,
    (
        SELECT COUNT(*) FROM tasks
        WHERE
            connection_id = connections.id
            AND status IN ('queued', 'running', 'cancelling')
    ) AS active_task_count,
    (
        SELECT COUNT(*) FROM authentication_sessions
        WHERE connection_id = connections.id
    ) AS authentication_session_count
FROM connections
WHERE connections.id = $1 AND connections.deleted_at IS NULL
",
    )
    .bind(connection_id)
    .fetch_one(&mut **transaction)
    .await
    .map(ConnectionDeletionImpactRow::into_impact)
    .map_err(ConnectionPersistenceError::Database)
}

fn parse_connection_status(status: &str) -> Result<ConnectionStatus, ConnectionPersistenceError> {
    match status {
        "pending" => Ok(ConnectionStatus::Pending),
        "ready" => Ok(ConnectionStatus::Ready),
        "unavailable" => Ok(ConnectionStatus::Unavailable),
        "disabled" => Ok(ConnectionStatus::Disabled),
        _ => Err(ConnectionPersistenceError::InvalidConnection),
    }
}

fn parse_source_availability(
    availability: &str,
) -> Result<AdapterSourceAvailability, ConnectionPersistenceError> {
    match availability {
        "available" => Ok(AdapterSourceAvailability::Available),
        "unavailable" => Ok(AdapterSourceAvailability::Unavailable),
        "removed" => Ok(AdapterSourceAvailability::Removed),
        _ => Err(ConnectionPersistenceError::InvalidConnection),
    }
}
