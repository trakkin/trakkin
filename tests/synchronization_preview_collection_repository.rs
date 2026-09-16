#[cfg(feature = "embedded-database")]
use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
};

#[cfg(feature = "embedded-database")]
use tempfile::tempdir;
#[cfg(feature = "embedded-database")]
use time::{OffsetDateTime, macros::datetime};
#[cfg(feature = "embedded-database")]
use trakkin::{
    application::{
        adapter_runtime::{
            AdapterIdentity, AdapterRetirement, AdapterSourceMembership, AdapterStatePresence,
            AdapterTargetedStateField, AdapterTargetedStateMatch, AdapterTargetedStateReadOutcome,
            ReadAdapterTargetedState,
        },
        connection::ConnectionSessionRegistry,
        sync_workflow::{
            PreflightSyncReceivers, SyncReceiverPreflightApi, SyncReceiverPreflightTarget,
        },
    },
    database::DatabaseRuntime,
    domain::{
        acquisition::{ProviderItemKey, StateField, StateSubject, Term},
        synchronization::Quantizer,
    },
    infrastructure::sync_preview_collection::{
        PostgresSyncReceiverPreflightService, SyncReceiverPreflightAdapterSession,
    },
};
#[cfg(feature = "embedded-database")]
use uuid::Uuid;

#[cfg(feature = "embedded-database")]
const WORKSPACE_ID: Uuid = Uuid::from_u128(1);
#[cfg(feature = "embedded-database")]
const PREVIEW_ID: Uuid = Uuid::from_u128(3);
#[cfg(feature = "embedded-database")]
const RECEIVER_ID: Uuid = Uuid::from_u128(5);
#[cfg(feature = "embedded-database")]
const CONNECTION_ID: Uuid = Uuid::from_u128(6);
#[cfg(feature = "embedded-database")]
const ACCOUNT_ID: Uuid = Uuid::from_u128(9);
#[cfg(feature = "embedded-database")]
const CREATED_AT: OffsetDateTime = datetime!(2030-01-02 3:04:05 UTC);
#[cfg(feature = "embedded-database")]
const EXPIRES_AT: OffsetDateTime = datetime!(2030-01-02 3:09:05 UTC);

#[cfg(feature = "embedded-database")]
#[tokio::test]
async fn postgres_preflight_reads_persisted_receiver_capabilities_without_writes() {
    let directory = tempdir().expect("temporary directory");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("database starts");
    let pool = database.pool();
    seed_receiver_source(&pool).await;
    let reads = Arc::new(Mutex::new(Vec::new()));
    let registry = TestSessionRegistry::new(
        CONNECTION_ID,
        TestPreflightSession {
            reads: reads.clone(),
            outcome: AdapterTargetedStateReadOutcome::Matched(AdapterTargetedStateMatch {
                membership: AdapterSourceMembership::Absent,
                fields: vec![AdapterTargetedStateField {
                    field: progress_field(),
                    presence: AdapterStatePresence::Absent,
                    value: None,
                }],
                provider_revision: b"receiver-r1".to_vec(),
                observed_at: CREATED_AT,
                expires_at: Some(EXPIRES_AT),
                precondition: Some(b"receiver-precondition-r1".to_vec()),
                write_causation: None,
            }),
        },
    );
    let service = PostgresSyncReceiverPreflightService::<_, TestPreflightSession>::new(
        pool.clone(),
        registry.clone(),
    );
    let target = SyncReceiverPreflightTarget {
        source_id: RECEIVER_ID,
        subject: provider_subject("receiver"),
        field: progress_field(),
    };

    let results = service
        .preflight_receivers(PreflightSyncReceivers {
            operation_id: PREVIEW_ID,
            workspace_id: WORKSPACE_ID,
            targets: vec![target.clone()],
        })
        .await
        .expect("receiver preflight succeeds");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].target, target);
    assert_eq!(results[0].connection_id, CONNECTION_ID);
    assert_eq!(results[0].source_capability_revision, 1);
    assert_eq!(results[0].connection_capability_revision, 1);
    assert_eq!(results[0].process_instance_id, "fixture-process-1");
    assert_eq!(results[0].receiver.progress_capability.minimum, 0);
    assert_eq!(results[0].receiver.progress_capability.maximum, 12);
    assert_eq!(results[0].receiver.progress_capability.step, 1);
    assert_eq!(
        results[0].receiver.progress_capability.quantizer,
        Quantizer::Exact
    );
    {
        let reads = reads.lock().expect("targeted read lock");
        assert_eq!(reads.len(), 1);
        assert_eq!(reads[0].source_key.namespace, "fixture.source");
        assert_eq!(reads[0].source_key.value, b"receiver-source");
        assert_eq!(reads[0].fields, vec![progress_field()]);
        assert!(reads[0].reconciliation_idempotency_key.is_none());
    }
    assert!(registry.identity(CONNECTION_ID).is_some());
    assert_eq!(table_count(&pool, "sync_previews").await, 0);
    assert_eq!(table_count(&pool, "sync_runs").await, 0);
    assert_eq!(table_count(&pool, "sync_actions").await, 0);
    assert_eq!(table_count(&pool, "tasks").await, 0);

    database.shutdown().await.expect("database shuts down");
}

#[cfg(feature = "embedded-database")]
#[derive(Clone)]
struct TestPreflightSession {
    reads: Arc<Mutex<Vec<ReadAdapterTargetedState>>>,
    outcome: AdapterTargetedStateReadOutcome,
}

#[cfg(feature = "embedded-database")]
impl SyncReceiverPreflightAdapterSession for TestPreflightSession {
    type Error = Infallible;

    async fn read_sync_receiver_preflight(
        &mut self,
        command: ReadAdapterTargetedState,
    ) -> Result<AdapterTargetedStateReadOutcome, Self::Error> {
        self.reads.lock().expect("targeted read lock").push(command);
        Ok(self.outcome.clone())
    }

    async fn retire_sync_receiver_preflight(self) -> Result<AdapterRetirement, Self::Error> {
        Ok(AdapterRetirement::Graceful)
    }
}

#[cfg(feature = "embedded-database")]
#[derive(Clone)]
struct TestSessionRegistry {
    connection_id: Uuid,
    session: Arc<Mutex<Option<TestPreflightSession>>>,
    identity: AdapterIdentity,
}

#[cfg(feature = "embedded-database")]
impl TestSessionRegistry {
    fn new(connection_id: Uuid, session: TestPreflightSession) -> Self {
        Self {
            connection_id,
            session: Arc::new(Mutex::new(Some(session))),
            identity: AdapterIdentity {
                adapter_key: "dev.trakkin.fixture".to_owned(),
                adapter_version: "1.0.0".to_owned(),
                process_instance_id: "fixture-process-1".to_owned(),
                protocol_major: 1,
                protocol_minor: 0,
            },
        }
    }
}

#[cfg(feature = "embedded-database")]
impl ConnectionSessionRegistry<TestPreflightSession> for TestSessionRegistry {
    fn replace(
        &self,
        connection_id: Uuid,
        session: TestPreflightSession,
    ) -> Option<TestPreflightSession> {
        assert_eq!(connection_id, self.connection_id);
        self.session.lock().expect("session lock").replace(session)
    }

    fn remove(&self, connection_id: Uuid) -> Option<TestPreflightSession> {
        assert_eq!(connection_id, self.connection_id);
        self.session.lock().expect("session lock").take()
    }

    fn identity(&self, connection_id: Uuid) -> Option<AdapterIdentity> {
        assert_eq!(connection_id, self.connection_id);
        self.session
            .lock()
            .expect("session lock")
            .as_ref()
            .map(|_| self.identity.clone())
    }
}

#[cfg(feature = "embedded-database")]
async fn seed_receiver_source(pool: &sqlx::PgPool) {
    sqlx::query(
        "INSERT INTO workspaces (id, name, created_at, updated_at)
VALUES ($1, 'Preview Collection', $2, $2)",
    )
    .bind(WORKSPACE_ID)
    .bind(CREATED_AT)
    .execute(pool)
    .await
    .expect("workspace seeded");
    sqlx::query(
        "INSERT INTO connections (
    id, provider_id, display_name, status, capabilities,
    capability_revision, created_at, updated_at
) VALUES ($1, 'fixture', 'Fixture', 'ready', '{}'::jsonb, 1, $2, $2)",
    )
    .bind(CONNECTION_ID)
    .bind(CREATED_AT)
    .execute(pool)
    .await
    .expect("connection seeded");
    sqlx::query(
        "INSERT INTO accounts (
    id, connection_id, provider_key_namespace, provider_key_value,
    display_name, first_seen_at, last_seen_at
) VALUES ($1, $2, 'fixture.account', $3, 'Fixture Account', $4, $4)",
    )
    .bind(ACCOUNT_ID)
    .bind(CONNECTION_ID)
    .bind(b"account".as_slice())
    .bind(CREATED_AT)
    .execute(pool)
    .await
    .expect("account seeded");
    sqlx::query(
        "INSERT INTO sources (
    id, connection_id, account_id, provider_key_namespace, provider_key_value,
    display_name, kind_namespace, kind_name, availability, capabilities,
    capability_revision, first_seen_at, last_seen_at
) VALUES (
    $1, $2, $3, 'fixture.source', $4, 'Receiver', 'fixture.kind', 'list',
    'available', $5, 1, $6, $6
)",
    )
    .bind(RECEIVER_ID)
    .bind(CONNECTION_ID)
    .bind(ACCOUNT_ID)
    .bind(b"receiver-source".as_slice())
    .bind(serde_json::json!({
        "stateFields": [{
            "field": {"namespace": "trakkin.state", "name": "progress"},
            "unit": {"namespace": "trakkin.unit", "name": "episode"},
            "valueKind": "integer",
            "numericRange": {"minimum": "0", "maximum": "12", "step": "1"},
            "quantizer": "exact"
        }],
        "targetedStateRead": {
            "maximumFields": 1,
            "maximumResponseBytes": 4096
        }
    }))
    .bind(CREATED_AT)
    .execute(pool)
    .await
    .expect("receiver source seeded");
    sqlx::query(
        "INSERT INTO workspace_sources (workspace_id, source_id, attached_at)
VALUES ($1, $2, $3)",
    )
    .bind(WORKSPACE_ID)
    .bind(RECEIVER_ID)
    .bind(CREATED_AT)
    .execute(pool)
    .await
    .expect("receiver attached");
}

#[cfg(feature = "embedded-database")]
async fn table_count(pool: &sqlx::PgPool, table: &str) -> i64 {
    match table {
        "sync_previews" => sqlx::query_scalar("SELECT COUNT(*) FROM sync_previews"),
        "sync_runs" => sqlx::query_scalar("SELECT COUNT(*) FROM sync_runs"),
        "sync_actions" => sqlx::query_scalar("SELECT COUNT(*) FROM sync_actions"),
        "tasks" => sqlx::query_scalar("SELECT COUNT(*) FROM tasks"),
        _ => panic!("unsupported table"),
    }
    .fetch_one(pool)
    .await
    .expect("table count")
}

#[cfg(feature = "embedded-database")]
fn progress_field() -> StateField {
    StateField::new(
        Term::new("trakkin.state", "progress").unwrap(),
        Some(Term::new("trakkin.unit", "episode").unwrap()),
    )
}

#[cfg(feature = "embedded-database")]
fn provider_subject(value: &str) -> StateSubject {
    StateSubject::ProviderItem(
        ProviderItemKey::new("fixture.subject", value.as_bytes().to_vec()).unwrap(),
    )
}
