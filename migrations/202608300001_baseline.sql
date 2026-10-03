CREATE TABLE provider_sources (
    id UUID PRIMARY KEY,
    repository_url TEXT NOT NULL UNIQUE CHECK (
        repository_url
        ~ (
            '^https?://[a-z0-9][a-z0-9.-]*(:[0-9]+)?'
            || '/[a-z0-9]+([._/-][a-z0-9]+)*$'
        )
    ),
    available_providers JSONB NOT NULL DEFAULT '[]'::JSONB CHECK (
        jsonb_typeof(available_providers) = 'array'
    ),
    trusted_at TIMESTAMPTZ NOT NULL,
    last_checked_at TIMESTAMPTZ,
    last_error TEXT
);

CREATE TABLE providers (
    provider_id TEXT PRIMARY KEY CHECK (provider_id <> ''),
    artifact_id UUID NOT NULL UNIQUE,
    source_id UUID NOT NULL REFERENCES provider_sources(id) ON DELETE RESTRICT,
    version TEXT NOT NULL CHECK (version <> ''),
    platform TEXT NOT NULL CHECK (
        platform IN ('linux/amd64', 'linux/arm64')
    ),
    index_digest TEXT NOT NULL CHECK (
        index_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    manifest_digest TEXT NOT NULL CHECK (
        manifest_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    metadata_digest TEXT NOT NULL CHECK (
        metadata_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    binary_digest TEXT NOT NULL CHECK (
        binary_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    metadata JSONB NOT NULL,
    update_policy TEXT NOT NULL CHECK (
        update_policy IN ('manual', 'automatic')
    ),
    available_version TEXT,
    available_discovered_at TIMESTAMPTZ,
    last_update_error TEXT,
    installed_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (
        (available_version IS NULL) = (available_discovered_at IS NULL)
    ),
    CHECK (
        available_version IS NULL OR available_version <> version
    )
);

CREATE TABLE connections (
    id UUID PRIMARY KEY,
    provider_id TEXT NOT NULL CHECK (provider_id <> ''),
    display_name TEXT NOT NULL CHECK (display_name <> ''),
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'ready', 'unavailable', 'disabled')
    ),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ,
    CHECK (deleted_at IS NULL OR status = 'disabled')
);

CREATE TABLE connection_settings (
    connection_id UUID NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    setting_key TEXT NOT NULL CHECK (setting_key <> ''),
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (connection_id, setting_key)
);

CREATE TABLE encrypted_secrets (
    id UUID PRIMARY KEY,
    nonce BYTEA NOT NULL CHECK (octet_length(nonce) = 24),
    ciphertext BYTEA NOT NULL CHECK (octet_length(ciphertext) > 0),
    encryption_version INTEGER NOT NULL DEFAULT 1 CHECK (
        encryption_version > 0
    ),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE connection_secret_bindings (
    connection_id UUID NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    setting_key TEXT NOT NULL CHECK (setting_key <> ''),
    secret_id UUID NOT NULL REFERENCES encrypted_secrets(id) ON DELETE RESTRICT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (connection_id, setting_key)
);

CREATE INDEX connection_secret_bindings_secret_idx
ON connection_secret_bindings (secret_id);

CREATE TABLE authentication_sessions (
    id UUID PRIMARY KEY,
    connection_id UUID NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    provider_authentication_id TEXT NOT NULL CHECK (
        provider_authentication_id <> ''
    ),
    method TEXT NOT NULL CHECK (method <> ''),
    status TEXT NOT NULL CHECK (
        status IN (
            'waiting',
            'input_required',
            'completed',
            'cancelled',
            'expired',
            'failed'
        )
    ),
    step JSONB NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (connection_id, provider_authentication_id)
);

CREATE INDEX authentication_sessions_active_idx
ON authentication_sessions (connection_id, status, updated_at DESC)
WHERE status IN ('waiting', 'input_required');

CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    connection_id UUID NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    provider_key_namespace TEXT NOT NULL CHECK (provider_key_namespace <> ''),
    provider_key_value BYTEA NOT NULL CHECK (
        octet_length(provider_key_value) > 0
    ),
    display_name TEXT NOT NULL CHECK (display_name <> ''),
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    removed_at TIMESTAMPTZ,
    UNIQUE (connection_id, provider_key_namespace, provider_key_value),
    UNIQUE (id, connection_id)
);

CREATE TABLE sources (
    id UUID PRIMARY KEY,
    connection_id UUID NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    account_id UUID NOT NULL,
    provider_key_namespace TEXT NOT NULL CHECK (provider_key_namespace <> ''),
    provider_key_value BYTEA NOT NULL CHECK (
        octet_length(provider_key_value) > 0
    ),
    display_name TEXT NOT NULL CHECK (display_name <> ''),
    kind_namespace TEXT NOT NULL CHECK (kind_namespace <> ''),
    kind_name TEXT NOT NULL CHECK (kind_name <> ''),
    availability TEXT NOT NULL CHECK (
        availability IN ('available', 'unavailable', 'removed')
    ),
    capabilities JSONB NOT NULL,
    capability_revision BIGINT NOT NULL DEFAULT 1 CHECK (
        capability_revision > 0
    ),
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    removed_at TIMESTAMPTZ,
    FOREIGN KEY (account_id, connection_id)
    REFERENCES accounts(id, connection_id) ON DELETE CASCADE,
    UNIQUE (connection_id, provider_key_namespace, provider_key_value),
    UNIQUE (id, connection_id)
);

CREATE INDEX sources_account_idx ON sources (account_id, availability);

CREATE TABLE workspaces (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL CHECK (name <> ''),
    status TEXT NOT NULL DEFAULT 'active' CHECK (
        status IN ('active', 'archived')
    ),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    archived_at TIMESTAMPTZ,
    CHECK ((status = 'archived') = (archived_at IS NOT NULL))
);

CREATE TABLE workspace_sources (
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    attached_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (workspace_id, source_id)
);

CREATE INDEX workspace_sources_source_idx ON workspace_sources (source_id);

CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    connection_id UUID REFERENCES connections(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind <> ''),
    status TEXT NOT NULL CHECK (
        status IN (
            'queued',
            'running',
            'cancelling',
            'succeeded',
            'failed',
            'cancelled',
            'interrupted'
        )
    ),
    cancellation_supported BOOLEAN NOT NULL,
    cancellation_requested_at TIMESTAMPTZ,
    failure JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (
        (status IN ('succeeded', 'failed', 'cancelled', 'interrupted'))
        = (finished_at IS NOT NULL)
    ),
    CHECK (status <> 'failed' OR failure IS NOT NULL)
);

CREATE INDEX tasks_workspace_created_idx
ON tasks (workspace_id, created_at DESC, id);

CREATE INDEX tasks_active_idx ON tasks (created_at, id)
WHERE status IN ('queued', 'running', 'cancelling');

CREATE TABLE task_claims (
    task_id UUID PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    owner_id UUID NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE task_attempts (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    status TEXT NOT NULL CHECK (
        status IN ('running', 'succeeded', 'failed', 'cancelled', 'interrupted')
    ),
    failure JSONB,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    UNIQUE (id, task_id),
    UNIQUE (task_id, ordinal),
    CHECK ((status = 'running') = (finished_at IS NULL)),
    CHECK (status <> 'failed' OR failure IS NOT NULL)
);

CREATE TABLE source_discovery_tasks (
    task_id UUID PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    connection_id UUID NOT NULL REFERENCES connections(id) ON DELETE CASCADE
);

CREATE TABLE read_tasks (
    task_id UUID PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    stream_kind TEXT NOT NULL CHECK (stream_kind IN ('catalog', 'state')),
    read_mode TEXT NOT NULL CHECK (read_mode IN ('full', 'incremental'))
);

CREATE TABLE reset_tasks (
    task_id UUID PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    stream_kind TEXT NOT NULL CHECK (stream_kind IN ('catalog', 'state'))
);

CREATE TABLE source_streams (
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    stream_kind TEXT NOT NULL CHECK (stream_kind IN ('catalog', 'state')),
    status TEXT NOT NULL DEFAULT 'unavailable' CHECK (
        status IN ('unavailable', 'available', 'failed')
    ),
    cursor BYTEA,
    publication_id UUID,
    evidence_revision BYTEA,
    last_success_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_id, stream_kind),
    CHECK (
        status <> 'available'
        OR (
            publication_id IS NOT NULL
            AND evidence_revision IS NOT NULL
            AND last_success_at IS NOT NULL
        )
    )
);

CREATE TABLE read_attempts (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL UNIQUE REFERENCES tasks(id) ON DELETE CASCADE,
    task_attempt_id UUID NOT NULL
    REFERENCES task_attempts(id) ON DELETE CASCADE,
    owner_id UUID NOT NULL,
    source_id UUID NOT NULL,
    stream_kind TEXT NOT NULL,
    read_mode TEXT NOT NULL CHECK (read_mode IN ('full', 'incremental')),
    status TEXT NOT NULL CHECK (
        status IN ('staging', 'published', 'abandoned')
    ),
    expected_cursor BYTEA,
    expected_publication_id UUID,
    expected_capability_revision BIGINT NOT NULL CHECK (
        expected_capability_revision > 0
    ),
    next_batch_sequence BIGINT NOT NULL DEFAULT 0 CHECK (
        next_batch_sequence >= 0
    ),
    next_cursor BYTEA,
    evidence_revision BYTEA,
    observed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    FOREIGN KEY (source_id, stream_kind)
    REFERENCES source_streams(source_id, stream_kind) ON DELETE CASCADE,
    CHECK (stream_kind IN ('catalog', 'state')),
    CHECK (
        (status = 'staging') = (completed_at IS NULL)
    ),
    CHECK (
        status <> 'published'
        OR (
            evidence_revision IS NOT NULL
            AND observed_at IS NOT NULL
        )
    )
);

CREATE INDEX read_attempts_active_stream_idx
ON read_attempts (source_id, stream_kind, created_at)
WHERE status = 'staging';

CREATE UNIQUE INDEX read_attempts_one_active_stream_idx
ON read_attempts (source_id, stream_kind)
WHERE status = 'staging';

CREATE TABLE staged_catalog_items (
    attempt_id UUID NOT NULL REFERENCES read_attempts(id) ON DELETE CASCADE,
    key_namespace TEXT NOT NULL CHECK (key_namespace <> ''),
    key_value BYTEA NOT NULL CHECK (octet_length(key_value) > 0),
    operation TEXT NOT NULL CHECK (operation IN ('upsert', 'delete')),
    kind_namespace TEXT,
    kind_name TEXT,
    display_name TEXT,
    attributes JSONB,
    batch_sequence BIGINT NOT NULL CHECK (batch_sequence >= 0),
    row_ordinal INTEGER NOT NULL CHECK (row_ordinal >= 0),
    PRIMARY KEY (attempt_id, key_namespace, key_value),
    CHECK (
        (operation = 'delete')
        = (
            kind_namespace IS NULL
            AND kind_name IS NULL
            AND display_name IS NULL
            AND attributes IS NULL
        )
    ),
    CHECK (
        operation <> 'upsert'
        OR (
            kind_namespace <> ''
            AND kind_name <> ''
            AND display_name <> ''
            AND attributes IS NOT NULL
        )
    )
);

CREATE TABLE staged_catalog_assets (
    attempt_id UUID NOT NULL,
    item_key_namespace TEXT NOT NULL CHECK (item_key_namespace <> ''),
    item_key_value BYTEA NOT NULL CHECK (octet_length(item_key_value) > 0),
    asset_key_namespace TEXT NOT NULL CHECK (asset_key_namespace <> ''),
    asset_key_value BYTEA NOT NULL CHECK (octet_length(asset_key_value) > 0),
    kind_namespace TEXT NOT NULL CHECK (kind_namespace <> ''),
    kind_name TEXT NOT NULL CHECK (kind_name <> ''),
    PRIMARY KEY (
        attempt_id,
        item_key_namespace,
        item_key_value,
        asset_key_namespace,
        asset_key_value
    ),
    FOREIGN KEY (attempt_id, item_key_namespace, item_key_value)
    REFERENCES staged_catalog_items(
        attempt_id,
        key_namespace,
        key_value
    ) ON DELETE CASCADE
);

CREATE TABLE staged_catalog_relations (
    attempt_id UUID NOT NULL REFERENCES read_attempts(id) ON DELETE CASCADE,
    key_namespace TEXT NOT NULL CHECK (key_namespace <> ''),
    key_value BYTEA NOT NULL CHECK (octet_length(key_value) > 0),
    operation TEXT NOT NULL CHECK (operation IN ('upsert', 'delete')),
    parent_key_namespace TEXT,
    parent_key_value BYTEA,
    item_key_namespace TEXT,
    item_key_value BYTEA,
    kind_namespace TEXT,
    kind_name TEXT,
    order_parts BIGINT [],
    attributes JSONB,
    batch_sequence BIGINT NOT NULL CHECK (batch_sequence >= 0),
    row_ordinal INTEGER NOT NULL CHECK (row_ordinal >= 0),
    PRIMARY KEY (attempt_id, key_namespace, key_value),
    CHECK (
        (parent_key_namespace IS NULL) = (parent_key_value IS NULL)
    ),
    CHECK (
        (item_key_namespace IS NULL) = (item_key_value IS NULL)
    ),
    CHECK (
        (operation = 'delete')
        = (
            parent_key_namespace IS NULL
            AND parent_key_value IS NULL
            AND item_key_namespace IS NULL
            AND item_key_value IS NULL
            AND kind_namespace IS NULL
            AND kind_name IS NULL
            AND order_parts IS NULL
            AND attributes IS NULL
        )
    ),
    CHECK (
        operation <> 'upsert'
        OR (
            coalesce(parent_key_namespace <> '', TRUE)
            AND coalesce(octet_length(parent_key_value) > 0, TRUE)
            AND coalesce(item_key_namespace <> '', TRUE)
            AND coalesce(octet_length(item_key_value) > 0, TRUE)
            AND kind_namespace <> ''
            AND kind_name <> ''
            AND order_parts IS NOT NULL
            AND attributes IS NOT NULL
        )
    )
);

CREATE TABLE staged_subject_unit_bindings (
    attempt_id UUID NOT NULL REFERENCES read_attempts(id) ON DELETE CASCADE,
    unit_key TEXT NOT NULL CHECK (unit_key <> ''),
    subject_kind TEXT NOT NULL CHECK (
        subject_kind IN ('provider_item', 'catalog_relation')
    ),
    subject_key_namespace TEXT NOT NULL CHECK (subject_key_namespace <> ''),
    subject_key_value BYTEA NOT NULL CHECK (
        octet_length(subject_key_value) > 0
    ),
    operation TEXT NOT NULL CHECK (operation IN ('upsert', 'delete')),
    relevance_rank BIGINT CHECK (
        relevance_rank BETWEEN 0 AND 4294967295
    ),
    batch_sequence BIGINT NOT NULL CHECK (batch_sequence >= 0),
    row_ordinal INTEGER NOT NULL CHECK (row_ordinal >= 0),
    PRIMARY KEY (
        attempt_id,
        unit_key,
        subject_kind,
        subject_key_namespace,
        subject_key_value
    ),
    CHECK ((operation = 'upsert') = (relevance_rank IS NOT NULL))
);

CREATE TABLE staged_state_observations (
    attempt_id UUID NOT NULL REFERENCES read_attempts(id) ON DELETE CASCADE,
    subject_kind TEXT NOT NULL CHECK (
        subject_kind IN ('provider_item', 'catalog_relation')
    ),
    subject_key_namespace TEXT NOT NULL CHECK (subject_key_namespace <> ''),
    subject_key_value BYTEA NOT NULL CHECK (
        octet_length(subject_key_value) > 0
    ),
    field_namespace TEXT NOT NULL CHECK (field_namespace <> ''),
    field_name TEXT NOT NULL CHECK (field_name <> ''),
    unit_namespace TEXT NOT NULL,
    unit_name TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('set', 'delete')),
    value JSONB,
    provider_revision BYTEA NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    batch_sequence BIGINT NOT NULL CHECK (batch_sequence >= 0),
    row_ordinal INTEGER NOT NULL CHECK (row_ordinal >= 0),
    PRIMARY KEY (
        attempt_id,
        subject_kind,
        subject_key_namespace,
        subject_key_value,
        field_namespace,
        field_name,
        unit_namespace,
        unit_name
    ),
    CHECK ((unit_namespace = '') = (unit_name = '')),
    CHECK ((operation = 'delete') = (value IS NULL))
);

CREATE TABLE catalog_items (
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    key_namespace TEXT NOT NULL CHECK (key_namespace <> ''),
    key_value BYTEA NOT NULL CHECK (octet_length(key_value) > 0),
    kind_namespace TEXT NOT NULL CHECK (kind_namespace <> ''),
    kind_name TEXT NOT NULL CHECK (kind_name <> ''),
    display_name TEXT NOT NULL CHECK (display_name <> ''),
    attributes JSONB NOT NULL,
    publication_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_id, key_namespace, key_value)
);

CREATE INDEX catalog_items_display_name_idx
ON catalog_items (source_id, display_name, key_namespace, key_value);

CREATE TABLE catalog_assets (
    source_id UUID NOT NULL,
    item_key_namespace TEXT NOT NULL CHECK (item_key_namespace <> ''),
    item_key_value BYTEA NOT NULL CHECK (octet_length(item_key_value) > 0),
    asset_key_namespace TEXT NOT NULL CHECK (asset_key_namespace <> ''),
    asset_key_value BYTEA NOT NULL CHECK (octet_length(asset_key_value) > 0),
    kind_namespace TEXT NOT NULL CHECK (kind_namespace <> ''),
    kind_name TEXT NOT NULL CHECK (kind_name <> ''),
    publication_id UUID NOT NULL,
    PRIMARY KEY (
        source_id,
        item_key_namespace,
        item_key_value,
        asset_key_namespace,
        asset_key_value
    ),
    FOREIGN KEY (source_id, item_key_namespace, item_key_value)
    REFERENCES catalog_items(source_id, key_namespace, key_value)
    ON DELETE CASCADE
);

CREATE TABLE catalog_relations (
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    key_namespace TEXT NOT NULL CHECK (key_namespace <> ''),
    key_value BYTEA NOT NULL CHECK (octet_length(key_value) > 0),
    parent_key_namespace TEXT CHECK (parent_key_namespace <> ''),
    parent_key_value BYTEA CHECK (octet_length(parent_key_value) > 0),
    item_key_namespace TEXT CHECK (item_key_namespace <> ''),
    item_key_value BYTEA CHECK (octet_length(item_key_value) > 0),
    kind_namespace TEXT NOT NULL CHECK (kind_namespace <> ''),
    kind_name TEXT NOT NULL CHECK (kind_name <> ''),
    order_parts BIGINT [] NOT NULL,
    attributes JSONB NOT NULL,
    publication_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_id, key_namespace, key_value),
    CHECK (
        (parent_key_namespace IS NULL) = (parent_key_value IS NULL)
    ),
    CHECK (
        (item_key_namespace IS NULL) = (item_key_value IS NULL)
    )
);

CREATE INDEX catalog_relations_parent_idx
ON catalog_relations (
    source_id,
    parent_key_namespace,
    parent_key_value,
    order_parts,
    key_namespace,
    key_value
);

CREATE TABLE subject_unit_bindings (
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    unit_key TEXT NOT NULL CHECK (unit_key <> ''),
    subject_kind TEXT NOT NULL CHECK (
        subject_kind IN ('provider_item', 'catalog_relation')
    ),
    subject_key_namespace TEXT NOT NULL CHECK (subject_key_namespace <> ''),
    subject_key_value BYTEA NOT NULL CHECK (
        octet_length(subject_key_value) > 0
    ),
    relevance_rank BIGINT NOT NULL CHECK (
        relevance_rank BETWEEN 0 AND 4294967295
    ),
    publication_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (
        source_id,
        unit_key,
        subject_kind,
        subject_key_namespace,
        subject_key_value
    )
);

CREATE INDEX subject_unit_bindings_subject_idx
ON subject_unit_bindings (
    source_id,
    subject_kind,
    subject_key_namespace,
    subject_key_value,
    relevance_rank,
    unit_key
);

CREATE TABLE state_observations (
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    subject_kind TEXT NOT NULL CHECK (
        subject_kind IN ('provider_item', 'catalog_relation')
    ),
    subject_key_namespace TEXT NOT NULL CHECK (subject_key_namespace <> ''),
    subject_key_value BYTEA NOT NULL CHECK (
        octet_length(subject_key_value) > 0
    ),
    field_namespace TEXT NOT NULL CHECK (field_namespace <> ''),
    field_name TEXT NOT NULL CHECK (field_name <> ''),
    unit_namespace TEXT NOT NULL,
    unit_name TEXT NOT NULL,
    value JSONB NOT NULL,
    provider_revision BYTEA NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    publication_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (
        source_id,
        subject_kind,
        subject_key_namespace,
        subject_key_value,
        field_namespace,
        field_name,
        unit_namespace,
        unit_name
    ),
    CHECK ((unit_namespace = '') = (unit_name = ''))
);

CREATE INDEX state_observations_subject_idx
ON state_observations (
    source_id,
    subject_kind,
    subject_key_namespace,
    subject_key_value
);

CREATE TABLE state_observation_tombstones (
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    subject_kind TEXT NOT NULL CHECK (
        subject_kind IN ('provider_item', 'catalog_relation')
    ),
    subject_key_namespace TEXT NOT NULL CHECK (subject_key_namespace <> ''),
    subject_key_value BYTEA NOT NULL CHECK (
        octet_length(subject_key_value) > 0
    ),
    field_namespace TEXT NOT NULL CHECK (field_namespace <> ''),
    field_name TEXT NOT NULL CHECK (field_name <> ''),
    unit_namespace TEXT NOT NULL,
    unit_name TEXT NOT NULL,
    provider_revision BYTEA NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    publication_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (
        source_id,
        subject_kind,
        subject_key_namespace,
        subject_key_value,
        field_namespace,
        field_name,
        unit_namespace,
        unit_name
    ),
    CHECK ((unit_namespace = '') = (unit_name = ''))
);

CREATE TABLE state_publication_coverage (
    source_id UUID PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    baseline_publication_id UUID NOT NULL,
    publication_id UUID NOT NULL,
    evidence_revision BYTEA NOT NULL CHECK (
        octet_length(evidence_revision) > 0
    ),
    observed_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (source_id, publication_id)
);

CREATE INDEX state_observation_tombstones_subject_idx
ON state_observation_tombstones (
    source_id,
    subject_kind,
    subject_key_namespace,
    subject_key_value
);

CREATE TABLE mapping_sources (
    id UUID PRIMARY KEY,
    source_key TEXT NOT NULL UNIQUE CHECK (source_key <> ''),
    source_kind TEXT NOT NULL CHECK (
        source_kind IN ('local', 'upload', 'remote')
    ),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    remote_url TEXT,
    http_etag TEXT,
    http_last_modified TEXT,
    priority BIGINT NOT NULL UNIQUE DEFERRABLE INITIALLY IMMEDIATE
    CHECK (priority >= 0),
    enabled BOOLEAN NOT NULL,
    current_version_id UUID NOT NULL,
    last_checked_at TIMESTAMPTZ,
    last_refresh_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (
        (
            source_kind = 'local'
            AND priority = 0
            AND enabled
            AND remote_url IS NULL
        )
        OR (
            source_kind = 'upload'
            AND priority > 0
            AND remote_url IS NULL
        )
        OR (
            source_kind = 'remote'
            AND priority > 0
            AND btrim(remote_url) <> ''
        )
    ),
    CHECK (last_refresh_error IS NULL OR last_checked_at IS NOT NULL)
);

CREATE UNIQUE INDEX mapping_sources_local_singleton_idx
ON mapping_sources (source_kind)
WHERE source_kind = 'local';

CREATE UNIQUE INDEX mapping_sources_remote_url_unique_idx
ON mapping_sources (remote_url)
WHERE source_kind = 'remote';

CREATE TABLE mapping_source_versions (
    id UUID PRIMARY KEY,
    source_id UUID NOT NULL REFERENCES mapping_sources(id) ON DELETE CASCADE,
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    byte_length BIGINT NOT NULL CHECK (byte_length >= 0),
    artifact_path TEXT NOT NULL CHECK (artifact_path <> ''),
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (id, source_id),
    UNIQUE (source_id, content_hash)
);

ALTER TABLE mapping_sources
ADD CONSTRAINT mapping_sources_current_version_fk
    FOREIGN KEY (current_version_id, id)
    REFERENCES mapping_source_versions(id, source_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE mapping_generations (
    id UUID PRIMARY KEY,
    chain_fingerprint TEXT NOT NULL CHECK (
        chain_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    artifact_path TEXT NOT NULL CHECK (artifact_path <> ''),
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (chain_fingerprint)
);

CREATE TABLE mapping_generation_sources (
    generation_id UUID NOT NULL
    REFERENCES mapping_generations(id) ON DELETE CASCADE,
    position BIGINT NOT NULL CHECK (position >= 0),
    source_id UUID NOT NULL REFERENCES mapping_sources(id) ON DELETE RESTRICT,
    source_version_id UUID NOT NULL,
    PRIMARY KEY (generation_id, position),
    UNIQUE (generation_id, source_id),
    FOREIGN KEY (source_version_id, source_id)
    REFERENCES mapping_source_versions(id, source_id) ON DELETE RESTRICT
);

CREATE TABLE mapping_chain_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    active_generation_id UUID
    REFERENCES mapping_generations(id) ON DELETE RESTRICT,
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO mapping_chain_state (singleton, updated_at)
VALUES (TRUE, current_timestamp);

CREATE FUNCTION reject_mapping_manifest_update() RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'mapping manifests are immutable' USING ERRCODE = '55000';
END
$$;

CREATE TRIGGER mapping_source_versions_immutable
BEFORE UPDATE ON mapping_source_versions
FOR EACH ROW EXECUTE FUNCTION reject_mapping_manifest_update();

CREATE TRIGGER mapping_generations_immutable
BEFORE UPDATE ON mapping_generations
FOR EACH ROW EXECUTE FUNCTION reject_mapping_manifest_update();

CREATE TRIGGER mapping_generation_sources_immutable
BEFORE UPDATE ON mapping_generation_sources
FOR EACH ROW EXECUTE FUNCTION reject_mapping_manifest_update();
