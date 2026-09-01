CREATE TABLE provider_repositories (
    id TEXT PRIMARY KEY CHECK (id <> ''),
    display_name TEXT NOT NULL CHECK (display_name <> ''),
    metadata_base_url TEXT NOT NULL CHECK (metadata_base_url <> ''),
    targets_base_url TEXT NOT NULL CHECK (targets_base_url <> ''),
    trusted_root BYTEA NOT NULL CHECK (octet_length(trusted_root) > 0),
    built_in BOOLEAN NOT NULL DEFAULT FALSE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    catalog_sequence BIGINT CHECK (catalog_sequence >= 0),
    catalog JSONB,
    refreshed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK ((catalog_sequence IS NULL) = (catalog IS NULL)),
    CHECK ((catalog IS NULL) = (refreshed_at IS NULL))
);

CREATE UNIQUE INDEX provider_repositories_one_built_in_idx
ON provider_repositories (built_in) WHERE built_in;

CREATE TABLE provider_installations (
    id UUID PRIMARY KEY,
    repository_id TEXT NOT NULL
    REFERENCES provider_repositories(id) ON DELETE RESTRICT,
    provider_id TEXT NOT NULL CHECK (provider_id <> ''),
    version TEXT NOT NULL CHECK (version <> ''),
    target TEXT NOT NULL CHECK (target <> ''),
    package_sha256 TEXT NOT NULL CHECK (
        package_sha256 ~ '^[0-9a-f]{64}$'
    ),
    manifest JSONB NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('installing', 'installed', 'failed')
    ),
    installed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (id, provider_id),
    CHECK ((status = 'installed') = (installed_at IS NOT NULL))
);

CREATE UNIQUE INDEX provider_installations_live_release_idx
ON provider_installations (repository_id, provider_id, version, target)
WHERE status IN ('installing', 'installed');

CREATE TABLE active_provider_installations (
    provider_id TEXT PRIMARY KEY CHECK (provider_id <> ''),
    installation_id UUID NOT NULL,
    generation UUID NOT NULL UNIQUE,
    activated_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (installation_id, provider_id)
    REFERENCES provider_installations(id, provider_id) ON DELETE RESTRICT
);

CREATE TABLE connections (
    id UUID PRIMARY KEY,
    provider_id TEXT NOT NULL CHECK (provider_id <> ''),
    display_name TEXT NOT NULL CHECK (display_name <> ''),
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'ready', 'unavailable', 'disabled')
    ),
    capabilities JSONB NOT NULL DEFAULT '{}'::JSONB,
    capability_revision BIGINT NOT NULL DEFAULT 0 CHECK (
        capability_revision >= 0
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
    invocation_context JSONB NOT NULL,
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

CREATE TABLE staged_catalog_item_references (
    attempt_id UUID NOT NULL,
    item_key_namespace TEXT NOT NULL CHECK (item_key_namespace <> ''),
    item_key_value BYTEA NOT NULL CHECK (octet_length(item_key_value) > 0),
    reference_namespace TEXT NOT NULL CHECK (reference_namespace <> ''),
    reference_value BYTEA NOT NULL CHECK (
        octet_length(reference_value) > 0
    ),
    PRIMARY KEY (
        attempt_id,
        item_key_namespace,
        item_key_value,
        reference_namespace,
        reference_value
    ),
    FOREIGN KEY (attempt_id, item_key_namespace, item_key_value)
    REFERENCES staged_catalog_items(
        attempt_id,
        key_namespace,
        key_value
    ) ON DELETE CASCADE
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

CREATE TABLE catalog_item_references (
    source_id UUID NOT NULL,
    item_key_namespace TEXT NOT NULL CHECK (item_key_namespace <> ''),
    item_key_value BYTEA NOT NULL CHECK (octet_length(item_key_value) > 0),
    reference_namespace TEXT NOT NULL CHECK (reference_namespace <> ''),
    reference_value BYTEA NOT NULL CHECK (
        octet_length(reference_value) > 0
    ),
    PRIMARY KEY (
        source_id,
        item_key_namespace,
        item_key_value,
        reference_namespace,
        reference_value
    ),
    FOREIGN KEY (source_id, item_key_namespace, item_key_value)
    REFERENCES catalog_items(source_id, key_namespace, key_value)
    ON DELETE CASCADE
);

CREATE INDEX catalog_item_references_lookup_idx
ON catalog_item_references (
    reference_namespace,
    reference_value,
    source_id,
    item_key_namespace,
    item_key_value
);

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

CREATE TABLE mapping_revisions (
    content_hash TEXT PRIMARY KEY CHECK (
        content_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    semantic_fingerprint TEXT NOT NULL CHECK (
        semantic_fingerprint ~ '^sha256:[0-9a-f]{64}$'
    ),
    schema_name TEXT NOT NULL CHECK (schema_name = 'trakkin.mapping/v1'),
    portable_revision BIGINT NOT NULL CHECK (
        portable_revision BETWEEN 1 AND 9007199254740991
    ),
    canonical_json BYTEA NOT NULL CHECK (
        octet_length(canonical_json) BETWEEN 1 AND 5242880
    ),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE mappings (
    id UUID PRIMARY KEY,
    head_content_hash TEXT NOT NULL
    REFERENCES mapping_revisions(content_hash) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE mapping_record_revisions (
    mapping_id UUID NOT NULL REFERENCES mappings(id) ON DELETE CASCADE,
    content_hash TEXT NOT NULL
    REFERENCES mapping_revisions(content_hash) ON DELETE RESTRICT,
    lineage_ordinal BIGINT NOT NULL CHECK (lineage_ordinal > 0),
    origin TEXT NOT NULL CHECK (
        origin IN ('created', 'updated', 'imported', 'forked')
    ),
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (mapping_id, content_hash),
    UNIQUE (mapping_id, lineage_ordinal),
    UNIQUE (mapping_id, content_hash, origin),
    UNIQUE (mapping_id, content_hash, created_at)
);

ALTER TABLE mappings
ADD CONSTRAINT mappings_head_revision_fk
    FOREIGN KEY (id, head_content_hash)
    REFERENCES mapping_record_revisions(mapping_id, content_hash)
    DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX mapping_record_revisions_lineage_idx
ON mapping_record_revisions (mapping_id, lineage_ordinal DESC);

CREATE TABLE mapping_revision_parents (
    mapping_id UUID NOT NULL,
    child_hash TEXT NOT NULL,
    parent_hash TEXT NOT NULL CHECK (
        parent_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    parent_ordinal INTEGER NOT NULL CHECK (parent_ordinal >= 0),
    verified BOOLEAN NOT NULL,
    PRIMARY KEY (mapping_id, child_hash, parent_hash),
    UNIQUE (mapping_id, child_hash, parent_ordinal),
    FOREIGN KEY (mapping_id, child_hash)
    REFERENCES mapping_record_revisions(mapping_id, content_hash)
    ON DELETE CASCADE,
    CHECK (child_hash <> parent_hash)
);

CREATE INDEX mapping_revision_parents_parent_idx
ON mapping_revision_parents (parent_hash, mapping_id, child_hash);

CREATE TABLE mapping_rows (
    content_hash TEXT NOT NULL
    REFERENCES mapping_revisions(content_hash) ON DELETE CASCADE,
    row_ordinal INTEGER NOT NULL CHECK (row_ordinal >= 0),
    row_hash TEXT NOT NULL CHECK (
        row_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    canonical_row JSONB NOT NULL CHECK (
        jsonb_typeof(canonical_row) = 'array'
    ),
    PRIMARY KEY (content_hash, row_hash),
    UNIQUE (content_hash, row_ordinal)
);

CREATE TABLE mapping_row_endpoints (
    content_hash TEXT NOT NULL,
    row_hash TEXT NOT NULL,
    endpoint_ordinal INTEGER NOT NULL CHECK (endpoint_ordinal >= 0),
    endpoint_hash TEXT NOT NULL CHECK (
        endpoint_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    reference_namespace TEXT NOT NULL CHECK (reference_namespace <> ''),
    reference_value BYTEA NOT NULL CHECK (octet_length(reference_value) > 0),
    selector TEXT,
    coordinate_ids TEXT [] NOT NULL,
    ordinal_coordinate TEXT,
    ordinal_start BIGINT,
    ordinal_end BIGINT,
    CHECK (
        (
            ordinal_coordinate IS NULL
            AND ordinal_start IS NULL
            AND ordinal_end IS NULL
        )
        OR (
            ordinal_coordinate IS NOT NULL
            AND ordinal_coordinate <> ''
            AND ordinal_start IS NOT NULL
            AND ordinal_end IS NOT NULL
            AND ordinal_start >= 0
            AND ordinal_start <= ordinal_end
        )
    ),
    PRIMARY KEY (content_hash, row_hash, endpoint_ordinal),
    FOREIGN KEY (content_hash, row_hash)
    REFERENCES mapping_rows(content_hash, row_hash) ON DELETE CASCADE
);

CREATE INDEX mapping_row_endpoints_reference_idx
ON mapping_row_endpoints (
    reference_namespace,
    reference_value,
    selector,
    content_hash,
    row_hash,
    endpoint_ordinal
);

CREATE INDEX mapping_row_endpoints_finite_ordinal_idx
ON mapping_row_endpoints (
    reference_namespace,
    reference_value,
    ordinal_coordinate,
    ordinal_start,
    ordinal_end,
    content_hash,
    row_hash,
    endpoint_ordinal
)
WHERE ordinal_coordinate IS NOT NULL;

CREATE TABLE mapping_package_provenance (
    package_id UUID NOT NULL,
    document_path TEXT NOT NULL CHECK (document_path <> ''),
    mapping_id UUID NOT NULL,
    content_hash TEXT NOT NULL,
    roots JSONB NOT NULL CHECK (jsonb_typeof(roots) = 'array'),
    license_expression TEXT NOT NULL CHECK (license_expression <> ''),
    attribution JSONB NOT NULL CHECK (jsonb_typeof(attribution) = 'array'),
    signatures JSONB NOT NULL CHECK (jsonb_typeof(signatures) = 'array'),
    forge_commit TEXT,
    required_mapping_schema TEXT NOT NULL CHECK (
        required_mapping_schema <> ''
    ),
    required_resolver TEXT NOT NULL CHECK (required_resolver <> ''),
    required_coordinate_registry_digest TEXT NOT NULL CHECK (
        required_coordinate_registry_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    imported_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (package_id, document_path),
    FOREIGN KEY (mapping_id, content_hash)
    REFERENCES mapping_record_revisions(mapping_id, content_hash)
    ON DELETE CASCADE
);

CREATE INDEX mapping_package_provenance_revision_idx
ON mapping_package_provenance (mapping_id, content_hash);

CREATE TABLE workspace_mapping_activations (
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    mapping_id UUID NOT NULL REFERENCES mappings(id) ON DELETE CASCADE,
    active_content_hash TEXT,
    generation BIGINT NOT NULL CHECK (generation > 0),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (workspace_id, mapping_id),
    FOREIGN KEY (mapping_id, active_content_hash)
    REFERENCES mapping_record_revisions(mapping_id, content_hash)
    ON DELETE RESTRICT
);

CREATE INDEX workspace_mapping_activations_active_idx
ON workspace_mapping_activations (
    workspace_id,
    active_content_hash,
    mapping_id
)
WHERE active_content_hash IS NOT NULL;

CREATE TABLE workspace_mapping_activation_events (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL,
    mapping_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    kind TEXT NOT NULL CHECK (kind IN ('activate', 'rollback', 'retire')),
    previous_content_hash TEXT,
    selected_content_hash TEXT,
    changed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (workspace_id, mapping_id, generation),
    FOREIGN KEY (workspace_id, mapping_id)
    REFERENCES workspace_mapping_activations(workspace_id, mapping_id)
    ON DELETE CASCADE,
    FOREIGN KEY (mapping_id, previous_content_hash)
    REFERENCES mapping_record_revisions(mapping_id, content_hash)
    ON DELETE RESTRICT,
    FOREIGN KEY (mapping_id, selected_content_hash)
    REFERENCES mapping_record_revisions(mapping_id, content_hash)
    ON DELETE RESTRICT,
    CHECK ((kind = 'retire') = (selected_content_hash IS NULL))
);

CREATE INDEX workspace_mapping_activation_events_history_idx
ON workspace_mapping_activation_events (
    workspace_id,
    changed_at DESC,
    id
);

CREATE TABLE source_coordinate_publications (
    source_id UUID PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    publication_id UUID NOT NULL,
    evidence_revision BYTEA NOT NULL CHECK (
        octet_length(evidence_revision) > 0
    ),
    published_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    UNIQUE (source_id, publication_id),
    CHECK (expires_at IS NULL OR expires_at > published_at)
);

CREATE TABLE source_coordinate_bindings (
    source_id UUID NOT NULL,
    publication_id UUID NOT NULL,
    item_key_namespace TEXT NOT NULL CHECK (item_key_namespace <> ''),
    item_key_value BYTEA NOT NULL CHECK (octet_length(item_key_value) > 0),
    reference_namespace TEXT NOT NULL CHECK (reference_namespace <> ''),
    reference_value BYTEA NOT NULL CHECK (octet_length(reference_value) > 0),
    selector TEXT NOT NULL,
    backing TEXT NOT NULL CHECK (
        backing IN ('materialized', 'virtual', 'aggregate')
    ),
    observed_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    PRIMARY KEY (
        source_id,
        item_key_namespace,
        item_key_value,
        reference_namespace,
        reference_value,
        selector
    ),
    FOREIGN KEY (source_id, publication_id)
    REFERENCES source_coordinate_publications(source_id, publication_id)
    ON DELETE CASCADE,
    FOREIGN KEY (source_id, item_key_namespace, item_key_value)
    REFERENCES catalog_items(source_id, key_namespace, key_value)
    ON DELETE CASCADE,
    CHECK (expires_at IS NULL OR expires_at > observed_at)
);

CREATE INDEX source_coordinate_bindings_reference_idx
ON source_coordinate_bindings (
    reference_namespace,
    reference_value,
    selector,
    source_id,
    item_key_namespace,
    item_key_value
);

CREATE TABLE sync_policies (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    head_revision BIGINT NOT NULL CHECK (head_revision > 0),
    head_content_hash TEXT NOT NULL CHECK (
        head_content_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (id, workspace_id)
);

CREATE TABLE sync_policy_revisions (
    policy_id UUID NOT NULL,
    workspace_id UUID NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    content_hash TEXT NOT NULL CHECK (
        content_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    schema_name TEXT NOT NULL CHECK (schema_name = 'trakkin.sync-policy/v1'),
    canonical_json BYTEA NOT NULL CHECK (octet_length(canonical_json) > 0),
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (policy_id, revision),
    UNIQUE (policy_id, workspace_id, revision),
    UNIQUE (policy_id, workspace_id, revision, content_hash),
    UNIQUE (policy_id, content_hash),
    FOREIGN KEY (policy_id, workspace_id)
    REFERENCES sync_policies(id, workspace_id)
    ON DELETE CASCADE
);

CREATE TABLE sync_policy_source_roles (
    policy_id UUID NOT NULL,
    workspace_id UUID NOT NULL,
    revision BIGINT NOT NULL,
    source_id UUID NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('contributor', 'receiver')),
    PRIMARY KEY (policy_id, revision, source_id, role),
    FOREIGN KEY (policy_id, workspace_id, revision)
    REFERENCES sync_policy_revisions(policy_id, workspace_id, revision)
    ON DELETE CASCADE
);

CREATE INDEX sync_policy_source_roles_workspace_source_idx
ON sync_policy_source_roles (workspace_id, source_id, role);

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

CREATE INDEX state_observation_tombstones_subject_idx
ON state_observation_tombstones (
    source_id,
    subject_kind,
    subject_key_namespace,
    subject_key_value
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

CREATE TABLE sync_previews (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    policy_id UUID NOT NULL,
    policy_revision BIGINT NOT NULL CHECK (policy_revision > 0),
    policy_content_hash TEXT NOT NULL CHECK (
        policy_content_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    content_hash TEXT NOT NULL CHECK (
        content_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    schema_name TEXT NOT NULL CHECK (
        schema_name = 'trakkin.sync-preview/v1'
    ),
    canonical_json BYTEA NOT NULL CHECK (octet_length(canonical_json) > 0),
    fingerprint_hash TEXT NOT NULL CHECK (
        fingerprint_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    fingerprint_json BYTEA NOT NULL CHECK (
        octet_length(fingerprint_json) > 0
    ),
    created_at TIMESTAMPTZ NOT NULL,
    fresh_until TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    UNIQUE (id, workspace_id, content_hash),
    UNIQUE (workspace_id, content_hash),
    FOREIGN KEY (
        policy_id,
        workspace_id,
        policy_revision,
        policy_content_hash
    ) REFERENCES sync_policy_revisions(
        policy_id,
        workspace_id,
        revision,
        content_hash
    ),
    CHECK (expires_at > created_at),
    CHECK (fresh_until > created_at AND fresh_until <= expires_at)
);

CREATE INDEX sync_previews_workspace_created_idx
ON sync_previews (workspace_id, created_at DESC, id);

CREATE TABLE sync_preview_actions (
    preview_id UUID NOT NULL,
    workspace_id UUID NOT NULL,
    preview_hash TEXT NOT NULL CHECK (
        preview_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    action_hash TEXT NOT NULL CHECK (
        action_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    receiver_source_id UUID NOT NULL,
    receiver_subject_kind TEXT NOT NULL CHECK (
        receiver_subject_kind IN ('provider_item', 'catalog_relation')
    ),
    receiver_subject_namespace TEXT NOT NULL CHECK (
        receiver_subject_namespace <> ''
    ),
    receiver_subject_value BYTEA NOT NULL CHECK (
        octet_length(receiver_subject_value) > 0
    ),
    semantic_field TEXT NOT NULL CHECK (semantic_field <> ''),
    segment_coordinate TEXT NOT NULL CHECK (segment_coordinate <> ''),
    segment_start BIGINT NOT NULL CHECK (segment_start >= 0),
    segment_end BIGINT NOT NULL CHECK (segment_end >= segment_start),
    canonical_json BYTEA NOT NULL CHECK (octet_length(canonical_json) > 0),
    PRIMARY KEY (preview_id, ordinal),
    UNIQUE (preview_id, action_hash),
    FOREIGN KEY (preview_id, workspace_id, preview_hash)
    REFERENCES sync_previews(id, workspace_id, content_hash)
    ON DELETE CASCADE
);

CREATE INDEX sync_preview_actions_receiver_idx
ON sync_preview_actions (
    workspace_id,
    receiver_source_id,
    semantic_field,
    segment_coordinate,
    segment_start,
    segment_end
);

CREATE FUNCTION reject_sync_preview_update()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'synchronization preview history is immutable';
END;
$$;

CREATE TRIGGER sync_previews_reject_update
BEFORE UPDATE ON sync_previews
FOR EACH ROW EXECUTE FUNCTION reject_sync_preview_update();

CREATE TRIGGER sync_preview_actions_reject_update
BEFORE UPDATE ON sync_preview_actions
FOR EACH ROW EXECUTE FUNCTION reject_sync_preview_update();

CREATE TABLE sync_schedules (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL,
    policy_id UUID NOT NULL,
    interval_seconds BIGINT NOT NULL CHECK (
        interval_seconds BETWEEN 60 AND 2678400
    ),
    state TEXT NOT NULL CHECK (state IN ('enabled', 'paused')),
    next_run_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (id, workspace_id, policy_id),
    UNIQUE (workspace_id, policy_id),
    FOREIGN KEY (policy_id, workspace_id)
    REFERENCES sync_policies(id, workspace_id)
    ON DELETE CASCADE,
    CHECK ((state = 'enabled') = (next_run_at IS NOT NULL)),
    CHECK (updated_at >= created_at)
);

CREATE INDEX sync_schedules_due_idx
ON sync_schedules (next_run_at, id)
WHERE state = 'enabled';

CREATE TABLE sync_schedule_occurrences (
    id UUID PRIMARY KEY,
    schedule_id UUID NOT NULL,
    workspace_id UUID NOT NULL,
    policy_id UUID NOT NULL,
    scheduled_for TIMESTAMPTZ NOT NULL,
    preview_id UUID NOT NULL UNIQUE,
    run_id UUID NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('pending', 'completed')),
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    UNIQUE (id, workspace_id, run_id),
    UNIQUE (schedule_id, scheduled_for),
    FOREIGN KEY (schedule_id, workspace_id, policy_id)
    REFERENCES sync_schedules(id, workspace_id, policy_id)
    ON DELETE CASCADE,
    CHECK ((status = 'completed') = (completed_at IS NOT NULL)),
    CHECK (created_at >= scheduled_for),
    CHECK (completed_at IS NULL OR completed_at >= created_at)
);

CREATE INDEX sync_schedule_occurrences_pending_idx
ON sync_schedule_occurrences (scheduled_for, id)
WHERE status = 'pending';

CREATE TABLE sync_runs (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    preview_id UUID NOT NULL,
    preview_hash TEXT NOT NULL CHECK (
        preview_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    schedule_occurrence_id UUID UNIQUE,
    status TEXT NOT NULL DEFAULT 'queued' CHECK (
        status IN (
            'queued',
            'running',
            'succeeded',
            'failed',
            'partial',
            'indeterminate',
            'cancelled'
        )
    ),
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (id, workspace_id, preview_id),
    UNIQUE (preview_id),
    FOREIGN KEY (preview_id, workspace_id, preview_hash)
    REFERENCES sync_previews(id, workspace_id, content_hash)
    ON DELETE RESTRICT,
    FOREIGN KEY (schedule_occurrence_id, workspace_id, id)
    REFERENCES sync_schedule_occurrences(id, workspace_id, run_id)
    ON DELETE RESTRICT,
    CHECK ((
        (
            status = 'queued'
            AND started_at IS NULL
            AND finished_at IS NULL
        )
        OR (
            status = 'running'
            AND started_at IS NOT NULL
            AND finished_at IS NULL
        )
        OR (
            status IN (
                'succeeded',
                'failed',
                'partial',
                'indeterminate',
                'cancelled'
            )
            AND finished_at IS NOT NULL
        )
    ) IS TRUE),
    CHECK (
        started_at IS NULL
        OR finished_at IS NULL
        OR finished_at >= started_at
    )
);

CREATE INDEX sync_runs_workspace_created_idx
ON sync_runs (workspace_id, created_at DESC, id);

CREATE TABLE sync_actions (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL,
    workspace_id UUID NOT NULL,
    preview_id UUID NOT NULL,
    preview_action_hash TEXT NOT NULL CHECK (
        preview_action_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    receiver_source_id UUID NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued' CHECK (
        status IN (
            'queued',
            'running',
            'applied',
            'no_op',
            'precondition_failed',
            'retryable_failure',
            'failed',
            'indeterminate',
            'cancelled'
        )
    ),
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (id, receiver_source_id),
    UNIQUE (run_id, preview_action_hash),
    UNIQUE (preview_id, preview_action_hash),
    FOREIGN KEY (run_id, workspace_id, preview_id)
    REFERENCES sync_runs(id, workspace_id, preview_id)
    ON DELETE RESTRICT,
    FOREIGN KEY (preview_id, preview_action_hash)
    REFERENCES sync_preview_actions(preview_id, action_hash)
    ON DELETE RESTRICT,
    CHECK ((
        (
            status = 'queued'
            AND started_at IS NULL
            AND finished_at IS NULL
        )
        OR (
            status = 'running'
            AND started_at IS NOT NULL
            AND finished_at IS NULL
        )
        OR (
            status IN (
                'applied',
                'no_op',
                'precondition_failed',
                'retryable_failure',
                'failed',
                'indeterminate',
                'cancelled'
            )
            AND finished_at IS NOT NULL
        )
    ) IS TRUE),
    CHECK (
        started_at IS NULL
        OR finished_at IS NULL
        OR finished_at >= started_at
    )
);

CREATE INDEX sync_actions_run_idx
ON sync_actions (run_id, created_at, id);

CREATE INDEX sync_actions_run_status_idx
ON sync_actions (run_id, status, id);

CREATE TABLE sync_action_tasks (
    task_id UUID PRIMARY KEY REFERENCES tasks(id) ON DELETE RESTRICT,
    action_id UUID NOT NULL UNIQUE,
    receiver_source_id UUID NOT NULL,
    UNIQUE (task_id, action_id, receiver_source_id),
    FOREIGN KEY (action_id, receiver_source_id)
    REFERENCES sync_actions(id, receiver_source_id)
    ON DELETE RESTRICT
);

CREATE TABLE sync_action_attempts (
    task_attempt_id UUID PRIMARY KEY,
    task_id UUID NOT NULL,
    action_id UUID NOT NULL,
    receiver_source_id UUID NOT NULL,
    owner_id UUID NOT NULL,
    operation_id UUID NOT NULL UNIQUE,
    UNIQUE (task_attempt_id, action_id),
    idempotency_key UUID NOT NULL,
    preview_action_hash TEXT NOT NULL CHECK (
        preview_action_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    receiver_subject_kind TEXT NOT NULL CHECK (
        receiver_subject_kind IN ('provider_item', 'catalog_relation')
    ),
    receiver_subject_namespace TEXT NOT NULL CHECK (
        receiver_subject_namespace <> ''
    ),
    receiver_subject_value BYTEA NOT NULL CHECK (
        octet_length(receiver_subject_value) > 0
    ),
    semantic_field TEXT NOT NULL CHECK (semantic_field <> ''),
    provider_field_namespace TEXT NOT NULL CHECK (
        provider_field_namespace <> ''
    ),
    provider_field_name TEXT NOT NULL CHECK (provider_field_name <> ''),
    provider_unit_namespace TEXT,
    provider_unit_name TEXT,
    intended_value BIGINT NOT NULL CHECK (intended_value >= 0),
    expected_membership TEXT NOT NULL CHECK (
        expected_membership IN ('present', 'absent')
    ),
    precondition BYTEA,
    allow_create_membership BOOLEAN NOT NULL,
    source_key_namespace TEXT,
    source_key_value BYTEA,
    write_capability JSONB,
    settlement_kind TEXT,
    outcome_status TEXT,
    outcome_certainty TEXT,
    outcome_retry_disposition TEXT,
    outcome_membership_effect TEXT,
    outcome_provider_revision BYTEA,
    outcome_successor_precondition BYTEA,
    outcome_receipt BYTEA,
    outcome_error_category TEXT,
    outcome_error_code TEXT,
    outcome_error_retryable BOOLEAN,
    outcome_error_safe_message TEXT,
    outcome_error_diagnostic_id TEXT,
    outcome_error_retry_after_seconds BIGINT,
    outcome_error_retry_after_nanoseconds INTEGER,
    state TEXT NOT NULL CHECK (
        state IN ('prepared', 'dispatched', 'settled')
    ),
    recovery_result TEXT CHECK (
        recovery_result IN ('confirmed_not_applied', 'indeterminate')
    ),
    prepared_at TIMESTAMPTZ NOT NULL,
    dispatched_at TIMESTAMPTZ,
    settled_at TIMESTAMPTZ,
    FOREIGN KEY (task_id, action_id, receiver_source_id)
    REFERENCES sync_action_tasks(task_id, action_id, receiver_source_id)
    ON DELETE RESTRICT,
    FOREIGN KEY (task_attempt_id, task_id)
    REFERENCES task_attempts(id, task_id)
    ON DELETE RESTRICT,
    CHECK (idempotency_key = action_id),
    CHECK (
        (provider_unit_namespace IS NULL) = (provider_unit_name IS NULL)
    ),
    CHECK (
        (expected_membership = 'absent') = allow_create_membership
    ),
    CONSTRAINT sync_action_attempts_dispatch_target_check CHECK (
        (
            source_key_namespace IS NULL
            AND source_key_value IS NULL
            AND write_capability IS NULL
        )
        OR (
            source_key_namespace IS NOT NULL
            AND source_key_namespace <> ''
            AND source_key_value IS NOT NULL
            AND octet_length(source_key_value) > 0
            AND write_capability IS NOT NULL
            AND jsonb_typeof(write_capability) = 'object'
        )
    ),
    CONSTRAINT sync_action_attempts_settlement_kind_check CHECK (
        settlement_kind IN ('recovery', 'provider_outcome')
    ),
    CONSTRAINT sync_action_attempts_provider_outcome_check CHECK ((
        (
            outcome_status IS NULL
            AND outcome_certainty IS NULL
            AND outcome_retry_disposition IS NULL
            AND outcome_membership_effect IS NULL
            AND outcome_provider_revision IS NULL
            AND outcome_successor_precondition IS NULL
            AND outcome_receipt IS NULL
            AND outcome_error_category IS NULL
            AND outcome_error_code IS NULL
            AND outcome_error_retryable IS NULL
            AND outcome_error_safe_message IS NULL
            AND outcome_error_diagnostic_id IS NULL
            AND outcome_error_retry_after_seconds IS NULL
            AND outcome_error_retry_after_nanoseconds IS NULL
        )
        OR (
            outcome_status IN (
                'applied', 'no_op', 'rejected', 'failed', 'indeterminate'
            )
            AND outcome_certainty IN (
                'confirmed_applied',
                'confirmed_not_applied',
                'unknown'
            )
            AND outcome_retry_disposition IN (
                'not_retryable',
                'safe_same_key',
                'reconcile_first'
            )
            AND outcome_membership_effect IN ('unchanged', 'created', 'unknown')
            AND outcome_provider_revision IS NOT NULL
            AND outcome_receipt IS NOT NULL
            AND (
                (
                    outcome_error_category IS NULL
                    AND outcome_error_code IS NULL
                    AND outcome_error_retryable IS NULL
                    AND outcome_error_safe_message IS NULL
                    AND outcome_error_diagnostic_id IS NULL
                    AND outcome_error_retry_after_seconds IS NULL
                    AND outcome_error_retry_after_nanoseconds IS NULL
                )
                OR (
                    outcome_error_category IN (
                        'invalid_input',
                        'authentication',
                        'authorization',
                        'rate_limited',
                        'unavailable',
                        'conflict',
                        'unsupported',
                        'invalid_remote_data',
                        'internal'
                    )
                    AND outcome_error_code IS NOT NULL
                    AND outcome_error_code <> ''
                    AND outcome_error_retryable IS NOT NULL
                    AND outcome_error_safe_message IS NOT NULL
                    AND outcome_error_safe_message <> ''
                    AND outcome_error_diagnostic_id IS NOT NULL
                    AND outcome_error_diagnostic_id <> ''
                    AND (
                        (
                            outcome_error_retry_after_seconds IS NULL
                            AND outcome_error_retry_after_nanoseconds IS NULL
                        )
                        OR (
                            outcome_error_retry_after_seconds IS NOT NULL
                            AND outcome_error_retry_after_seconds >= 0
                            AND outcome_error_retry_after_nanoseconds
                            BETWEEN 0 AND 999999999
                        )
                    )
                )
            )
        )
    ) IS TRUE),
    CONSTRAINT sync_action_attempts_state_shape_v2_check CHECK (
        (
            state = 'prepared'
            AND dispatched_at IS NULL
            AND settled_at IS NULL
            AND settlement_kind IS NULL
            AND recovery_result IS NULL
            AND outcome_status IS NULL
        )
        OR (
            state = 'dispatched'
            AND dispatched_at IS NOT NULL
            AND settled_at IS NULL
            AND settlement_kind IS NULL
            AND recovery_result IS NULL
            AND outcome_status IS NULL
        )
        OR (
            state = 'settled'
            AND settled_at IS NOT NULL
            AND (
                (
                    settlement_kind = 'recovery'
                    AND recovery_result IS NOT NULL
                    AND outcome_status IS NULL
                )
                OR (
                    settlement_kind = 'provider_outcome'
                    AND recovery_result IS NULL
                    AND outcome_status IS NOT NULL
                )
            )
        )
    ),
    CHECK (dispatched_at IS NULL OR dispatched_at >= prepared_at),
    CHECK (
        settled_at IS NULL
        OR settled_at >= coalesce(dispatched_at, prepared_at)
    ),
    CHECK (
        recovery_result IS DISTINCT FROM 'confirmed_not_applied'
        OR dispatched_at IS NULL
    ),
    CHECK (
        recovery_result IS DISTINCT FROM 'indeterminate'
        OR dispatched_at IS NOT NULL
    )
);

CREATE INDEX sync_action_attempts_action_idx
ON sync_action_attempts (action_id, prepared_at, task_attempt_id);

CREATE TABLE sync_action_attempt_field_effects (
    task_attempt_id UUID NOT NULL REFERENCES sync_action_attempts(
        task_attempt_id
    )
    ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    field_namespace TEXT NOT NULL CHECK (field_namespace <> ''),
    field_name TEXT NOT NULL CHECK (field_name <> ''),
    unit_namespace TEXT,
    unit_name TEXT,
    effect TEXT NOT NULL CHECK (
        effect IN ('set', 'cleared', 'unchanged', 'unknown')
    ),
    value_kind TEXT CHECK (
        value_kind IN (
            'boolean',
            'integer',
            'decimal',
            'text',
            'bytes',
            'timestamp_milliseconds'
        )
    ),
    boolean_value BOOLEAN,
    integer_value BIGINT,
    decimal_value TEXT,
    text_value TEXT,
    bytes_value BYTEA,
    timestamp_milliseconds BIGINT,
    PRIMARY KEY (task_attempt_id, ordinal),
    CHECK ((unit_namespace IS NULL) = (unit_name IS NULL)),
    CHECK (
        (
            value_kind IS NULL
            AND boolean_value IS NULL
            AND integer_value IS NULL
            AND decimal_value IS NULL
            AND text_value IS NULL
            AND bytes_value IS NULL
            AND timestamp_milliseconds IS NULL
        )
        OR (
            value_kind = 'boolean'
            AND boolean_value IS NOT NULL
            AND integer_value IS NULL
            AND decimal_value IS NULL
            AND text_value IS NULL
            AND bytes_value IS NULL
            AND timestamp_milliseconds IS NULL
        )
        OR (
            value_kind = 'integer'
            AND boolean_value IS NULL
            AND integer_value IS NOT NULL
            AND decimal_value IS NULL
            AND text_value IS NULL
            AND bytes_value IS NULL
            AND timestamp_milliseconds IS NULL
        )
        OR (
            value_kind = 'decimal'
            AND boolean_value IS NULL
            AND integer_value IS NULL
            AND decimal_value IS NOT NULL
            AND text_value IS NULL
            AND bytes_value IS NULL
            AND timestamp_milliseconds IS NULL
        )
        OR (
            value_kind = 'text'
            AND boolean_value IS NULL
            AND integer_value IS NULL
            AND decimal_value IS NULL
            AND text_value IS NOT NULL
            AND bytes_value IS NULL
            AND timestamp_milliseconds IS NULL
        )
        OR (
            value_kind = 'bytes'
            AND boolean_value IS NULL
            AND integer_value IS NULL
            AND decimal_value IS NULL
            AND text_value IS NULL
            AND bytes_value IS NOT NULL
            AND timestamp_milliseconds IS NULL
        )
        OR (
            value_kind = 'timestamp_milliseconds'
            AND boolean_value IS NULL
            AND integer_value IS NULL
            AND decimal_value IS NULL
            AND text_value IS NULL
            AND bytes_value IS NULL
            AND timestamp_milliseconds IS NOT NULL
        )
    )
);

CREATE TABLE sync_action_attempt_error_field_problems (
    task_attempt_id UUID NOT NULL REFERENCES sync_action_attempts(
        task_attempt_id
    )
    ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    path TEXT NOT NULL CHECK (path <> ''),
    code TEXT NOT NULL CHECK (code <> ''),
    message TEXT NOT NULL CHECK (message <> ''),
    PRIMARY KEY (task_attempt_id, ordinal)
);

CREATE TABLE sync_action_reconciliation_tasks (
    task_id UUID PRIMARY KEY REFERENCES tasks(id) ON DELETE RESTRICT,
    action_id UUID NOT NULL,
    receiver_source_id UUID NOT NULL,
    write_task_attempt_id UUID NOT NULL UNIQUE,
    UNIQUE (
        task_id,
        action_id,
        receiver_source_id,
        write_task_attempt_id
    ),
    FOREIGN KEY (action_id, receiver_source_id)
    REFERENCES sync_actions(id, receiver_source_id)
    ON DELETE RESTRICT,
    FOREIGN KEY (write_task_attempt_id, action_id)
    REFERENCES sync_action_attempts(task_attempt_id, action_id)
    ON DELETE RESTRICT
);

CREATE TABLE sync_action_reconciliation_attempts (
    task_attempt_id UUID PRIMARY KEY,
    task_id UUID NOT NULL,
    action_id UUID NOT NULL,
    receiver_source_id UUID NOT NULL,
    write_task_attempt_id UUID NOT NULL,
    owner_id UUID NOT NULL,
    operation_id UUID NOT NULL UNIQUE,
    UNIQUE (task_attempt_id, action_id, write_task_attempt_id),
    idempotency_key UUID NOT NULL,
    source_key_namespace TEXT NOT NULL CHECK (source_key_namespace <> ''),
    source_key_value BYTEA NOT NULL CHECK (octet_length(source_key_value) > 0),
    receiver_subject_kind TEXT NOT NULL CHECK (
        receiver_subject_kind IN ('provider_item', 'catalog_relation')
    ),
    receiver_subject_namespace TEXT NOT NULL CHECK (
        receiver_subject_namespace <> ''
    ),
    receiver_subject_value BYTEA NOT NULL CHECK (
        octet_length(receiver_subject_value) > 0
    ),
    provider_field_namespace TEXT NOT NULL CHECK (
        provider_field_namespace <> ''
    ),
    provider_field_name TEXT NOT NULL CHECK (provider_field_name <> ''),
    provider_unit_namespace TEXT,
    provider_unit_name TEXT,
    intended_value BIGINT NOT NULL CHECK (intended_value >= 0),
    before_membership TEXT NOT NULL CHECK (
        before_membership IN ('present', 'absent')
    ),
    before_presence TEXT NOT NULL CHECK (
        before_presence IN (
            'present',
            'deleted',
            'absent',
            'unknown',
            'stale'
        )
    ),
    before_value BIGINT,
    before_provider_revision BYTEA NOT NULL CHECK (
        octet_length(before_provider_revision) > 0
    ),
    maximum_response_bytes BIGINT NOT NULL CHECK (maximum_response_bytes > 0),
    state TEXT NOT NULL CHECK (state IN ('prepared', 'settled')),
    outcome_kind TEXT CHECK (
        outcome_kind IN (
            'matched',
            'unsupported',
            'not_found',
            'ambiguous',
            'indeterminate'
        )
    ),
    observed_membership TEXT CHECK (
        observed_membership IN ('present', 'absent', 'unknown')
    ),
    observed_presence TEXT CHECK (
        observed_presence IN (
            'present',
            'deleted',
            'absent',
            'unknown',
            'stale'
        )
    ),
    observed_value BIGINT,
    observed_provider_revision BYTEA,
    observed_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    observed_precondition BYTEA,
    causation_idempotency_key UUID,
    causation_receipt BYTEA,
    causation_provider_revision BYTEA,
    result TEXT CHECK (
        result IN (
            'confirmed_applied',
            'confirmed_not_applied',
            'state_matches_causation_unknown',
            'unresolved'
        )
    ),
    outcome_error_code TEXT,
    outcome_error_retryable BOOLEAN,
    outcome_error_safe_message TEXT,
    prepared_at TIMESTAMPTZ NOT NULL,
    settled_at TIMESTAMPTZ,
    FOREIGN KEY (
        task_id,
        action_id,
        receiver_source_id,
        write_task_attempt_id
    )
    REFERENCES sync_action_reconciliation_tasks(
        task_id,
        action_id,
        receiver_source_id,
        write_task_attempt_id
    )
    ON DELETE RESTRICT,
    FOREIGN KEY (task_attempt_id, task_id)
    REFERENCES task_attempts(id, task_id)
    ON DELETE RESTRICT,
    CHECK (idempotency_key = action_id),
    CHECK (
        (provider_unit_namespace IS NULL) = (provider_unit_name IS NULL)
    ),
    CHECK ((before_presence = 'present') = (before_value IS NOT NULL)),
    CHECK (
        (observed_presence = 'present') = (observed_value IS NOT NULL)
        OR observed_presence IS NULL
    ),
    CHECK (
        (causation_idempotency_key IS NULL)
        = (causation_receipt IS NULL)
        AND (causation_receipt IS NULL)
        = (causation_provider_revision IS NULL)
    ),
    CHECK (
        causation_idempotency_key IS NULL
        OR (
            causation_idempotency_key = action_id
            AND octet_length(causation_receipt) > 0
            AND causation_provider_revision = observed_provider_revision
        )
    ),
    CHECK (
        (
            state = 'prepared'
            AND outcome_kind IS NULL
            AND result IS NULL
            AND settled_at IS NULL
        )
        OR (
            state = 'settled'
            AND outcome_kind IS NOT NULL
            AND result IS NOT NULL
            AND settled_at IS NOT NULL
        )
    ),
    CHECK (
        outcome_kind IS DISTINCT FROM 'matched'
        OR (
            observed_membership IS NOT NULL
            AND observed_presence IS NOT NULL
            AND octet_length(observed_provider_revision) > 0
            AND observed_at IS NOT NULL
            AND outcome_error_code IS NULL
        )
    ),
    CHECK (
        outcome_kind IS NULL
        OR outcome_kind = 'matched'
        OR (
            observed_membership IS NULL
            AND observed_presence IS NULL
            AND observed_value IS NULL
            AND observed_provider_revision IS NULL
            AND observed_at IS NULL
            AND expires_at IS NULL
            AND observed_precondition IS NULL
            AND causation_idempotency_key IS NULL
        )
    ),
    CHECK (
        outcome_kind IS DISTINCT FROM 'indeterminate'
        OR (
            outcome_error_code IS NOT NULL
            AND outcome_error_retryable IS NOT NULL
            AND outcome_error_safe_message IS NOT NULL
            AND result = 'unresolved'
        )
    ),
    CHECK (
        outcome_kind IS NULL
        OR outcome_kind = 'indeterminate'
        OR (
            outcome_error_code IS NULL
            AND outcome_error_retryable IS NULL
            AND outcome_error_safe_message IS NULL
        )
    ),
    CHECK (
        result IS NULL
        OR result = 'unresolved'
        OR (
            result = 'confirmed_applied'
            AND outcome_kind = 'matched'
            AND observed_membership = 'present'
            AND observed_presence = 'present'
            AND observed_value = intended_value
            AND causation_idempotency_key IS NOT NULL
        )
        OR (
            result = 'confirmed_not_applied'
            AND outcome_kind = 'matched'
            AND observed_membership = before_membership
            AND observed_presence = before_presence
            AND observed_value IS NOT DISTINCT FROM before_value
            AND observed_provider_revision = before_provider_revision
            AND causation_idempotency_key IS NULL
        )
        OR (
            result = 'state_matches_causation_unknown'
            AND outcome_kind = 'matched'
            AND observed_membership = 'present'
            AND observed_presence = 'present'
            AND observed_value = intended_value
            AND causation_idempotency_key IS NULL
        )
    ),
    CHECK (settled_at IS NULL OR settled_at >= prepared_at),
    CHECK (expires_at IS NULL OR expires_at > observed_at)
);

CREATE INDEX sync_action_reconciliation_attempts_action_idx
ON sync_action_reconciliation_attempts (
    action_id,
    prepared_at,
    task_attempt_id
);

CREATE TABLE sync_action_provenance (
    action_id UUID PRIMARY KEY REFERENCES sync_actions(id) ON DELETE RESTRICT,
    terminal_task_attempt_id UUID NOT NULL UNIQUE,
    reconciliation_task_attempt_id UUID UNIQUE,
    recorded_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (terminal_task_attempt_id, action_id)
    REFERENCES sync_action_attempts(task_attempt_id, action_id)
    ON DELETE RESTRICT,
    FOREIGN KEY (
        reconciliation_task_attempt_id,
        action_id,
        terminal_task_attempt_id
    ) REFERENCES sync_action_reconciliation_attempts(
        task_attempt_id,
        action_id,
        write_task_attempt_id
    ) ON DELETE RESTRICT,
    CHECK (
        reconciliation_task_attempt_id IS NULL
        OR reconciliation_task_attempt_id <> terminal_task_attempt_id
    )
);

CREATE FUNCTION reject_sync_action_provenance_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	IF
		TG_OP = 'DELETE'
		AND current_setting('trakkin.sync_retention', TRUE) = 'enabled'
	THEN
		RETURN OLD;
	END IF;
	RAISE EXCEPTION 'synchronization action provenance is immutable';
END;
$$;

CREATE TRIGGER sync_action_provenance_reject_update
BEFORE UPDATE ON sync_action_provenance
FOR EACH ROW EXECUTE FUNCTION reject_sync_action_provenance_mutation();

CREATE TRIGGER sync_action_provenance_reject_delete
BEFORE DELETE ON sync_action_provenance
FOR EACH ROW EXECUTE FUNCTION reject_sync_action_provenance_mutation();

CREATE INDEX sync_previews_retention_idx
ON sync_previews (workspace_id, expires_at, id);

CREATE INDEX sync_runs_retention_idx
ON sync_runs (workspace_id, finished_at, id)
WHERE finished_at IS NOT NULL;

CREATE INDEX sync_policy_revisions_retention_idx
ON sync_policy_revisions (workspace_id, created_at, policy_id, revision);
