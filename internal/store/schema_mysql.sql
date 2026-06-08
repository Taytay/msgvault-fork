-- msgvault MySQL / Dolt schema
-- Parallel to schema.sql (SQLite) and schema_pg.sql (PostgreSQL).
--
-- This schema targets Dolt (MySQL wire protocol). It is the *system of
-- record* schema only: full-text search (SQLite FTS5 / PG tsvector) is NOT
-- represented here because, under the Dolt architecture, keyword and
-- semantic search are served by a local SQLite read-replica rebuilt from
-- Dolt (see docs/research/msgvault-dolt-architecture-plan.md). So there is
-- no search_fts column and no FTS index.
--
-- Translation notes vs schema_pg.sql:
--   * BIGINT GENERATED ALWAYS AS IDENTITY  -> BIGINT AUTO_INCREMENT
--   * TIMESTAMPTZ                           -> DATETIME(6) (microsecond precision;
--     bare DATETIME truncates to whole seconds, losing fidelity SQLite/PG keep)
--   * BYTEA                                 -> LONGBLOB
--   * JSONB                                 -> JSON
--   * TEXT used in an index/unique/PK       -> VARCHAR(n) (MySQL cannot index
--     a bare TEXT column without a prefix length). Free-text columns that are
--     never indexed stay TEXT / LONGTEXT.
--   * Partial unique indexes (... WHERE col IS NOT NULL) become plain UNIQUE
--     keys: MySQL UNIQUE indexes already permit multiple NULLs, which matches
--     the partial-index intent for email_address / phone_number / canonical_id.
--   * Indexes are declared inline so a re-run is a no-op under
--     CREATE TABLE IF NOT EXISTS (MySQL has no CREATE INDEX IF NOT EXISTS).

-- ============================================================================
-- SOURCES & IDENTITY
-- ============================================================================

CREATE TABLE IF NOT EXISTS sources (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_type VARCHAR(64) NOT NULL,
    identifier VARCHAR(320) NOT NULL,
    display_name TEXT,

    google_user_id VARCHAR(255),

    last_sync_at DATETIME(6),
    sync_cursor TEXT,
    sync_config JSON,
    oauth_app TEXT,

    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),

    UNIQUE KEY uq_sources_type_identifier (source_type, identifier),
    UNIQUE KEY uq_sources_google_user_id (google_user_id),
    KEY idx_sources_type (source_type)
);

CREATE TABLE IF NOT EXISTS participants (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    email_address VARCHAR(320),
    phone_number VARCHAR(64),
    display_name TEXT,
    domain VARCHAR(255),

    canonical_id VARCHAR(255),

    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),

    -- Partial unique indexes in PG (WHERE ... IS NOT NULL) map to plain
    -- UNIQUE keys: MySQL allows multiple NULLs in a UNIQUE index.
    UNIQUE KEY idx_participants_email (email_address),
    UNIQUE KEY idx_participants_phone (phone_number),
    KEY idx_participants_canonical (canonical_id)
);

CREATE TABLE IF NOT EXISTS participant_identifiers (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    participant_id BIGINT NOT NULL,

    identifier_type VARCHAR(64) NOT NULL,
    identifier_value VARCHAR(320) NOT NULL,
    display_value TEXT,

    is_primary BOOLEAN DEFAULT FALSE,

    UNIQUE KEY uq_participant_identifiers (identifier_type, identifier_value),
    KEY idx_participant_identifiers_value (identifier_value),
    KEY idx_participant_identifiers_participant (participant_id),
    CONSTRAINT fk_pi_participant FOREIGN KEY (participant_id)
        REFERENCES participants(id) ON DELETE CASCADE
);

-- ============================================================================
-- CONVERSATIONS & MESSAGES
-- ============================================================================

CREATE TABLE IF NOT EXISTS conversations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_id BIGINT NOT NULL,

    source_conversation_id VARCHAR(255),

    conversation_type VARCHAR(64) NOT NULL DEFAULT 'email_thread',
    title TEXT,

    participant_count INTEGER DEFAULT 0,
    message_count INTEGER DEFAULT 0,
    unread_count INTEGER DEFAULT 0,
    last_message_at DATETIME(6),
    last_message_preview TEXT,

    metadata JSON,

    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),

    UNIQUE KEY uq_conversations_source_conv (source_id, source_conversation_id),
    KEY idx_conversations_source (source_id),
    KEY idx_conversations_last_message (last_message_at),
    KEY idx_conversations_type (conversation_type),
    CONSTRAINT fk_conversations_source FOREIGN KEY (source_id)
        REFERENCES sources(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS conversation_participants (
    conversation_id BIGINT NOT NULL,
    participant_id BIGINT NOT NULL,

    role VARCHAR(32) DEFAULT 'member',
    joined_at DATETIME(6),
    left_at DATETIME(6),

    PRIMARY KEY (conversation_id, participant_id),
    CONSTRAINT fk_cp_conversation FOREIGN KEY (conversation_id)
        REFERENCES conversations(id) ON DELETE CASCADE,
    CONSTRAINT fk_cp_participant FOREIGN KEY (participant_id)
        REFERENCES participants(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS messages (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    conversation_id BIGINT NOT NULL,
    source_id BIGINT NOT NULL,

    -- 512 is generous for provider message IDs; it must stay VARCHAR because it
    -- participates in uq_messages_source_msg / idx_messages_source_message_id
    -- (MySQL/Dolt reject overflow rather than truncating).
    source_message_id VARCHAR(512),
    rfc822_message_id TEXT,

    message_type VARCHAR(64) NOT NULL DEFAULT 'email',

    sent_at DATETIME(6),
    received_at DATETIME(6),
    read_at DATETIME(6),
    delivered_at DATETIME(6),
    internal_date DATETIME(6),

    sender_id BIGINT,
    is_from_me BOOLEAN DEFAULT FALSE,

    subject TEXT,
    snippet TEXT,

    reply_to_message_id BIGINT,
    thread_position INTEGER,

    is_read BOOLEAN DEFAULT TRUE,
    is_delivered BOOLEAN,
    is_sent BOOLEAN DEFAULT TRUE,
    is_edited BOOLEAN DEFAULT FALSE,
    is_forwarded BOOLEAN DEFAULT FALSE,

    size_estimate BIGINT,
    has_attachments BOOLEAN DEFAULT FALSE,
    attachment_count INTEGER DEFAULT 0,

    deleted_at DATETIME(6),
    deleted_from_source_at DATETIME(6),
    delete_batch_id VARCHAR(255),

    archived_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    indexing_version INTEGER DEFAULT 1,

    metadata JSON,

    UNIQUE KEY uq_messages_source_msg (source_id, source_message_id),
    KEY idx_messages_conversation (conversation_id, sent_at),
    KEY idx_messages_source (source_id),
    KEY idx_messages_sender (sender_id),
    KEY idx_messages_sent_at (sent_at),
    KEY idx_messages_type (message_type),
    KEY idx_messages_deleted (source_id, deleted_from_source_at),
    KEY idx_messages_source_message_id (source_message_id),
    CONSTRAINT fk_messages_conversation FOREIGN KEY (conversation_id)
        REFERENCES conversations(id) ON DELETE CASCADE,
    CONSTRAINT fk_messages_source FOREIGN KEY (source_id)
        REFERENCES sources(id) ON DELETE CASCADE,
    CONSTRAINT fk_messages_sender FOREIGN KEY (sender_id)
        REFERENCES participants(id),
    CONSTRAINT fk_messages_reply_to FOREIGN KEY (reply_to_message_id)
        REFERENCES messages(id)
);

CREATE TABLE IF NOT EXISTS message_recipients (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    message_id BIGINT NOT NULL,
    participant_id BIGINT NOT NULL,

    recipient_type VARCHAR(32) NOT NULL,
    display_name TEXT,

    UNIQUE KEY uq_message_recipients (message_id, participant_id, recipient_type),
    KEY idx_message_recipients_message (message_id),
    KEY idx_message_recipients_participant (participant_id, recipient_type),
    CONSTRAINT fk_mr_message FOREIGN KEY (message_id)
        REFERENCES messages(id) ON DELETE CASCADE,
    CONSTRAINT fk_mr_participant FOREIGN KEY (participant_id)
        REFERENCES participants(id) ON DELETE CASCADE
);

-- ============================================================================
-- REACTIONS
-- ============================================================================

CREATE TABLE IF NOT EXISTS reactions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    message_id BIGINT NOT NULL,
    participant_id BIGINT NOT NULL,

    reaction_type VARCHAR(64) NOT NULL,
    reaction_value VARCHAR(255) NOT NULL,

    created_at DATETIME(6),
    removed_at DATETIME(6),

    UNIQUE KEY uq_reactions (message_id, participant_id, reaction_type, reaction_value),
    KEY idx_reactions_message (message_id),
    CONSTRAINT fk_reactions_message FOREIGN KEY (message_id)
        REFERENCES messages(id) ON DELETE CASCADE,
    CONSTRAINT fk_reactions_participant FOREIGN KEY (participant_id)
        REFERENCES participants(id) ON DELETE CASCADE
);

-- ============================================================================
-- ATTACHMENTS
-- ============================================================================

CREATE TABLE IF NOT EXISTS attachments (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    message_id BIGINT NOT NULL,

    filename TEXT,
    mime_type VARCHAR(255),
    size BIGINT,

    content_hash VARCHAR(128),
    -- Reproduces the PG/SQLite partial unique index
    -- (UNIQUE (message_id, content_hash) WHERE content_hash IS NOT NULL AND != '')
    -- which MySQL cannot express directly. NULLIF maps '' (and NULL) to NULL, and
    -- MySQL UNIQUE indexes permit multiple NULLs, so empty/absent hashes never
    -- collide while real hashes are deduped — matching UpsertAttachment's intent.
    dedup_content_hash VARCHAR(128) AS (NULLIF(content_hash, '')) STORED,
    storage_path VARCHAR(512) NOT NULL DEFAULT '',

    media_type VARCHAR(64),
    width INTEGER,
    height INTEGER,
    duration_ms INTEGER,

    thumbnail_hash VARCHAR(128),
    thumbnail_path TEXT,

    source_attachment_id TEXT,
    attachment_metadata JSON,

    encryption_version INTEGER DEFAULT 0,

    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),

    KEY idx_attachments_message (message_id),
    KEY idx_attachments_hash (content_hash),
    KEY idx_attachments_storage_path (storage_path),
    -- Enforces attachment idempotency for non-empty hashes (see dedup_content_hash
    -- above); UpsertAttachment's DO NOTHING -> INSERT IGNORE relies on this key.
    UNIQUE KEY uq_attachments_msg_content_hash (message_id, dedup_content_hash),
    CONSTRAINT fk_attachments_message FOREIGN KEY (message_id)
        REFERENCES messages(id) ON DELETE CASCADE
);

-- ============================================================================
-- LABELS
-- ============================================================================

CREATE TABLE IF NOT EXISTS labels (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_id BIGINT,

    source_label_id VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    label_type VARCHAR(64),
    color VARCHAR(32),

    UNIQUE KEY uq_labels_source_name (source_id, name),
    KEY idx_labels_source (source_id),
    CONSTRAINT fk_labels_source FOREIGN KEY (source_id)
        REFERENCES sources(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS message_labels (
    message_id BIGINT NOT NULL,
    label_id BIGINT NOT NULL,

    PRIMARY KEY (message_id, label_id),
    KEY idx_message_labels_label (label_id),
    CONSTRAINT fk_ml_message FOREIGN KEY (message_id)
        REFERENCES messages(id) ON DELETE CASCADE,
    CONSTRAINT fk_ml_label FOREIGN KEY (label_id)
        REFERENCES labels(id) ON DELETE CASCADE
);

-- ============================================================================
-- RAW DATA
-- ============================================================================

CREATE TABLE IF NOT EXISTS message_bodies (
    message_id BIGINT PRIMARY KEY,
    body_text LONGTEXT,
    body_html LONGTEXT,
    CONSTRAINT fk_bodies_message FOREIGN KEY (message_id)
        REFERENCES messages(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS message_raw (
    message_id BIGINT PRIMARY KEY,

    raw_data LONGBLOB NOT NULL,
    raw_format VARCHAR(32) NOT NULL,

    compression VARCHAR(32) DEFAULT 'zlib',
    encryption_version INTEGER DEFAULT 0,
    CONSTRAINT fk_raw_message FOREIGN KEY (message_id)
        REFERENCES messages(id) ON DELETE CASCADE
);

-- ============================================================================
-- SYNC STATE
-- ============================================================================

CREATE TABLE IF NOT EXISTS sync_runs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_id BIGINT NOT NULL,

    started_at DATETIME(6) NOT NULL,
    completed_at DATETIME(6),
    status VARCHAR(32) DEFAULT 'running',

    messages_processed BIGINT DEFAULT 0,
    messages_added BIGINT DEFAULT 0,
    messages_updated BIGINT DEFAULT 0,
    errors_count BIGINT DEFAULT 0,

    error_message TEXT,
    cursor_before TEXT,
    cursor_after TEXT,

    KEY idx_sync_runs_source (source_id, started_at),
    CONSTRAINT fk_sync_runs_source FOREIGN KEY (source_id)
        REFERENCES sources(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sync_checkpoints (
    source_id BIGINT NOT NULL,
    checkpoint_type VARCHAR(64) NOT NULL,
    checkpoint_value TEXT NOT NULL,

    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),

    PRIMARY KEY (source_id, checkpoint_type),
    CONSTRAINT fk_sync_checkpoints_source FOREIGN KEY (source_id)
        REFERENCES sources(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS source_import_items (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_id BIGINT NOT NULL,
    provider VARCHAR(64) NOT NULL,
    provider_id VARCHAR(512) NOT NULL,
    name VARCHAR(512) NOT NULL,
    checksum VARCHAR(128),
    size BIGINT DEFAULT 0,
    modified_at DATETIME(6),
    imported_at DATETIME(6),
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    records_imported INTEGER DEFAULT 0,
    error_message TEXT,
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_source_import_items (source_id, provider, provider_id),
    KEY idx_source_import_items_source_provider (source_id, provider, status),
    CONSTRAINT fk_source_import_items_source FOREIGN KEY (source_id)
        REFERENCES sources(id) ON DELETE CASCADE
);

-- ============================================================================
-- COLLECTIONS
-- ============================================================================

CREATE TABLE IF NOT EXISTS collections (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_collections_name (name)
);

CREATE TABLE IF NOT EXISTS collection_sources (
    collection_id BIGINT NOT NULL,
    source_id BIGINT NOT NULL,
    PRIMARY KEY (collection_id, source_id),
    KEY idx_collection_sources_source_id (source_id),
    CONSTRAINT fk_cs_collection FOREIGN KEY (collection_id)
        REFERENCES collections(id) ON DELETE CASCADE,
    CONSTRAINT fk_cs_source FOREIGN KEY (source_id)
        REFERENCES sources(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS account_identities (
    source_id     BIGINT NOT NULL,
    address       VARCHAR(320) NOT NULL,
    source_signal VARCHAR(64) NOT NULL DEFAULT '',
    confirmed_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (source_id, address),
    KEY idx_account_identities_address (address),
    CONSTRAINT fk_account_identities_source FOREIGN KEY (source_id)
        REFERENCES sources(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS applied_migrations (
    name       VARCHAR(255) PRIMARY KEY,
    applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);
