GRANT ALL PRIVILEGES ON *.* TO 'migration_user'@'%';
FLUSH PRIVILEGES;

CREATE DATABASE IF NOT EXISTS migration_control;
USE migration_control;

CREATE TABLE IF NOT EXISTS migration_jobs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_id CHAR(36) NOT NULL,
    requested_by_user_id CHAR(36) NOT NULL,
    migration_name VARCHAR(100) NOT NULL,
    status ENUM(
        'queued',
        'running',
        'completed',
        'completed_with_errors',
        'failed',
        'cancelled'
    ) NOT NULL DEFAULT 'queued',
    dry_run TINYINT(1) NOT NULL DEFAULT 0,
    total_targets INT NOT NULL DEFAULT 0,
    queued_targets INT NOT NULL DEFAULT 0,
    running_targets INT NOT NULL DEFAULT 0,
    completed_targets INT NOT NULL DEFAULT 0,
    failed_targets INT NOT NULL DEFAULT 0,
    skipped_targets INT NOT NULL DEFAULT 0,
    queued_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at DATETIME NULL,
    finished_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_migration_jobs_job_id (job_id),
    KEY idx_migration_jobs_requested_by_user_id (requested_by_user_id),
    KEY idx_migration_jobs_status (status),
    KEY idx_migration_jobs_migration_name (migration_name),
    KEY idx_migration_jobs_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS migration_job_targets (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_target_id CHAR(36) NOT NULL,
    job_id CHAR(36) NOT NULL,
    tenant_id VARCHAR(128) NOT NULL,
    status ENUM(
        'queued',
        'running',
        'completed',
        'failed',
        'skipped'
    ) NOT NULL DEFAULT 'queued',
    records_discovered INT NOT NULL DEFAULT 0,
    records_processed INT NOT NULL DEFAULT 0,
    records_succeeded INT NOT NULL DEFAULT 0,
    records_failed INT NOT NULL DEFAULT 0,
    batches_processed INT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL DEFAULT 0,
    started_at DATETIME NULL,
    finished_at DATETIME NULL,
    last_heartbeat_at DATETIME NULL,
    error_message TEXT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_migration_job_targets_job_target_id (job_target_id),
    UNIQUE KEY uq_migration_job_targets_job_id_tenant_id (job_id, tenant_id),
    KEY idx_migration_job_targets_job_id (job_id),
    KEY idx_migration_job_targets_tenant_id (tenant_id),
    KEY idx_migration_job_targets_status (status),
    KEY idx_migration_job_targets_started_at (started_at),
    CONSTRAINT fk_migration_job_targets_job_id
        FOREIGN KEY (job_id) REFERENCES migration_jobs(job_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS migration_job_events (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_id CHAR(36) NOT NULL,
    job_target_id CHAR(36) NULL,
    tenant_id VARCHAR(128) NULL,
    event_type VARCHAR(100) NOT NULL,
    event_level ENUM('info', 'warning', 'error') NOT NULL DEFAULT 'info',
    message TEXT NOT NULL,
    event_payload JSON NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_migration_job_events_job_id (job_id),
    KEY idx_migration_job_events_job_target_id (job_target_id),
    KEY idx_migration_job_events_tenant_id (tenant_id),
    KEY idx_migration_job_events_event_type (event_type),
    KEY idx_migration_job_events_created_at (created_at),
    CONSTRAINT fk_migration_job_events_job_id
        FOREIGN KEY (job_id) REFERENCES migration_jobs(job_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT fk_migration_job_events_job_target_id
        FOREIGN KEY (job_target_id) REFERENCES migration_job_targets(job_target_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS migration_checkpoints (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_target_id CHAR(36) NOT NULL,
    tenant_id VARCHAR(128) NOT NULL,
    checkpoint_key VARCHAR(255) NOT NULL,
    checkpoint_value VARCHAR(1000) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_migration_checkpoints_target_key (job_target_id, checkpoint_key),
    KEY idx_migration_checkpoints_tenant_id (tenant_id),
    CONSTRAINT fk_migration_checkpoints_job_target_id
        FOREIGN KEY (job_target_id) REFERENCES migration_job_targets(job_target_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;