from datetime import datetime, timezone

from sqlalchemy import (
    JSON,
    BigInteger,
    CHAR,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


job_status_enum = Enum(
    "queued",
    "running",
    "completed",
    "completed_with_errors",
    "failed",
    "cancelled",
    name="migration_job_status",
)

job_target_status_enum = Enum(
    "queued",
    "running",
    "completed",
    "failed",
    "skipped",
    name="migration_job_target_status",
)

event_level_enum = Enum(
    "info",
    "warning",
    "error",
    name="migration_event_level",
)


class MigrationJob(Base):
    __tablename__ = "migration_jobs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(CHAR(36), unique=True, index=True)
    requested_by_user_id: Mapped[str] = mapped_column(CHAR(36), index=True)
    migration_name: Mapped[str] = mapped_column(String(100), index=True)
    status: Mapped[str] = mapped_column(job_status_enum, default="queued", index=True)
    dry_run: Mapped[bool] = mapped_column(default=False)
    total_targets: Mapped[int] = mapped_column(Integer, default=0)
    queued_targets: Mapped[int] = mapped_column(Integer, default=0)
    running_targets: Mapped[int] = mapped_column(Integer, default=0)
    completed_targets: Mapped[int] = mapped_column(Integer, default=0)
    failed_targets: Mapped[int] = mapped_column(Integer, default=0)
    skipped_targets: Mapped[int] = mapped_column(Integer, default=0)
    queued_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now, onupdate=utc_now)


class MigrationJobTarget(Base):
    __tablename__ = "migration_job_targets"
    __table_args__ = (
        UniqueConstraint("job_id", "tenant_id", name="uq_migration_job_targets_job_id_tenant_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_target_id: Mapped[str] = mapped_column(CHAR(36), unique=True, index=True)
    job_id: Mapped[str] = mapped_column(
        CHAR(36),
        ForeignKey("migration_jobs.job_id", ondelete="CASCADE", onupdate="CASCADE"),
        index=True,
    )
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(job_target_status_enum, default="queued", index=True)
    records_discovered: Mapped[int] = mapped_column(Integer, default=0)
    records_processed: Mapped[int] = mapped_column(Integer, default=0)
    records_succeeded: Mapped[int] = mapped_column(Integer, default=0)
    records_failed: Mapped[int] = mapped_column(Integer, default=0)
    batches_processed: Mapped[int] = mapped_column(Integer, default=0)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now, onupdate=utc_now)


class MigrationJobEvent(Base):
    __tablename__ = "migration_job_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(
        CHAR(36),
        ForeignKey("migration_jobs.job_id", ondelete="CASCADE", onupdate="CASCADE"),
        index=True,
    )
    job_target_id: Mapped[str | None] = mapped_column(
        CHAR(36),
        ForeignKey("migration_job_targets.job_target_id", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=True,
        index=True,
    )
    tenant_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    event_type: Mapped[str] = mapped_column(String(100), index=True)
    event_level: Mapped[str] = mapped_column(event_level_enum, default="info")
    message: Mapped[str] = mapped_column(Text)
    event_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now, index=True)


class MigrationCheckpoint(Base):
    __tablename__ = "migration_checkpoints"
    __table_args__ = (
        UniqueConstraint("job_target_id", "checkpoint_key", name="uq_migration_checkpoints_target_key"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_target_id: Mapped[str] = mapped_column(
        CHAR(36),
        ForeignKey("migration_job_targets.job_target_id", ondelete="CASCADE", onupdate="CASCADE"),
        index=True,
    )
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    checkpoint_key: Mapped[str] = mapped_column(String(255))
    checkpoint_value: Mapped[str] = mapped_column(String(1000))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now, onupdate=utc_now)