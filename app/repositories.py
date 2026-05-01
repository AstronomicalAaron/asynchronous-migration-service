from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import Session

from .models import (
    MigrationCheckpoint,
    MigrationJob,
    MigrationJobEvent,
    MigrationJobTarget,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class JobCounterSummary:
    total_targets: int
    queued_targets: int
    running_targets: int
    completed_targets: int
    failed_targets: int
    skipped_targets: int


class MigrationJobRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def create_job(
        self,
        *,
        job_id: str,
        requested_by_user_id: str,
        migration_name: str,
        dry_run: bool = False,
        status: str = "queued",
    ) -> MigrationJob:
        job = MigrationJob(
            job_id=job_id,
            requested_by_user_id=requested_by_user_id,
            migration_name=migration_name,
            status=status,
            dry_run=dry_run,
            queued_at=utc_now(),
        )
        self.db.add(job)
        self.db.flush()
        self.db.refresh(job)
        return job

    def get_by_job_id(self, job_id: str) -> MigrationJob | None:
        stmt = select(MigrationJob).where(MigrationJob.job_id == job_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def list_jobs(self, limit: int = 100) -> list[MigrationJob]:
        stmt = (
            select(MigrationJob)
            .order_by(MigrationJob.created_at.desc())
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())

    def mark_started(self, job_id: str) -> None:
        job = self.get_by_job_id(job_id)
        if job is None:
            raise ValueError(f"Migration job not found: {job_id}")

        if job.started_at is None:
            job.started_at = utc_now()

        job.status = "running"
        job.updated_at = utc_now()
        self.db.flush()

    def mark_finished(
        self,
        job_id: str,
        *,
        status: str,
    ) -> None:
        job = self.get_by_job_id(job_id)
        if job is None:
            raise ValueError(f"Migration job not found: {job_id}")

        if job.started_at is None and status in {"running", "completed", "completed_with_errors", "failed"}:
            job.started_at = utc_now()

        job.status = status
        job.finished_at = utc_now()
        job.updated_at = utc_now()
        self.db.flush()

    def update_counters(self, job_id: str, summary: JobCounterSummary) -> None:
        job = self.get_by_job_id(job_id)
        if job is None:
            raise ValueError(f"Migration job not found: {job_id}")

        job.total_targets = summary.total_targets
        job.queued_targets = summary.queued_targets
        job.running_targets = summary.running_targets
        job.completed_targets = summary.completed_targets
        job.failed_targets = summary.failed_targets
        job.skipped_targets = summary.skipped_targets
        job.updated_at = utc_now()

        if summary.running_targets > 0 and job.started_at is None:
            job.started_at = utc_now()

        if 0 < summary.total_targets == (
            summary.completed_targets + summary.failed_targets + summary.skipped_targets
        ):
            job.finished_at = utc_now()

            if summary.failed_targets > 0 and summary.completed_targets > 0:
                job.status = "completed_with_errors"
            elif summary.failed_targets > 0 and summary.completed_targets == 0:
                job.status = "failed"
            else:
                job.status = "completed"
        elif summary.running_targets > 0:
            job.status = "running"
        else:
            job.status = "queued"

        self.db.flush()


class MigrationJobTargetRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def bulk_create_targets(
        self,
        *,
        job_id: str,
        tenant_ids: list[str],
        job_target_ids_by_tenant: dict[str, str],
    ) -> list[MigrationJobTarget]:
        now = utc_now()
        targets: list[MigrationJobTarget] = []

        for tenant_id in tenant_ids:
            job_target_id = job_target_ids_by_tenant[tenant_id]
            target = MigrationJobTarget(
                job_target_id=job_target_id,
                job_id=job_id,
                tenant_id=tenant_id,
                status="queued",
                created_at=now,
                updated_at=now,
            )
            self.db.add(target)
            targets.append(target)

        self.db.flush()
        return targets

    def get_by_job_target_id(self, job_target_id: str) -> MigrationJobTarget | None:
        stmt = select(MigrationJobTarget).where(
            MigrationJobTarget.job_target_id == job_target_id
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def get_by_job_and_tenant(
        self,
        *,
        job_id: str,
        tenant_id: str,
    ) -> MigrationJobTarget | None:
        stmt = select(MigrationJobTarget).where(
            MigrationJobTarget.job_id == job_id,
            MigrationJobTarget.tenant_id == tenant_id,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def list_by_job_id(self, job_id: str) -> list[MigrationJobTarget]:
        stmt = (
            select(MigrationJobTarget)
            .where(MigrationJobTarget.job_id == job_id)
            .order_by(MigrationJobTarget.created_at.asc())
        )
        return list(self.db.execute(stmt).scalars().all())

    def mark_running(self, job_target_id: str) -> None:
        target = self.get_by_job_target_id(job_target_id)
        if target is None:
            raise ValueError(f"Migration job target not found: {job_target_id}")

        now = utc_now()
        target.status = "running"
        target.started_at = target.started_at or now
        target.last_heartbeat_at = now
        target.updated_at = now
        self.db.flush()

    def mark_completed(
        self,
        job_target_id: str,
        *,
        records_discovered: int | None = None,
        records_processed: int | None = None,
        records_succeeded: int | None = None,
        records_failed: int | None = None,
        batches_processed: int | None = None,
    ) -> None:
        target = self.get_by_job_target_id(job_target_id)
        if target is None:
            raise ValueError(f"Migration job target not found: {job_target_id}")

        now = utc_now()

        if records_discovered is not None:
            target.records_discovered = records_discovered
        if records_processed is not None:
            target.records_processed = records_processed
        if records_succeeded is not None:
            target.records_succeeded = records_succeeded
        if records_failed is not None:
            target.records_failed = records_failed
        if batches_processed is not None:
            target.batches_processed = batches_processed

        target.status = "completed"
        target.finished_at = now
        target.last_heartbeat_at = now
        target.error_message = None
        target.updated_at = now
        self.db.flush()

    def mark_failed(
        self,
        job_target_id: str,
        *,
        error_message: str,
        increment_retry_count: bool = False,
    ) -> None:
        target = self.get_by_job_target_id(job_target_id)
        if target is None:
            raise ValueError(f"Migration job target not found: {job_target_id}")

        now = utc_now()
        target.status = "failed"
        target.finished_at = now
        target.last_heartbeat_at = now
        target.error_message = error_message
        target.updated_at = now

        if increment_retry_count:
            target.retry_count += 1

        self.db.flush()

    def mark_skipped(
        self,
        job_target_id: str,
        *,
        reason: str | None = None,
    ) -> None:
        target = self.get_by_job_target_id(job_target_id)
        if target is None:
            raise ValueError(f"Migration job target not found: {job_target_id}")

        now = utc_now()
        target.status = "skipped"
        target.finished_at = now
        target.last_heartbeat_at = now
        target.error_message = reason
        target.updated_at = now
        self.db.flush()

    def heartbeat(
        self,
        job_target_id: str,
        *,
        records_discovered: int | None = None,
        records_processed: int | None = None,
        records_succeeded: int | None = None,
        records_failed: int | None = None,
        batches_processed: int | None = None,
    ) -> None:
        target = self.get_by_job_target_id(job_target_id)
        if target is None:
            raise ValueError(f"Migration job target not found: {job_target_id}")

        now = utc_now()
        target.last_heartbeat_at = now
        target.updated_at = now

        if records_discovered is not None:
            target.records_discovered = records_discovered
        if records_processed is not None:
            target.records_processed = records_processed
        if records_succeeded is not None:
            target.records_succeeded = records_succeeded
        if records_failed is not None:
            target.records_failed = records_failed
        if batches_processed is not None:
            target.batches_processed = batches_processed

        self.db.flush()

    def summarize_job_targets(self, job_id: str) -> JobCounterSummary:
        stmt = (
            select(
                func.count(MigrationJobTarget.id),
                func.sum(MigrationJobTarget.status == "queued"),
                func.sum(MigrationJobTarget.status == "running"),
                func.sum(MigrationJobTarget.status == "completed"),
                func.sum(MigrationJobTarget.status == "failed"),
                func.sum(MigrationJobTarget.status == "skipped"),
            )
            .where(MigrationJobTarget.job_id == job_id)
        )

        row = self.db.execute(stmt).one()

        return JobCounterSummary(
            total_targets=int(row[0] or 0),
            queued_targets=int(row[1] or 0),
            running_targets=int(row[2] or 0),
            completed_targets=int(row[3] or 0),
            failed_targets=int(row[4] or 0),
            skipped_targets=int(row[5] or 0),
        )


class MigrationJobEventRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def append_event(
        self,
        *,
        job_id: str,
        event_type: str,
        message: str,
        event_level: str = "info",
        job_target_id: str | None = None,
        tenant_id: str | None = None,
        event_payload: dict[str, Any] | None = None,
    ) -> MigrationJobEvent:
        event = MigrationJobEvent(
            job_id=job_id,
            job_target_id=job_target_id,
            tenant_id=tenant_id,
            event_type=event_type,
            event_level=event_level,
            message=message,
            event_payload=event_payload,
            created_at=utc_now(),
        )
        self.db.add(event)
        self.db.flush()
        self.db.refresh(event)
        return event

    def list_for_job(self, job_id: str, limit: int = 500) -> list[MigrationJobEvent]:
        stmt = (
            select(MigrationJobEvent)
            .where(MigrationJobEvent.job_id == job_id)
            .order_by(MigrationJobEvent.created_at.asc(), MigrationJobEvent.id.asc())
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())


class MigrationCheckpointRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def upsert_checkpoint(
        self,
        *,
        job_target_id: str,
        tenant_id: str,
        checkpoint_key: str,
        checkpoint_value: str,
    ) -> None:
        stmt = mysql_insert(MigrationCheckpoint).values(
            job_target_id=job_target_id,
            tenant_id=tenant_id,
            checkpoint_key=checkpoint_key,
            checkpoint_value=checkpoint_value,
            created_at=utc_now(),
            updated_at=utc_now(),
        )

        stmt = stmt.on_duplicate_key_update(
            checkpoint_value=stmt.inserted.checkpoint_value,
            tenant_id=stmt.inserted.tenant_id,
            updated_at=utc_now(),
        )

        self.db.execute(stmt)
        self.db.flush()

    def get_checkpoint(
        self,
        *,
        job_target_id: str,
        checkpoint_key: str,
    ) -> MigrationCheckpoint | None:
        stmt = select(MigrationCheckpoint).where(
            MigrationCheckpoint.job_target_id == job_target_id,
            MigrationCheckpoint.checkpoint_key == checkpoint_key,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def list_checkpoints_for_target(
        self,
        *,
        job_target_id: str,
    ) -> list[MigrationCheckpoint]:
        stmt = (
            select(MigrationCheckpoint)
            .where(MigrationCheckpoint.job_target_id == job_target_id)
            .order_by(MigrationCheckpoint.checkpoint_key.asc())
        )
        return list(self.db.execute(stmt).scalars().all())