from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy.orm import Session

from .models import MigrationJob, MigrationJobEvent, MigrationJobTarget
from .repositories import (
    MigrationCheckpointRepository,
    MigrationJobEventRepository,
    MigrationJobRepository,
    MigrationJobTargetRepository,
)
import json
from .clients import redis_client

@dataclass(slots=True)
class MigrationJobDetail:
    job: MigrationJob
    targets: list[MigrationJobTarget]
    events: list[MigrationJobEvent]


class MigrationJobService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.job_repo = MigrationJobRepository(db)
        self.target_repo = MigrationJobTargetRepository(db)
        self.event_repo = MigrationJobEventRepository(db)
        self.checkpoint_repo = MigrationCheckpointRepository(db)

    def create_job(
        self,
        *,
        requested_by_user_id: str,
        migration_name: str,
        tenant_ids: list[str],
        dry_run: bool = False,
    ) -> MigrationJob:
        if not tenant_ids:
            raise ValueError("At least one tenant_id is required")

        tenant_ids = list(dict.fromkeys(tenant_ids))
        job_id = str(uuid4())

        try:
            job = self.job_repo.create_job(
                job_id=job_id,
                requested_by_user_id=requested_by_user_id,
                migration_name=migration_name,
                dry_run=dry_run,
            )

            job_target_ids_by_tenant = {
                tenant_id: str(uuid4()) for tenant_id in tenant_ids
            }

            targets = self.target_repo.bulk_create_targets(
                job_id=job.job_id,
                tenant_ids=tenant_ids,
                job_target_ids_by_tenant=job_target_ids_by_tenant,
            )

            summary = self.target_repo.summarize_job_targets(job.job_id)
            self.job_repo.update_counters(job.job_id, summary)

            self.event_repo.append_event(
                job_id=job.job_id,
                event_type="job_created",
                message="Migration job created.",
                event_payload={
                    "migrationName": migration_name,
                    "dryRun": dry_run,
                    "tenantCount": len(tenant_ids),
                },
            )

            for target in targets:
                self.event_repo.append_event(
                    job_id=job.job_id,
                    job_target_id=target.job_target_id,
                    tenant_id=target.tenant_id,
                    event_type="target_queued",
                    message=f"Tenant {target.tenant_id} queued for migration.",
                )

                self.db.commit()
                self.db.refresh(job)

                payload = {
                    "jobId": job.job_id,
                    "jobTargetId": target.job_target_id,
                    "tenantId": target.tenant_id,
                    "migrationName": migration_name,
                    "dryRun": dry_run,
                }

                redis_client.rpush("migration:jobs", json.dumps(payload))
            return job
        except Exception:
            self.db.rollback()
            raise

    def get_job(self, job_id: str) -> MigrationJob | None:
        return self.job_repo.get_by_job_id(job_id)

    def list_jobs(self, limit: int = 100) -> list[MigrationJob]:
        return self.job_repo.list_jobs(limit=limit)

    def get_job_detail(self, job_id: str) -> MigrationJobDetail | None:
        job = self.job_repo.get_by_job_id(job_id)
        if job is None:
            return None

        targets = self.target_repo.list_by_job_id(job_id)
        events = self.event_repo.list_for_job(job_id)
        return MigrationJobDetail(job=job, targets=targets, events=events)

    def mark_target_running(self, *, job_id: str, job_target_id: str) -> None:
        try:
            self.target_repo.mark_running(job_target_id)

            target = self.target_repo.get_by_job_target_id(job_target_id)
            if target is None:
                raise ValueError(f"Migration job target not found: {job_target_id}")

            self.event_repo.append_event(
                job_id=job_id,
                job_target_id=job_target_id,
                tenant_id=target.tenant_id,
                event_type="target_started",
                message=f"Tenant {target.tenant_id} migration started.",
            )

            summary = self.target_repo.summarize_job_targets(job_id)
            self.job_repo.update_counters(job_id, summary)
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def heartbeat_target(
        self,
        *,
        job_id: str,
        job_target_id: str,
        records_discovered: int | None = None,
        records_processed: int | None = None,
        records_succeeded: int | None = None,
        records_failed: int | None = None,
        batches_processed: int | None = None,
    ) -> None:
        try:
            self.target_repo.heartbeat(
                job_target_id,
                records_discovered=records_discovered,
                records_processed=records_processed,
                records_succeeded=records_succeeded,
                records_failed=records_failed,
                batches_processed=batches_processed,
            )

            target = self.target_repo.get_by_job_target_id(job_target_id)
            if target is None:
                raise ValueError(f"Migration job target not found: {job_target_id}")

            self.event_repo.append_event(
                job_id=job_id,
                job_target_id=job_target_id,
                tenant_id=target.tenant_id,
                event_type="target_heartbeat",
                message=f"Tenant {target.tenant_id} migration progress updated.",
                event_payload={
                    "recordsDiscovered": records_discovered,
                    "recordsProcessed": records_processed,
                    "recordsSucceeded": records_succeeded,
                    "recordsFailed": records_failed,
                    "batchesProcessed": batches_processed,
                },
            )

            summary = self.target_repo.summarize_job_targets(job_id)
            self.job_repo.update_counters(job_id, summary)
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def complete_target(
        self,
        *,
        job_id: str,
        job_target_id: str,
        records_discovered: int = 0,
        records_processed: int = 0,
        records_succeeded: int = 0,
        records_failed: int = 0,
        batches_processed: int = 0,
    ) -> None:
        try:
            self.target_repo.mark_completed(
                job_target_id,
                records_discovered=records_discovered,
                records_processed=records_processed,
                records_succeeded=records_succeeded,
                records_failed=records_failed,
                batches_processed=batches_processed,
            )

            target = self.target_repo.get_by_job_target_id(job_target_id)
            if target is None:
                raise ValueError(f"Migration job target not found: {job_target_id}")

            self.event_repo.append_event(
                job_id=job_id,
                job_target_id=job_target_id,
                tenant_id=target.tenant_id,
                event_type="target_completed",
                message=f"Tenant {target.tenant_id} migration completed.",
                event_payload={
                    "recordsDiscovered": records_discovered,
                    "recordsProcessed": records_processed,
                    "recordsSucceeded": records_succeeded,
                    "recordsFailed": records_failed,
                    "batchesProcessed": batches_processed,
                },
            )

            summary = self.target_repo.summarize_job_targets(job_id)
            self.job_repo.update_counters(job_id, summary)
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def fail_target(
        self,
        *,
        job_id: str,
        job_target_id: str,
        error_message: str,
        increment_retry_count: bool = False,
    ) -> None:
        try:
            self.target_repo.mark_failed(
                job_target_id,
                error_message=error_message,
                increment_retry_count=increment_retry_count,
            )

            target = self.target_repo.get_by_job_target_id(job_target_id)
            if target is None:
                raise ValueError(f"Migration job target not found: {job_target_id}")

            self.event_repo.append_event(
                job_id=job_id,
                job_target_id=job_target_id,
                tenant_id=target.tenant_id,
                event_type="target_failed",
                event_level="error",
                message=f"Tenant {target.tenant_id} migration failed.",
                event_payload={
                    "errorMessage": error_message,
                    "retryCount": target.retry_count,
                },
            )

            summary = self.target_repo.summarize_job_targets(job_id)
            self.job_repo.update_counters(job_id, summary)
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def skip_target(
        self,
        *,
        job_id: str,
        job_target_id: str,
        reason: str | None = None,
    ) -> None:
        try:
            self.target_repo.mark_skipped(job_target_id, reason=reason)

            target = self.target_repo.get_by_job_target_id(job_target_id)
            if target is None:
                raise ValueError(f"Migration job target not found: {job_target_id}")

            self.event_repo.append_event(
                job_id=job_id,
                job_target_id=job_target_id,
                tenant_id=target.tenant_id,
                event_type="target_skipped",
                event_level="warning",
                message=f"Tenant {target.tenant_id} migration skipped.",
                event_payload={"reason": reason},
            )

            summary = self.target_repo.summarize_job_targets(job_id)
            self.job_repo.update_counters(job_id, summary)
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def save_checkpoint(
        self,
        *,
        job_target_id: str,
        tenant_id: str,
        checkpoint_key: str,
        checkpoint_value: str,
    ) -> None:
        try:
            self.checkpoint_repo.upsert_checkpoint(
                job_target_id=job_target_id,
                tenant_id=tenant_id,
                checkpoint_key=checkpoint_key,
                checkpoint_value=checkpoint_value,
            )
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise