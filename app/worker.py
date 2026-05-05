from __future__ import annotations

import json
import logging
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from app.clients import redis_client
from app.db import SessionLocal
from app.migrations.base import MigrationContext
from app.migrations.registry import registry
from app.services import MigrationJobService
from app.tenant_db import tenant_session

QUEUE_NAME = "migration:jobs"
POLL_TIMEOUT_SECONDS = 5
BATCH_SIZE = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [worker] %(message)s",
)
logger = logging.getLogger(__name__)

_should_stop = False


def _handle_shutdown(signum: int, frame: Any) -> None:
    del frame
    global _should_stop
    logger.info("Received signal %s. Worker will stop after current task.", signum)
    _should_stop = True


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


@dataclass(slots=True)
class MigrationTask:
    job_id: str
    job_target_id: str
    tenant_id: str
    migration_name: str
    dry_run: bool

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> MigrationTask:
        required_keys = [
            "jobId",
            "jobTargetId",
            "tenantId",
            "migrationName",
            "dryRun",
        ]

        missing = [key for key in required_keys if key not in payload]
        if missing:
            raise ValueError(f"Missing required task keys: {', '.join(missing)}")

        return cls(
            job_id=str(payload["jobId"]),
            job_target_id=str(payload["jobTargetId"]),
            tenant_id=str(payload["tenantId"]),
            migration_name=str(payload["migrationName"]),
            dry_run=bool(payload["dryRun"]),
        )


@dataclass(slots=True)
class MigrationExecutionResult:
    records_discovered: int
    records_processed: int
    records_succeeded: int
    records_failed: int
    batches_processed: int


def run() -> None:
    logger.info("Worker started. Listening on Redis queue '%s'.", QUEUE_NAME)

    while not _should_stop:
        try:
            item = redis_client.blpop(QUEUE_NAME, timeout=POLL_TIMEOUT_SECONDS)
        except Exception:
            logger.exception("Redis BLPOP failed. Sleeping before retry.")
            time.sleep(2)
            continue

        if item is None:
            continue

        _, payload_json = item

        try:
            payload = json.loads(payload_json)
            task = MigrationTask.from_payload(payload)
        except Exception:
            logger.exception("Failed to decode task payload: %s", payload_json)
            continue

        process_task(task)

    logger.info("Worker stopped.")


def process_task(task: MigrationTask) -> None:
    logger.info(
        "Processing task job_id=%s job_target_id=%s tenant_id=%s migration_name=%s dry_run=%s",
        task.job_id,
        task.job_target_id,
        task.tenant_id,
        task.migration_name,
        task.dry_run,
    )

    db: Session = SessionLocal()
    service = MigrationJobService(db)

    try:
        target = service.target_repo.get_by_job_target_id(task.job_target_id)
        if target is None:
            logger.warning(
                "Skipping stale task. No migration_job_target found for job_target_id=%s",
                task.job_target_id,
            )
            return

        service.mark_target_running(
            job_id=task.job_id,
            job_target_id=task.job_target_id,
        )

        result = execute_migration(task, service)

        service.complete_target(
            job_id=task.job_id,
            job_target_id=task.job_target_id,
            records_discovered=result.records_discovered,
            records_processed=result.records_processed,
            records_succeeded=result.records_succeeded,
            records_failed=result.records_failed,
            batches_processed=result.batches_processed,
        )

        logger.info(
            "Completed task job_id=%s job_target_id=%s tenant_id=%s migration_name=%s",
            task.job_id,
            task.job_target_id,
            task.tenant_id,
            task.migration_name,
        )

    except Exception as exc:
        logger.exception(
            "Task failed job_id=%s job_target_id=%s tenant_id=%s migration_name=%s",
            task.job_id,
            task.job_target_id,
            task.tenant_id,
            task.migration_name,
        )

        try:
            service.fail_target(
                job_id=task.job_id,
                job_target_id=task.job_target_id,
                error_message=str(exc),
                increment_retry_count=False,
            )
        except Exception:
            logger.exception(
                "Failed to persist target failure state for job_target_id=%s",
                task.job_target_id,
            )
    finally:
        db.close()


def execute_migration(
        task: MigrationTask,
        service: MigrationJobService,
) -> MigrationExecutionResult:
    migration = registry.get(task.migration_name)

    context = MigrationContext(
        job_id=task.job_id,
        job_target_id=task.job_target_id,
        tenant_id=task.tenant_id,
        migration_name=task.migration_name,
        dry_run=task.dry_run,
        batch_size=BATCH_SIZE,
    )

    with tenant_session(task.tenant_id) as tenant_db:
        validation = migration.validate(tenant_db, context)
        if not validation.valid:
            raise ValueError(f"Migration validation failed: {validation.message}")

        result = migration.up(
            tenant_db=tenant_db,
            service=service,
            context=context,
        )

    return MigrationExecutionResult(
        records_discovered=result.records_discovered,
        records_processed=result.records_processed,
        records_succeeded=result.records_succeeded,
        records_failed=result.records_failed,
        batches_processed=result.batches_processed,
    )


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Worker interrupted by keyboard input.")
        sys.exit(0)
