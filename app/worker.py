from __future__ import annotations

import json
import logging
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from .clients import redis_client
from .db import SessionLocal
from .services import MigrationJobService
from .tenant_db import tenant_session

QUEUE_NAME = "migration:jobs"
POLL_TIMEOUT_SECONDS = 5
HEARTBEAT_INTERVAL_SECONDS = 10
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
    def from_payload(cls, payload: dict[str, Any]) -> "MigrationTask":
        required_keys = ["jobId", "jobTargetId", "tenantId", "migrationName", "dryRun"]
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
        # Defensive skip for stale Redis messages after local dev resets.
        target = service.target_repo.get_by_job_target_id(task.job_target_id)
        if target is None:
            logger.warning(
                "Skipping stale task. No migration_job_target found for job_target_id=%s",
                task.job_target_id,
            )
            return

        service.mark_target_running(job_id=task.job_id, job_target_id=task.job_target_id)
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
            "Completed task job_id=%s job_target_id=%s tenant_id=%s",
            task.job_id,
            task.job_target_id,
            task.tenant_id,
        )
    except Exception as exc:
        logger.exception(
            "Task failed job_id=%s job_target_id=%s tenant_id=%s",
            task.job_id,
            task.job_target_id,
            task.tenant_id,
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

def execute_migration(task: MigrationTask, service: MigrationJobService) -> MigrationExecutionResult:
    if task.migration_name == "add_global_user_id_to_users":
        return add_global_user_id_to_users(task, service)

    raise ValueError(f"Unsupported migration type: {task.migration_name}")

def add_global_user_id_to_users(
    task: MigrationTask,
    service: MigrationJobService,
) -> MigrationExecutionResult:
    """
    In-place tenant DB migration.

    Mutates the selected tenant database by:
    - adding users.global_user_id if missing
    - backfilling UUIDv4 values for rows where global_user_id is NULL
    """
    records_discovered = 0
    records_processed = 0
    records_succeeded = 0
    records_failed = 0
    batches_processed = 0
    last_seen_id = 0
    last_heartbeat_at = time.monotonic()

    with tenant_session(task.tenant_id) as tenant_db:
        ensure_global_user_id_column(tenant_db)

        count_result = tenant_db.execute(
            text("SELECT COUNT(*) FROM users WHERE global_user_id IS NULL")
        )
        records_discovered = int(count_result.scalar_one())

        while not _should_stop:
            rows = tenant_db.execute(
                text(
                    """
                    SELECT id
                    FROM users
                    WHERE id > :last_seen_id
                      AND global_user_id IS NULL
                    ORDER BY id ASC
                    LIMIT :limit
                    """
                ),
                {"last_seen_id": last_seen_id, "limit": BATCH_SIZE},
            ).mappings().all()

            if not rows:
                break

            for row in rows:
                user_id = int(row["id"])

                try:
                    if not task.dry_run:
                        tenant_db.execute(
                            text(
                                """
                                UPDATE users
                                SET global_user_id = :global_user_id,
                                    updated_at = NOW()
                                WHERE id = :user_id
                                  AND global_user_id IS NULL
                                """
                            ),
                            {
                                "global_user_id": str(uuid4()),
                                "user_id": user_id,
                            },
                        )

                    records_processed += 1
                    records_succeeded += 1
                except Exception as exc:
                    records_processed += 1
                    records_failed += 1
                    service.save_checkpoint(
                        job_target_id=task.job_target_id,
                        tenant_id=task.tenant_id,
                        checkpoint_key=f"failed_user_{user_id}",
                        checkpoint_value=str(exc),
                    )

                last_seen_id = user_id

            if not task.dry_run:
                tenant_db.commit()
            else:
                tenant_db.rollback()

            batches_processed += 1
            service.save_checkpoint(
                job_target_id=task.job_target_id,
                tenant_id=task.tenant_id,
                checkpoint_key="last_processed_user_id",
                checkpoint_value=str(last_seen_id),
            )

            now_monotonic = time.monotonic()
            if now_monotonic - last_heartbeat_at >= HEARTBEAT_INTERVAL_SECONDS or len(rows) < BATCH_SIZE:
                service.heartbeat_target(
                    job_id=task.job_id,
                    job_target_id=task.job_target_id,
                    records_discovered=records_discovered,
                    records_processed=records_processed,
                    records_succeeded=records_succeeded,
                    records_failed=records_failed,
                    batches_processed=batches_processed,
                )
                last_heartbeat_at = now_monotonic

            if len(rows) < BATCH_SIZE:
                break

        if _should_stop:
            raise RuntimeError("Worker interrupted during migration")

        return MigrationExecutionResult(
            records_discovered=records_discovered,
            records_processed=records_processed,
            records_succeeded=records_succeeded,
            records_failed=records_failed,
            batches_processed=batches_processed,
        )

def ensure_global_user_id_column(tenant_db: Session) -> None:
    column_exists = tenant_db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'users'
              AND column_name = 'global_user_id'
            """
        )
    ).scalar_one()

    if int(column_exists) == 0:
        tenant_db.execute(text("ALTER TABLE users ADD COLUMN global_user_id CHAR(36) NULL"))
        tenant_db.commit()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Worker interrupted by keyboard input.")
        sys.exit(0)