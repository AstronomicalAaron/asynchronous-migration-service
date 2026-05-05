from __future__ import annotations

from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.migrations.base import MigrationContext, MigrationResult, ValidationResult
from app.services import MigrationJobService


class AddGlobalUserIdToUsers:
    name = "add_global_user_id_to_users"

    def validate(self, tenant_db: Session, context: MigrationContext) -> ValidationResult:
        users_table_exists = int(
            tenant_db.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                      AND table_name = 'users'
                    """
                )
            ).scalar_one()
        )

        if users_table_exists == 0:
            return ValidationResult(valid=False, message="users table does not exist")

        id_column_exists = int(
            tenant_db.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND table_name = 'users'
                      AND column_name = 'id'
                    """
                )
            ).scalar_one()
        )

        if id_column_exists == 0:
            return ValidationResult(valid=False, message="users.id column does not exist")

        return ValidationResult(valid=True, message="validation passed")

    def up(
            self,
            tenant_db: Session,
            service: MigrationJobService,
            context: MigrationContext,
    ) -> MigrationResult:
        self._ensure_column(tenant_db)

        records_discovered = int(
            tenant_db.execute(
                text("SELECT COUNT(*) FROM users WHERE global_user_id IS NULL")
            ).scalar_one()
        )

        result = MigrationResult(records_discovered=records_discovered)
        last_seen_id = 0

        while True:
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
                {"last_seen_id": last_seen_id, "limit": context.batch_size},
            ).mappings().all()

            if not rows:
                break

            for row in rows:
                user_id = int(row["id"])

                try:
                    if not context.dry_run:
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

                    result.records_processed += 1
                    result.records_succeeded += 1
                except Exception as exc:
                    result.records_processed += 1
                    result.records_failed += 1
                    service.save_checkpoint(
                        job_target_id=context.job_target_id,
                        tenant_id=context.tenant_id,
                        checkpoint_key=f"failed_user_{user_id}",
                        checkpoint_value=str(exc),
                    )

                last_seen_id = user_id

            if context.dry_run:
                tenant_db.rollback()
            else:
                tenant_db.commit()

            result.batches_processed += 1

            service.save_checkpoint(
                job_target_id=context.job_target_id,
                tenant_id=context.tenant_id,
                checkpoint_key="last_processed_user_id",
                checkpoint_value=str(last_seen_id),
            )

            service.heartbeat_target(
                job_id=context.job_id,
                job_target_id=context.job_target_id,
                records_discovered=result.records_discovered,
                records_processed=result.records_processed,
                records_succeeded=result.records_succeeded,
                records_failed=result.records_failed,
                batches_processed=result.batches_processed,
            )

            if len(rows) < context.batch_size:
                break

        return result

    def down(
            self,
            tenant_db: Session,
            service: MigrationJobService,
            context: MigrationContext,
    ) -> MigrationResult:
        del service

        column_exists = self._column_exists(tenant_db)
        if column_exists == 0:
            return MigrationResult()

        if not context.dry_run:
            tenant_db.execute(text("ALTER TABLE users DROP COLUMN global_user_id"))
            tenant_db.commit()
        else:
            tenant_db.rollback()

        return MigrationResult(records_discovered=1, records_processed=1, records_succeeded=1)

    def _ensure_column(self, tenant_db: Session) -> None:
        if self._column_exists(tenant_db) == 0:
            tenant_db.execute(text("ALTER TABLE users ADD COLUMN global_user_id CHAR(36) NULL"))
            tenant_db.commit()

    def _column_exists(self, tenant_db: Session) -> int:
        return int(
            tenant_db.execute(
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
        )
