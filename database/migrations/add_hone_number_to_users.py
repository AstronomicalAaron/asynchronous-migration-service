from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.migrations.base import MigrationContext, MigrationResult, ValidationResult
from app.services import MigrationJobService


class AddPhoneNumberToUsers:
    name = "add_phone_number_to_users"

    # --------------------------------------------------
    # VALIDATE
    # --------------------------------------------------
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

        return ValidationResult(valid=True, message="validation passed")

    # --------------------------------------------------
    # UP
    # --------------------------------------------------
    def up(
        self,
        tenant_db: Session,
        service: MigrationJobService,
        context: MigrationContext,
    ) -> MigrationResult:
        del service  # not needed for this migration

        column_exists = self._column_exists(tenant_db)

        if column_exists == 0:
            if not context.dry_run:
                tenant_db.execute(
                    text(
                        """
                        ALTER TABLE users
                        ADD COLUMN phone_number VARCHAR(20) NULL
                        """
                    )
                )
                tenant_db.commit()
            else:
                tenant_db.rollback()

            return MigrationResult(
                records_discovered=1,
                records_processed=1,
                records_succeeded=1,
                batches_processed=1,
            )

        # Already exists → no-op (idempotent)
        return MigrationResult(
            records_discovered=1,
            records_processed=1,
            records_succeeded=1,
            batches_processed=1,
        )

    # --------------------------------------------------
    # DOWN
    # --------------------------------------------------
    def down(
        self,
        tenant_db: Session,
        service: MigrationJobService,
        context: MigrationContext,
    ) -> MigrationResult:
        del service

        column_exists = self._column_exists(tenant_db)

        if column_exists == 1:
            if not context.dry_run:
                tenant_db.execute(
                    text(
                        """
                        ALTER TABLE users
                        DROP COLUMN phone_number
                        """
                    )
                )
                tenant_db.commit()
            else:
                tenant_db.rollback()

            return MigrationResult(
                records_discovered=1,
                records_processed=1,
                records_succeeded=1,
                batches_processed=1,
            )

        # Column already gone → no-op
        return MigrationResult(
            records_discovered=1,
            records_processed=1,
            records_succeeded=1,
            batches_processed=1,
        )

    # --------------------------------------------------
    # HELPERS
    # --------------------------------------------------
    def _column_exists(self, tenant_db: Session) -> int:
        return int(
            tenant_db.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND table_name = 'users'
                      AND column_name = 'phone_number'
                    """
                )
            ).scalar_one()
        )