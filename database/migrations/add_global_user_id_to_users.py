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
