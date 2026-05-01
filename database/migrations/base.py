from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from sqlalchemy.orm import Session

from app.services import MigrationJobService


@dataclass(slots=True)
class MigrationContext:
    job_id: str
    job_target_id: str
    tenant_id: str
    migration_name: str
    dry_run: bool
    batch_size: int


@dataclass(slots=True)
class MigrationResult:
    records_discovered: int = 0
    records_processed: int = 0
    records_succeeded: int = 0
    records_failed: int = 0
    batches_processed: int = 0


@dataclass(slots=True)
class ValidationResult:
    valid: bool
    message: str


class TenantMigration(Protocol):
    name: str

    def validate(self, tenant_db: Session, context: MigrationContext) -> ValidationResult:
        ...

    def up(
        self,
        tenant_db: Session,
        service: MigrationJobService,
        context: MigrationContext,
    ) -> MigrationResult:
        ...

    def down(
        self,
        tenant_db: Session,
        service: MigrationJobService,
        context: MigrationContext,
    ) -> MigrationResult:
        ...