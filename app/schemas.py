from datetime import datetime
from pydantic import BaseModel, Field


class CreateMigrationJobRequest(BaseModel):
    requested_by_user_id: str = Field(min_length=36, max_length=36)
    migration_name: str = Field(min_length=1, max_length=100)
    tenant_ids: list[str] | None = None
    dry_run: bool = False


class MigrationJobResponse(BaseModel):
    job_id: str
    requested_by_user_id: str
    migration_name: str
    status: str
    dry_run: bool
    total_targets: int
    queued_targets: int
    running_targets: int
    completed_targets: int
    failed_targets: int
    skipped_targets: int
    queued_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class MigrationJobTargetResponse(BaseModel):
    job_target_id: str
    job_id: str
    tenant_id: str
    status: str
    records_discovered: int
    records_processed: int
    records_succeeded: int
    records_failed: int
    batches_processed: int
    retry_count: int
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    error_message: str | None = None
    created_at: datetime
    updated_at: datetime


class MigrationJobEventResponse(BaseModel):
    id: int
    job_id: str
    job_target_id: str | None = None
    tenant_id: str | None = None
    event_type: str
    event_level: str
    message: str
    event_payload: dict | None = None
    created_at: datetime


class MigrationJobDetailResponse(BaseModel):
    job: MigrationJobResponse
    targets: list[MigrationJobTargetResponse]
    events: list[MigrationJobEventResponse]