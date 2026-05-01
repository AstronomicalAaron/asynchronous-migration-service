from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import MigrationJob, MigrationJobEvent, MigrationJobTarget
from ..schemas import (
    CreateMigrationJobRequest,
    MigrationJobDetailResponse,
    MigrationJobEventResponse,
    MigrationJobResponse,
    MigrationJobTargetResponse,
)
from ..services import MigrationJobDetail, MigrationJobService
from ..tenant_registry import resolve_target_tenant_ids

router = APIRouter(prefix="/jobs", tags=["jobs"])

def to_job_response(job: MigrationJob) -> MigrationJobResponse:
    return MigrationJobResponse(
        job_id=job.job_id,
        requested_by_user_id=job.requested_by_user_id,
        migration_name=job.migration_name,
        status=job.status,
        dry_run=job.dry_run,
        total_targets=job.total_targets,
        queued_targets=job.queued_targets,
        running_targets=job.running_targets,
        completed_targets=job.completed_targets,
        failed_targets=job.failed_targets,
        skipped_targets=job.skipped_targets,
        queued_at=job.queued_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )

def to_target_response(target: MigrationJobTarget) -> MigrationJobTargetResponse:
    return MigrationJobTargetResponse(
        job_target_id=target.job_target_id,
        job_id=target.job_id,
        tenant_id=target.tenant_id,
        status=target.status,
        records_discovered=target.records_discovered,
        records_processed=target.records_processed,
        records_succeeded=target.records_succeeded,
        records_failed=target.records_failed,
        batches_processed=target.batches_processed,
        retry_count=target.retry_count,
        started_at=target.started_at,
        finished_at=target.finished_at,
        last_heartbeat_at=target.last_heartbeat_at,
        error_message=target.error_message,
        created_at=target.created_at,
        updated_at=target.updated_at,
    )

def to_event_response(event: MigrationJobEvent) -> MigrationJobEventResponse:
    return MigrationJobEventResponse(
        id=event.id,
        job_id=event.job_id,
        job_target_id=event.job_target_id,
        tenant_id=event.tenant_id,
        event_type=event.event_type,
        event_level=event.event_level,
        message=event.message,
        event_payload=event.event_payload,
        created_at=event.created_at,
    )


def to_job_detail_response(detail: MigrationJobDetail) -> MigrationJobDetailResponse:
    return MigrationJobDetailResponse(
        job=to_job_response(detail.job),
        targets=[to_target_response(target) for target in detail.targets],
        events=[to_event_response(event) for event in detail.events],
    )

@router.post("", response_model=MigrationJobResponse, status_code=status.HTTP_201_CREATED)
def create_job(
    request: CreateMigrationJobRequest,
    db: Session = Depends(get_db),
) -> MigrationJobResponse:
    try:
        tenant_ids = resolve_target_tenant_ids(request.tenant_ids)

        service = MigrationJobService(db)
        job = service.create_job(
            requested_by_user_id=request.requested_by_user_id,
            migration_name=request.migration_name,
            tenant_ids=tenant_ids,
            dry_run=request.dry_run,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return to_job_response(job)


@router.get("", response_model=list[MigrationJobResponse])
def list_jobs(
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
) -> list[MigrationJobResponse]:
    service = MigrationJobService(db)
    jobs = service.list_jobs(limit=limit)
    return [to_job_response(job) for job in jobs]


@router.get("/{job_id}", response_model=MigrationJobDetailResponse)
def get_job(
    job_id: str,
    db: Session = Depends(get_db),
) -> MigrationJobDetailResponse:
    service = MigrationJobService(db)
    detail = service.get_job_detail(job_id)

    if detail is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Migration job not found")

    return to_job_detail_response(detail)