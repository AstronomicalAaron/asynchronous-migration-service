from __future__ import annotations
from .clients import mongo_db

TENANTS_COLLECTION = "tenants"

def list_migration_enabled_tenant_ids() -> list[str]:
    docs = mongo_db[TENANTS_COLLECTION].find(
        {
            "isActive": True,
            "migrationEnabled": True,
        },
        {
            "tenantId": 1,
            "_id": 0,
        },
    )

    return sorted(doc["tenantId"] for doc in docs)


def validate_requested_tenant_ids(tenant_ids: list[str]) -> list[str]:
    unique_tenant_ids = sorted(set(tenant_ids))

    docs = mongo_db[TENANTS_COLLECTION].find(
        {
            "tenantId": {"$in": unique_tenant_ids},
            "isActive": True,
            "migrationEnabled": True,
        },
        {
            "tenantId": 1,
            "_id": 0,
        },
    )

    found = sorted(doc["tenantId"] for doc in docs)
    missing = sorted(set(unique_tenant_ids) - set(found))

    if missing:
        raise ValueError(
            "Unknown, inactive, or migration-disabled tenant_ids: " + ", ".join(missing)
        )

    return found


def resolve_target_tenant_ids(requested_tenant_ids: list[str] | None) -> list[str]:
    if requested_tenant_ids:
        return validate_requested_tenant_ids(requested_tenant_ids)

    tenant_ids = list_migration_enabled_tenant_ids()

    if not tenant_ids:
        raise ValueError("No active migration-enabled tenants found in MongoDB")

    return tenant_ids