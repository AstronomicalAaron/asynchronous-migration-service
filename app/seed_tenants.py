from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .clients import mongo_db

MYSQL_HOST = "mysql"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "rootpass"
MYSQL_ADMIN_DATABASE = "migration_control"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def mysql_admin_engine() -> Engine:
    return create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_ADMIN_DATABASE}",
        pool_pre_ping=True,
    )


@dataclass(slots=True)
class SeedTenant:
    tenant_id: str
    display_name: str
    users: list[dict[str, str]]


def build_seed_tenants() -> list[SeedTenant]:
    return [
        SeedTenant(
            tenant_id=str(uuid4()),
            display_name="Acme Corp",
            users=[
                {"email": "alice@example.com", "first_name": "Alice", "last_name": "Smith"},
                {"email": "bob@example.com", "first_name": "Bob", "last_name": "Jones"},
                {"email": "shared@example.com", "first_name": "Shared", "last_name": "User"},
            ],
        ),
        SeedTenant(
            tenant_id=str(uuid4()),
            display_name="Zenith Health",
            users=[
                {"email": "carol@example.com", "first_name": "Carol", "last_name": "Taylor"},
                {"email": "dave@example.com", "first_name": "Dave", "last_name": "Brown"},
                {"email": "shared@example.com", "first_name": "Shared", "last_name": "User"},
            ],
        ),
    ]


def create_tenant_database(engine: Engine, tenant: SeedTenant) -> None:
    db_name = tenant.tenant_id

    create_database_sql = text(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
    create_users_table_sql = text(
        f"""
        CREATE TABLE IF NOT EXISTS `{db_name}`.`users` (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(255) NOT NULL,
            first_name VARCHAR(255) NULL,
            last_name VARCHAR(255) NULL,
            created_at DATETIME NULL,
            updated_at DATETIME NULL,
            UNIQUE KEY uq_users_email (email)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )
    insert_user_sql = text(
        f"""
        INSERT INTO `{db_name}`.`users` (email, first_name, last_name, created_at, updated_at)
        VALUES (:email, :first_name, :last_name, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
            first_name = VALUES(first_name),
            last_name = VALUES(last_name),
            updated_at = NOW()
        """
    )

    with engine.begin() as conn:
        conn.execute(create_database_sql)
        conn.execute(create_users_table_sql)
        for user in tenant.users:
            conn.execute(insert_user_sql, user)


def seed_mongo_tenant_document(tenant: SeedTenant) -> None:
    tenants = mongo_db["tenants"]
    now = utc_now_iso()

    tenants.update_one(
        {"tenantId": tenant.tenant_id},
        {
            "$set": {
                "displayName": tenant.display_name,
                "database": {
                    "engine": "mysql",
                    "host": MYSQL_HOST,
                    "port": MYSQL_PORT,
                    "databaseName": tenant.tenant_id,
                },
                "isActive": True,
                "migrationEnabled": True,
                "updatedAt": now,
            },
            "$setOnInsert": {
                "tenantId": tenant.tenant_id,
                "createdAt": now,
            },
        },
        upsert=True,
    )


def ensure_mongo_indexes() -> None:
    mongo_db["tenants"].create_index("tenantId", unique=True)
    mongo_db["global_identities"].create_index("primaryEmail", unique=True)
    mongo_db["global_identities"].create_index("userId", unique=True)
    mongo_db["global_identities"].create_index(
        [("tenantLinks.tenantId", 1), ("tenantLinks.legacyUserId", 1)]
    )


def main() -> None:
    ensure_mongo_indexes()
    engine = mysql_admin_engine()
    tenants = build_seed_tenants()

    for tenant in tenants:
        create_tenant_database(engine, tenant)
        seed_mongo_tenant_document(tenant)
        print(f"Seeded tenant {tenant.display_name}: {tenant.tenant_id}")

    print("Done.")


if __name__ == "__main__":
    main()