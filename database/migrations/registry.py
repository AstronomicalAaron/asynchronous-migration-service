from __future__ import annotations

from app.migrations.base import TenantMigration


class MigrationRegistry:
    def __init__(self) -> None:
        self._migrations: dict[str, TenantMigration] = {}

    def register(self, migration: TenantMigration) -> None:
        if migration.name in self._migrations:
            raise ValueError(f"Migration already registered: {migration.name}")

        self._migrations[migration.name] = migration

    def get(self, migration_name: str) -> TenantMigration:
        try:
            return self._migrations[migration_name]
        except KeyError as exc:
            available = ", ".join(sorted(self._migrations.keys()))
            raise ValueError(
                f"Unknown migration: {migration_name}. Available migrations: {available}"
            ) from exc

    def list_names(self) -> list[str]:
        return sorted(self._migrations.keys())


registry = MigrationRegistry()
registry.register(AddGlobalUserIdToUsers())