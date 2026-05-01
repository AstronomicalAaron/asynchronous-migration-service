from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER = os.getenv("MYSQL_USER", "migration_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "migration_pass")

_engine_cache: dict[str, Engine] = {}
_session_factory_cache: dict[str, sessionmaker] = {}


def get_tenant_database_url(tenant_id: str) -> str:
    return f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{tenant_id}"


def get_tenant_engine(tenant_id: str) -> Engine:
    if tenant_id not in _engine_cache:
        _engine_cache[tenant_id] = create_engine(
            get_tenant_database_url(tenant_id),
            pool_pre_ping=True,
        )
    return _engine_cache[tenant_id]


def get_tenant_session_factory(tenant_id: str) -> sessionmaker:
    if tenant_id not in _session_factory_cache:
        _session_factory_cache[tenant_id] = sessionmaker(
            bind=get_tenant_engine(tenant_id),
            autoflush=False,
            autocommit=False,
        )
    return _session_factory_cache[tenant_id]


@contextmanager
def tenant_session(tenant_id: str) -> Iterator[Session]:
    session_factory = get_tenant_session_factory(tenant_id)
    db = session_factory()
    try:
        yield db
    finally:
        db.close()


def tenant_database_exists(tenant_id: str) -> bool:
    engine = get_tenant_engine(tenant_id)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return True