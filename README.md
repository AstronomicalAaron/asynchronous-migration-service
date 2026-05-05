# Multi-Tenant Migration Orchestrator

A distributed system for executing schema and data migrations across
thousands of tenant-specific databases in a controlled, observable, and
scalable way.

------------------------------------------------------------------------

## 🚀 Overview

This system solves a common SaaS problem:

> How do you safely run migrations across thousands of tenant databases?

It provides:

-   Centralized job orchestration
-   Per-tenant migration execution
-   Queue-based distributed processing
-   Checkpointing and observability
-   Idempotent, versionable migrations

------------------------------------------------------------------------

## 🧠 Architecture

Client (POST /jobs)\
↓\
API (FastAPI)\
↓\
MongoDB (tenant registry - read only)\
↓\
MySQL (migration_control - job tracking)\
↓\
Redis (queue broker)\
↓\
Worker(s)\
↓\
Tenant MySQL Databases (actual migrations)

------------------------------------------------------------------------

## 🧩 Components

### API

-   Accepts migration requests
-   Resolves tenant DB names from Mongo
-   Creates jobs in MySQL
-   Queues work in Redis

### Worker

-   Consumes jobs from Redis
-   Executes migrations against tenant DBs
-   Updates status and checkpoints

### MongoDB (Tenant Registry)

-   Stores tenant metadata
-   Read-only during migrations

### MySQL (migration_control)

Tracks: - migration_jobs - migration_job_targets -
migration_job_events - migration_checkpoints

### Redis

-   Queue broker
-   One job per tenant DB

### Tenant Databases

-   UUID-based database per tenant
-   Migrations executed in-place

------------------------------------------------------------------------

## 🔁 Migration Flow

1.  POST /jobs
2.  API resolves tenant DBs from Mongo
3.  API creates job + targets in MySQL
4.  API queues jobs in Redis
5.  Worker processes each tenant DB
6.  Worker runs migration
7.  Worker updates job status

------------------------------------------------------------------------

## 🧱 Migration System

### Structure

app/migrations/ - base.py - registry.py - tenant/ -
add_phone_number_to_users.py

------------------------------------------------------------------------

### Interface

Each migration implements:

-   validate()
-   up()
-   down()

------------------------------------------------------------------------

## 📦 Example Migration

Adds phone_number column:

``` sql
ALTER TABLE users ADD COLUMN phone_number VARCHAR(20);
```

------------------------------------------------------------------------

## 🧪 Running Locally

``` bash
docker compose up --build
```

Seed tenants:

``` bash
docker compose exec api python -m app.seed_tenants
```

------------------------------------------------------------------------

## 🔍 Verify

``` sql
DESCRIBE users;
```

------------------------------------------------------------------------

## ⚠️ Design Decisions

-   Mongo is read-only
-   Migrations are idempotent
-   Worker is generic
-   Queue enables scale

------------------------------------------------------------------------

## 🧠 Summary

Transforms:

"run scripts manually"

into:

a scalable migration platform.

---

## Full Command List

### Start Everything Fresh

```shell
docker compose down -v
docker compose up --build
```
Use `-d` for detach mode (containers run in background)

### Seed Tenant Databases

```shell
docker compose exec api python -m app.seed_tenants
```

### Verify Tenants Exist (Mongo)

```shell
docker exec -it migration-mongo mongosh
```

```javascript
use migration_artifacts
db.tenants.find().pretty()
```

### Verify Tenant DB Exists (MySQL)

```shell
docker exec -it migration-mysql mysql -u root -prootpass
```

```sql
SHOW DATABASES;
USE `your-tenant-uuid`;
SHOW TABLES;
SELECT * FROM users;
```

### Trigger a Migration

`https://localhost:8080/docs`

POST `/jobs`

Option A -- Specific Tenant

```json
{
  "requested_by_user_id": "550e8400-e29b-41d4-a716-446655440000",
  "migration_name": "add_phone_number_to_users",
  "tenant_ids": ["your-tenant-uuid"],
  "dry_run": false
}
```

Option B -- All Tenants

```json
{
  "requested_by_user_id": "550e8400-e29b-41d4-a716-446655440000",
  "migration_name": "add_phone_number_to_users",
  "dry_run": false
}
```

### Watch Worker Execute

```shell
docker compose logs -f worker
```

### Verify Migration Applied (REAL RESULT)

```shell
docker exec -it migration-mysql mysql -u root -prootpass
```

```sql
USE `your-tenant-uuid`;

DESCRIBE users;
```

### Verify Job Tracking (Control DB)

```sql
USE migration_control;

SELECT job_id, status, total_targets, completed_targets
FROM migration_jobs
ORDER BY id DESC;

SELECT tenant_id, status, records_processed
FROM migration_job_targets
ORDER BY id DESC;
```

### Inspect Redis Queue

```shell
docker exec -it migration-redis redis-cli
```

```shell
LRANGE migration:jobs 0 -1
```

Clear (if needed):

```shell
DEL migration:jobs
```