
## Docker

### Spin up Containers
```commandline
docker compose up --build
```

Detached Mode
```commandline
docker compose up --build -d
```

Tear down
```commandline
docker compose down --remove-orphans
```

Tear down and remove volumes
```commandline
docker compose down --remove-orphans -v
```

### MySQL
```commandline
docker exec -it migration-mysql mysql -u root -prootpass
```

### Redis
```commandline
docker exec -it migration-redis redis-cli
LRANGE migration:jobs 0 -1
```

### Mongo
```commandline
docker exec -it migration-mongo mongosh
```

### Worker Logs
```commandline
docker compose logs -f worker
```

## Recommended Mongo indexes

Run these once in Mongo:

```JavaScript
db.global_identities.createIndex({ primaryEmail: 1 }, { unique: true });
db.global_identities.createIndex({ userId: 1 }, { unique: true });
db.global_identities.createIndex({ "tenantLinks.tenantId": 1, "tenantLinks.legacyUserId": 1 });
```

## Seeding

```commandline
docker compose exec api python -m app.seed_tenants
```

### What this does

It creates:

#### In MySQL

Two databases named exactly like:

`550e8400-e29b-41d4-a716-446655440000`

Each DB gets a users table and seed data.

#### In Mongo

Matching tenant docs like:

```javascript
{
  "tenantId": "550e8400-e29b-41d4-a716-446655440000",
  "displayName": "Acme Corp",
  "database": {
    "engine": "mysql",
    "host": "mysql",
    "port": 3306,
    "databaseName": "550e8400-e29b-41d4-a716-446655440000"
  },
  "isActive": true,
  "migrationEnabled": true
}
```