# Tanilytics Infrastructure

This directory contains the Docker Compose configuration for the Tanilytics local infrastructure stack, including Redpanda, Redis, ClickHouse, ClickStack, Prometheus, Jaeger, ingestion, and processing.

## Quick Start

1. Copy the environment file:

   ```bash
   cp .env.example .env
   ```

2. Start the cluster:
   ```bash
   docker compose up -d
   ```

## Migrations

ClickHouse migrations live in `migrations/` and are managed manually with `golang-migrate` via `mise`.

1. Install the local tools:

   ```bash
   mise trust
   mise install
   ```

2. Make sure `CLICKHOUSE_MIGRATIONS_URL` in `.env` points at the ClickHouse instance you want to migrate and uses the same `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USERNAME`, and `CLICKHOUSE_PASSWORD` values as Docker Compose.

3. Run the migration tasks:
   ```bash
   mise run migrate-up
   mise run migrate-version
   ```

Create a new sequential migration with:

```bash
mise run migrate-create -- add_some_change
```

## Configuration

### Changing the Superuser

The default superuser is defined in `.env` as `REDPANDA_SUPERUSER=superuser`. If you change this value, you must also update the `superusers` list in `bootstrap.yml` to match:

```yaml
# bootstrap.yml
superusers:
  - <your-new-username>
```

This is required because Docker Compose does not substitute environment variables in mounted configuration files.
