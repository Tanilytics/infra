# Tanilytics Infrastructure

> **Note:** This is an alternate version of the project that uses **Apache Kafka 4.2.0** instead of Redpanda. The purpose of this branch is to compare performance and behavior between Kafka and Redpanda under the same workload. All application-facing environment variables retain the `REDPANDA_*` prefix to minimize code changes, but the underlying broker is Kafka running in KRaft mode.

This directory contains the Docker Compose configuration for the Tanilytics local infrastructure stack, including Kafka, Redis, ClickHouse, Prometheus, Jaeger, ingestion, and processing.

## Quick Start

1. Copy the environment file:

   ```bash
   cp .env.example .env
   ```

2. Start the full local stack:
    ```bash
    docker compose up -d
    ```

## Dev Pipeline Compose

`docker-compose.dev.yml` mirrors the main compose stack, but replaces the three-node Kafka cluster with a single `kafka` broker (combined controller + broker in KRaft mode).

The `pipeline` profile enables only the data pipeline services:

- `kafka`
- `console`
- `createtopic`
- `redis`
- `clickhouse`
- `ingestion`
- `processing`

Start only the pipeline with:

```bash
docker compose -f docker-compose.dev.yml --profile pipeline up -d
```

The remaining app services are grouped under the `app` profile:

- `postgres`
- `redis`
- `clickhouse`
- `auth-service`
- `query-service`
- `prometheus`
- `jaeger`

Start only those app services with:

```bash
docker compose -f docker-compose.dev.yml --profile app up -d
```

Start the full dev stack with:

```bash
docker compose -f docker-compose.dev.yml --profile app --profile pipeline up -d
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

The default superuser is defined in `.env` as `REDPANDA_SUPERUSER=superuser`. The SCRAM user is created automatically by the `createtopic` init container on startup.
