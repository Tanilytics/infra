# Tanilytics Infrastructure

This directory contains the Docker Compose configuration for the Tanilytics Redpanda cluster.

## Quick Start

1. Copy the environment file:
   ```bash
   cp .env.example .env
   ```

2. Start the cluster:
   ```bash
   docker compose up -d
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
