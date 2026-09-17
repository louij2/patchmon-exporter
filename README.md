# patchmon-exporter

Prometheus exporter for [PatchMon](https://github.com/PatchMon/PatchMon)
that answers "is the fleet actually patched?" — a different question from
"is PatchMon up" (which a blackbox probe already answers) or "did the agent
check in" (which can stay green while updates silently fail to apply).

Reads PatchMon's Postgres backing store directly, read-only, as a dedicated
DB role with `SELECT` and nothing else — PatchMon's own REST API needs a
session/API key per host with no fleet-wide read token, so the database is
the only place to see every host's patch state in one query.

Key metric: `patchmon_host_patch_stale_seconds` — age since a host last
successfully applied updates, independent of whether its agent is still
checking in on schedule. See the module docstring in `exporter.py` for the
real incident (a host silently unpatched for 2+ months while every
check-in-based signal read healthy) that this exists to catch.

## Build

    docker build -t patchmon-exporter:latest .

## Run

    docker run -d --name patchmon-exporter --restart unless-stopped \
      -p 9820:9820 \
      --network patchmon-internal \
      -v /path/to/patchmon-dsn:/run/secrets/patchmon-dsn:ro \
      patchmon-exporter:latest

The DSN file (default path `/run/secrets/patchmon-dsn`, override with
`PATCHMON_DSN_FILE`) holds a standard Postgres connection string, e.g.
`host=patchmon-database-1 port=5432 dbname=patchmon_db user=patchmon_exporter password=...`,
for a dedicated read-only role — never bake the DSN into the image or a
plain env var. `LISTEN_PORT` (default 9820) and `CACHE_SECONDS` (default
60) are also overridable via env.

Scrape `:9820/metrics` with Prometheus. Needs network access to PatchMon's
Postgres instance — join its internal docker network or expose the DB port.
