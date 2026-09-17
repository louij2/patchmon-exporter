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
      -p 9823:9823 \
      --network patchmon-internal \
      -e PATCHMON_DB_HOST=patchmon-database-1 \
      -e PATCHMON_DB_USER=patchmon_exporter \
      -e PATCHMON_DB_PASSWORD=<password> \
      -e PATCHMON_DB_NAME=patchmon_db \
      patchmon-exporter:latest

Scrape `:9823/metrics` with Prometheus. Needs network access to PatchMon's
Postgres instance — join its internal docker network or expose the DB port.
