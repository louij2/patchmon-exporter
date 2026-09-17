# patchmon-exporter — Prometheus exporter for PatchMon's Postgres backing store.
#
# Unlike semaphore-exporter (stdlib only, speaks HTTP), this has to speak the
# PostgreSQL wire protocol, so psycopg is a genuine dependency rather than a
# convenience. It is pinned and installed with --no-cache-dir; nothing else is
# added to the image.
FROM python:3.13-slim

# Non-root, UID matching the port — the convention used by the sibling
# exporters on tower. The DSN file is bind-mounted 0600 and must be owned by
# this UID: the container starts fine without that and fails only at scrape
# time, returning 502 "Permission denied", which is the failure mode
# semaphore-exporter's Dockerfile documents from experience.
RUN useradd --system --uid 9820 --no-create-home --shell /usr/sbin/nologin exporter

RUN pip install --no-cache-dir 'psycopg[binary]==3.2.3'

COPY exporter.py /app/exporter.py

USER 9820
EXPOSE 9820

# No HEALTHCHECK: it would open a Postgres connection on a timer for no
# benefit. Prometheus scraping /metrics is already the liveness signal
# (patchmon_exporter_last_run_timestamp).
ENTRYPOINT ["python3", "/app/exporter.py"]
