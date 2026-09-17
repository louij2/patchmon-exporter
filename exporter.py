#!/usr/bin/env python3
"""Prometheus exporter for PatchMon (patchmon.int.lcserver.co.uk / tower:3001).

PatchMon has no /metrics endpoint. Its REST API needs a session or API key per
host, and there is no fleet-wide read token, so this reads the Postgres backing
store directly over the patchmon-internal docker network -- read-only, as a
dedicated `patchmon_exporter` role with SELECT and nothing else.


THE BLIND SPOT THIS EXISTS TO CLOSE
-----------------------------------
Before this exporter, PatchMon appeared in Prometheus three times: a blackbox
probe of its URL, a redis exporter, and a postgres exporter. All three measure
whether PATCHMON is up. None measures whether the FLEET IS PATCHED. Those are
different questions, and on 2026-08-28 they had different answers: every
PatchMon-related series was green while pi0 carried 7 unapplied security
updates that had been failing to install since 2026-06-19.

    A HOST THAT CHECKS IN IS NOT A HOST THAT PATCHES.

pi0 is the worked example and the reason for `patchmon_host_patch_stale_seconds`.
Its agent checked in every hour without fail, so `last_update` was minutes old
and every freshness signal read healthy. What had actually happened is that an
`apt-get upgrade` was OOM-killed on 2026-06-18 (the host has 407 MB of RAM),
leaving initramfs-tools-core half-configured with an unanswered conffile
prompt. Every patch run after that died at the same prompt with exit 100. Two
months, three failed runs, zero alerts.

So freshness of CHECK-IN and freshness of SUCCESSFUL PATCHING are exported as
two separate metrics, because they fail independently and need different
responses:

| Condition                    | Metric                             | Response          |
|------------------------------|------------------------------------|-------------------|
| agent stopped reporting      | patchmon_host_report_stale_seconds | is the host up?   |
| reports fine, patching fails | patchmon_host_patch_stale_seconds  | read the run log  |
| enrolled but never activated | patchmon_host_enrolled             | finish the enrol  |


COUNTING THE SET, NOT PROBING ITS MEMBERS
-----------------------------------------
`patchmon_fleet_hosts_total` is exported so alerts can be written against the
SIZE of the inventory rather than against per-host series. A per-host rule can
only fire for a host whose series exists; when a host is deleted from PatchMon,
or was never enrolled, its series simply stops and the rule goes quiet with no
config change and no notification. The estate has been bitten by exactly this
narrowing before (the 41-day expired vpsgb.co.uk certificate). Alert on the
count and a vanished host is a change in a number, which is visible.

`patchmon_host_enrolled` deliberately emits a series for hosts in `pending`
state -- the ghost records that never completed enrolment. There are five of
them here (tower.local, aapanel, Mini1.local, a duplicate Pi4.local, Manjaro),
each of which has looked like "a host PatchMon knows about" for ~70 days while
being monitored by nothing at all. Silence about them is the failure.


THE MANAGED MACHINE IS `node`, NOT `host`
-----------------------------------------
Every per-machine series here is labelled `node`, which reads oddly next to the
rest of the estate where `host` is the usual name. It is deliberate, and the
first version of this exporter got it wrong.

`host` is already taken. Every scrape job in this estate's prometheus.yml
attaches `host: <the box running the exporter>` as a TARGET label, and this
exporter's job is no exception -- it carries `host: tower`. When a scraped
metric brings its own label of the same name, Prometheus does not merge them
and does not error: it silently renames the metric's label to `exported_host`
and keeps the target's value. So `patchmon_host_never_patched{host="Pi0.local"}`
arrived in Prometheus as `{host="tower", exported_host="Pi0.local"}`.

Nothing about that looks broken. The exporter served the right label, the
scrape succeeded, the series existed, the alert rules matched and fired. They
just all said `tower`. Sixteen distinct hosts produced sixteen alerts naming
the same machine, and the one thing an alert has to tell you -- which box to go
and look at -- was the thing that was wrong.

`node` does not collide, so it survives the scrape intact.


METRICS
-------
  patchmon_host_packages_pending{node,os_type}          packages needing update
  patchmon_host_packages_security_pending{node,os_type} of those, security-flagged
  patchmon_host_report_stale_seconds{node,os_type}      age of last agent check-in
  patchmon_host_patch_stale_seconds{node,os_type}       age of last SUCCESSFUL patch run
  patchmon_host_never_patched{node,os_type}             1 = no successful run, ever
  patchmon_host_patch_run_success{node,os_type}         last run reached a verdict: 1 ok / 0 failed
  patchmon_host_needs_reboot{node,os_type}              1 = reboot pending to finish patching
  patchmon_host_enrolled{node,status}                   1 = active, 0 = pending/never enrolled
  patchmon_host_info{node,os_type,os_version,agent_version,architecture}  always 1
  patchmon_fleet_hosts_total{status}                    inventory size by status
  patchmon_fleet_agent_versions{version}                hosts per agent version
  patchmon_exporter_last_run_timestamp                  heartbeat
"""
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "9820"))
CACHE_SECONDS = int(os.environ.get("CACHE_SECONDS", "60"))
DSN_FILE = os.environ.get("PATCHMON_DSN_FILE", "/run/secrets/patchmon-dsn")
QUERY_TIMEOUT_MS = int(os.environ.get("QUERY_TIMEOUT_MS", "10000"))

# A patch run reaches a VERDICT only in these states. `queued` and `running`
# are in flight; `cancelled` is a decision (PatchMon's own 30-minute reaper, or
# a human), not a failure of patching. Scoring an in-flight run as failed is
# the bug that made semaphore-exporter v1's alerts unresolvable -- see that
# exporter's notes. The same mistake is not repeated here.
VERDICT = ("completed", "failed")

_cache = {"body": None, "at": 0.0}


def _dsn():
    with open(DSN_FILE, "r", encoding="utf-8") as fh:
        dsn = fh.read().strip()
    if not dsn:
        raise RuntimeError(f"{DSN_FILE} is empty")
    return dsn


def _esc(s):
    return str(s).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


# Package counts are aggregated in SQL rather than in Python because
# host_packages has one row per package per host and joining it to hosts in a
# single query would multiply the host rows. Two queries, each returning one
# row per host, keeps the host attributes authoritative.
Q_HOSTS = """
SELECT id,
       COALESCE(NULLIF(friendly_name, ''), NULLIF(hostname, ''), 'unnamed') AS name,
       COALESCE(NULLIF(os_type, ''), 'unknown')       AS os_type,
       COALESCE(NULLIF(os_version, ''), 'unknown')    AS os_version,
       COALESCE(NULLIF(agent_version, ''), 'none')    AS agent_version,
       COALESCE(NULLIF(architecture, ''), 'unknown')  AS architecture,
       COALESCE(NULLIF(status, ''), 'unknown')        AS status,
       COALESCE(needs_reboot, false)                  AS needs_reboot,
       EXTRACT(epoch FROM (now() - last_update))      AS report_age
FROM hosts
"""

Q_PACKAGES = """
SELECT host_id,
       COUNT(*) FILTER (WHERE needs_update)                             AS pending,
       COUNT(*) FILTER (WHERE needs_update AND is_security_update)      AS security
FROM host_packages
GROUP BY host_id
"""

# The most recent run per host that reached a verdict, and separately the most
# recent SUCCESSFUL one. Both are needed: a host whose last run failed still
# has a "last time patching worked" that the failure duration is measured from.
Q_RUNS = """
SELECT DISTINCT ON (host_id)
       host_id, status,
       EXTRACT(epoch FROM (now() - COALESCE(completed_at, created_at))) AS age
FROM patch_runs
WHERE status = ANY(%s)
ORDER BY host_id, created_at DESC
"""

Q_LAST_OK = """
SELECT host_id,
       EXTRACT(epoch FROM (now() - MAX(COALESCE(completed_at, created_at)))) AS age
FROM patch_runs
WHERE status = 'completed'
GROUP BY host_id
"""


def collect():
    now = time.time()
    with psycopg.connect(_dsn(), connect_timeout=10) as conn:
        conn.execute(f"SET statement_timeout = {QUERY_TIMEOUT_MS}")
        hosts = conn.execute(Q_HOSTS).fetchall()
        pkgs = {r[0]: (r[1], r[2]) for r in conn.execute(Q_PACKAGES).fetchall()}
        runs = {r[0]: (r[1], r[2]) for r in conn.execute(Q_RUNS, (list(VERDICT),)).fetchall()}
        last_ok = {r[0]: r[1] for r in conn.execute(Q_LAST_OK).fetchall()}

    lines = [
        "# HELP patchmon_host_packages_pending Packages on this host with an available update",
        "# TYPE patchmon_host_packages_pending gauge",
        "# HELP patchmon_host_packages_security_pending Pending packages PatchMon flags as security updates",
        "# TYPE patchmon_host_packages_security_pending gauge",
        "# HELP patchmon_host_report_stale_seconds Seconds since the agent last checked in. Freshness of REPORTING only -- a host can check in perfectly while failing every patch.",
        "# TYPE patchmon_host_report_stale_seconds gauge",
        "# HELP patchmon_host_patch_stale_seconds Seconds since patching last SUCCEEDED on this host. No series for a host that has never had a successful run -- see patchmon_host_never_patched.",
        "# TYPE patchmon_host_patch_stale_seconds gauge",
        "# HELP patchmon_host_never_patched 1 if no patch run has ever completed successfully on this host",
        "# TYPE patchmon_host_never_patched gauge",
        "# HELP patchmon_host_patch_run_success Whether the most recent CONCLUDED patch run succeeded. In-flight and cancelled runs are ignored -- they are not verdicts.",
        "# TYPE patchmon_host_patch_run_success gauge",
        "# HELP patchmon_host_needs_reboot 1 if a reboot is required to finish applying updates",
        "# TYPE patchmon_host_needs_reboot gauge",
        "# HELP patchmon_host_enrolled 1 if the host is active, 0 if enrolment never completed",
        "# TYPE patchmon_host_enrolled gauge",
        "# HELP patchmon_host_info Host attributes, always 1",
        "# TYPE patchmon_host_info gauge",
        "# HELP patchmon_fleet_hosts_total Hosts in the PatchMon inventory by status. Alert on this, not on per-host series -- a deleted host has no per-host series to alert on.",
        "# TYPE patchmon_fleet_hosts_total gauge",
        "# HELP patchmon_fleet_agent_versions Hosts running each agent version",
        "# TYPE patchmon_fleet_agent_versions gauge",
        "# HELP patchmon_exporter_last_run_timestamp Unix time this exporter last completed a successful scrape",
        "# TYPE patchmon_exporter_last_run_timestamp gauge",
        f"patchmon_exporter_last_run_timestamp {int(now)}",
    ]

    by_status = {}
    by_agent = {}

    for (hid, name, os_type, os_version, agent_version,
         arch, status, needs_reboot, report_age) in hosts:
        h, o = _esc(name), _esc(os_type)
        lab = f'node="{h}",os_type="{o}"'

        by_status[status] = by_status.get(status, 0) + 1
        by_agent[agent_version] = by_agent.get(agent_version, 0) + 1

        lines.append('patchmon_host_info{node="%s",os_type="%s",os_version="%s",'
                     'agent_version="%s",architecture="%s"} 1'
                     % (h, o, _esc(os_version), _esc(agent_version), _esc(arch)))
        lines.append('patchmon_host_enrolled{node="%s",status="%s"} %d'
                     % (h, _esc(status), 1 if status == "active" else 0))

        pending, security = pkgs.get(hid, (0, 0))
        lines.append("patchmon_host_packages_pending{%s} %d" % (lab, pending))
        lines.append("patchmon_host_packages_security_pending{%s} %d" % (lab, security))
        lines.append("patchmon_host_needs_reboot{%s} %d" % (lab, 1 if needs_reboot else 0))

        # A host that has never reported has last_update NULL -- report_age is
        # then None. Emitting 0 would read as "checked in just now", which is
        # the opposite of the truth, so the series is omitted and the host is
        # accounted for by patchmon_host_enrolled 0 instead.
        if report_age is not None:
            lines.append("patchmon_host_report_stale_seconds{%s} %d" % (lab, int(report_age)))

        ok_age = last_ok.get(hid)
        lines.append("patchmon_host_never_patched{%s} %d" % (lab, 0 if ok_age is not None else 1))
        if ok_age is not None:
            lines.append("patchmon_host_patch_stale_seconds{%s} %d" % (lab, int(ok_age)))

        run = runs.get(hid)
        if run is not None:
            lines.append("patchmon_host_patch_run_success{%s} %d"
                         % (lab, 1 if run[0] == "completed" else 0))

    for status, count in sorted(by_status.items()):
        lines.append('patchmon_fleet_hosts_total{status="%s"} %d' % (_esc(status), count))
    for version, count in sorted(by_agent.items()):
        lines.append('patchmon_fleet_agent_versions{version="%s"} %d' % (_esc(version), count))

    return "\n".join(lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def do_GET(self):
        if self.path not in ("/metrics", "/"):
            self.send_response(404)
            self.end_headers()
            return

        now = time.time()
        if _cache["body"] is None or (now - _cache["at"]) > CACHE_SECONDS:
            try:
                _cache["body"] = collect()
                _cache["at"] = now
            except (psycopg.Error, OSError, RuntimeError, ValueError) as exc:
                # 502 with the reason in the body. `docker logs` records only
                # the access line, which says "502" and nothing about why.
                self.send_response(502)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(f"scrape failed: {exc}\n".encode())
                return

        body = _cache["body"].encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    print(f"patchmon-exporter listening on :{LISTEN_PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
