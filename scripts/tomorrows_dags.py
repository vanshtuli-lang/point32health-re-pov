#!/usr/bin/env python3
"""Preview tomorrow's cron schedule boundaries, optionally pause and restore DAGs.

Python 3.10+; install dependencies with: python3 -m pip install requests croniter

Usage (from the repository root):
  export AIRFLOW_API="https://<deployment-airflow-url>/api/v2"
  export AIRFLOW_TOKEN="<Deployment or Workspace API token>"
  export LOCAL_TZ="America/New_York"  # defaults to UTC
  python3 scripts/tomorrows_dags.py
  python3 scripts/tomorrows_dags.py --date 2026-10-14
  python3 scripts/tomorrows_dags.py --pause
  python3 scripts/tomorrows_dags.py --unpause

Only unpaused DAGs are listed. Times are cron estimates, not guaranteed runs:
start/end dates, catchup, scheduler health, concurrency and timetable semantics
can affect actual execution. Asset, manual, interval and custom schedules that
aren't cron expressions are reported as unknown. Pausing does not stop runs
already queued/running; unpausing may schedule missed intervals with catchup.

Use the same deployment and state file for pause and unpause. Don't change DAG
pause states manually between these operations. An interrupted/failed PATCH can
leave an uncertain state; retained entries should be reviewed before restoring.
"""

import argparse
import datetime as dt
import os
from pathlib import Path
import sys
from urllib.parse import quote, urlsplit
from zoneinfo import ZoneInfo


STATE_FILE = Path(__file__).resolve().parent / "paused_dags.txt"
TIMEOUT = 30


def get_all_dags(session, api):
    dags, offset = [], 0
    while True:
        response = session.get(
            f"{api}/dags",
            params={"limit": 100, "offset": offset, "paused": "false"},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        page = response.json()
        batch = page["dags"]
        dags.extend(dag for dag in batch if not dag["is_paused"])
        offset += len(batch)
        if offset >= page["total_entries"]:
            return dags
        if not batch:
            raise RuntimeError("DAG pagination returned an empty page before completion.")


def runs_tomorrow(session, api, dag, start, end, croniter):
    """Estimate cron boundaries inside [start, end), in each DAG's timezone."""
    expr = (dag.get("timetable_summary") or "").strip()
    if not expr or not croniter.is_valid(expr):
        return None
    dag_id = quote(dag["dag_id"], safe="")
    response = session.get(f"{api}/dags/{dag_id}/details", timeout=TIMEOUT)
    response.raise_for_status()
    details = response.json()
    dag_tz = ZoneInfo(details.get("timezone") or "UTC")
    base = start.astimezone(dt.timezone.utc) - dt.timedelta(seconds=1)
    iterator = croniter(expr, base.astimezone(dag_tz))
    hits = []
    while True:
        instant = iterator.get_next(dt.datetime)
        if instant.astimezone(dt.timezone.utc) >= end.astimezone(dt.timezone.utc):
            return hits
        if instant.astimezone(dt.timezone.utc) >= start.astimezone(dt.timezone.utc):
            hits.append(instant)


def set_paused(session, api, dag_id, paused):
    response = session.patch(
        f"{api}/dags/{quote(dag_id, safe='')}",
        params={"update_mask": "is_paused"},
        json={"is_paused": paused},
        timeout=TIMEOUT,
    )
    response.raise_for_status()


def save_state(path, dag_ids):
    """Replace atomically; state is written before each potentially uncertain PATCH."""
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text("".join(f"{dag_id}\n" for dag_id in dag_ids), encoding="utf-8")
    temporary.replace(path)


def confirm(action, api, count):
    answer = input(f"{action} {count} DAG(s) on {api}? Type 'yes' to confirm: ")
    if answer != "yes":
        print("Cancelled; no DAGs changed.")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--pause", action="store_true", help="Pause matching DAGs after confirmation.")
    modes.add_argument("--unpause", action="store_true", help="Restore DAGs in the state file after confirmation.")
    parser.add_argument("--date", type=dt.date.fromisoformat, help="Preview YYYY-MM-DD instead of tomorrow.")
    parser.add_argument("--state-file", type=Path, default=STATE_FILE, help="Restore list (default: scripts/paused_dags.txt).")
    args = parser.parse_args()
    if args.unpause and args.date:
        parser.error("--date is not used with --unpause")

    # Defer optional imports and configuration so --help works without setup.
    import requests
    from croniter import croniter

    api = os.environ.get("AIRFLOW_API", "").rstrip("/")
    token = os.environ.get("AIRFLOW_TOKEN", "")
    if not api or not token:
        parser.error("Set AIRFLOW_API (ending in /api/v2) and AIRFLOW_TOKEN first.")
    parsed = urlsplit(api)
    if parsed.scheme not in {"https", "http"} or not parsed.netloc or not parsed.path.endswith("/api/v2") or parsed.query or parsed.fragment or parsed.username:
        parser.error("AIRFLOW_API must be an HTTP(S) URL ending in /api/v2, without credentials/query/fragment.")
    if parsed.scheme != "https" and parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        parser.error("Use HTTPS for remote deployments to protect your token.")

    state_file = args.state_file.resolve()
    with requests.Session() as session:
        session.headers.update({"Authorization": f"Bearer {token}"})
        if args.unpause:
            remaining = state_file.read_text(encoding="utf-8").splitlines()
            remaining = list(dict.fromkeys(line for line in remaining if line))
            if not remaining:
                print("No DAGs to restore.")
                return 0
            if not confirm("Unpause", api, len(remaining)):
                return 0
            for dag_id in remaining.copy():
                set_paused(session, api, dag_id, False)
                remaining.remove(dag_id)
                save_state(state_file, remaining)
                print(f"unpaused {dag_id}")
            return 0

        if args.pause and state_file.exists() and state_file.read_text(encoding="utf-8").strip():
            parser.error(f"Existing restore list at {state_file}; restore/review it before pausing again.")
        local_tz = ZoneInfo(os.environ.get("LOCAL_TZ", "UTC"))
        target_date = args.date or (dt.datetime.now(local_tz).date() + dt.timedelta(days=1))
        start = dt.datetime.combine(target_date, dt.time(), local_tz)
        end = dt.datetime.combine(target_date + dt.timedelta(days=1), dt.time(), local_tz)
        print(f"Deployment: {api}\nCron preview: {target_date} ({local_tz})\n")
        to_pause, unknown = [], []
        for dag in get_all_dags(session, api):
            hits = runs_tomorrow(session, api, dag, start, end, croniter)
            if hits is None:
                unknown.append(dag["dag_id"])
            elif hits:
                to_pause.append(dag["dag_id"])
                times = ", ".join(hit.astimezone(local_tz).strftime("%H:%M %Z") for hit in hits[:5])
                more = f" (+{len(hits) - 5} more)" if len(hits) > 5 else ""
                print(f"{dag['dag_id']:<50} {len(hits):>3} estimated run(s): {times}{more}")
        print(f"\n{len(to_pause)} DAG(s) have cron boundaries on {target_date} ({local_tz}).")
        if unknown:
            print(f"Not predictable (asset/custom/manual/interval schedules): {', '.join(unknown)}")
        print("Cron estimates only; actual scheduling may differ. Pausing does not stop existing runs.")
        if args.pause and to_pause and confirm("Pause", api, len(to_pause)):
            recorded = []
            for dag_id in to_pause:
                # Recheck in case someone paused it since the preview.
                response = session.get(f"{api}/dags/{quote(dag_id, safe='')}", timeout=TIMEOUT)
                response.raise_for_status()
                if response.json()["is_paused"]:
                    print(f"skipped already-paused {dag_id}")
                    continue
                recorded.append(dag_id)
                save_state(state_file, recorded)
                set_paused(session, api, dag_id, True)
                print(f"paused {dag_id}")
            print(f"Restore list saved to {state_file}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted; review the restore list before retrying.", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        # Avoid dumping request headers, tokens or server response bodies.
        print(f"Error: {exc}. If a mutation failed, review the restore list before retrying.", file=sys.stderr)
        sys.exit(1)
