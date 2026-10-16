Overview
========

Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
    - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://www.astronomer.io/docs/learn/get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.
- scripts: Operational helper scripts that run from your machine against a Deployment's Airflow REST API (not inside Airflow). See [Pausing Tomorrow's DAGs](#pausing-tomorrows-dags-scriptstomorrows_dagspy) below.
    - `tomorrows_dags.py`: Previews which DAGs have cron runs on a given day, and can pause them and later restore them.
    - `paused_dags.txt`: The restore list written by `tomorrows_dags.py --pause` and consumed by `--unpause`.

Deploy Your Project Locally
===========================

Start Airflow on your local machine by running 'astro dev start'.

This command will spin up five Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- DAG Processor: The Airflow component responsible for parsing DAGs
- API Server: The Airflow component responsible for serving the Airflow UI and API
- Triggerer: The Airflow component responsible for triggering deferred tasks

When all five containers are ready the command will open the browser to the Airflow UI at http://localhost:8080/. You should also be able to access your Postgres Database at 'localhost:5432/postgres' with username 'postgres' and password 'postgres'.

Note: If you already have either of the above ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

Pausing Tomorrow's DAGs (`scripts/tomorrows_dags.py`)
====================================================

`scripts/tomorrows_dags.py` lists the unpaused DAGs whose cron schedule fires on a given day (tomorrow by default), and can optionally pause them (e.g. ahead of a maintenance window) and later unpause exactly the DAGs it paused.

### Prerequisites

- Python 3.10+
- Dependencies: `python3 -m pip install requests croniter`
- An Airflow 3 REST API URL (ending in `/api/v2`) and an API token with permission to read and update DAGs (an Astro Deployment or Workspace API token).

### Configuration

The script is configured with environment variables:

| Variable | Required | Description |
|---|---|---|
| `AIRFLOW_API` | Yes | Airflow REST API base URL, ending in `/api/v2`, e.g. `https://<deployment-airflow-url>/api/v2`. Must be HTTPS unless pointing at `localhost`. |
| `AIRFLOW_TOKEN` | Yes | Bearer token used to authenticate to the API. |
| `LOCAL_TZ` | No | IANA timezone used to define "the day" and to display run times (e.g. `America/New_York`). Defaults to `UTC`. |

```bash
export AIRFLOW_API="https://<deployment-airflow-url>/api/v2"
export AIRFLOW_TOKEN="<Deployment or Workspace API token>"
export LOCAL_TZ="America/New_York"
```

Don't commit the token or put it in a tracked file.

### Usage

Run from the repository root:

```bash
# Preview tomorrow's estimated cron runs (read-only)
python3 scripts/tomorrows_dags.py

# Preview a specific date (read-only)
python3 scripts/tomorrows_dags.py --date 2026-10-14

# Preview, then pause every DAG with a run that day (prompts for confirmation)
python3 scripts/tomorrows_dags.py --pause
python3 scripts/tomorrows_dags.py --pause --date 2026-10-14

# Unpause the DAGs recorded in the restore list (prompts for confirmation)
python3 scripts/tomorrows_dags.py --unpause

# Use a custom restore list instead of scripts/paused_dags.txt
python3 scripts/tomorrows_dags.py --pause --state-file /path/to/restore.txt
python3 scripts/tomorrows_dags.py --unpause --state-file /path/to/restore.txt

# Full help
python3 scripts/tomorrows_dags.py --help
```

| Option | Description |
|---|---|
| `--date YYYY-MM-DD` | Day to preview instead of tomorrow (in `LOCAL_TZ`). Not allowed with `--unpause`. |
| `--pause` | After the preview, pause the matching DAGs. You must type `yes` to proceed. |
| `--unpause` | Unpause every DAG listed in the restore list. You must type `yes` to proceed. |
| `--state-file PATH` | Restore list location. Defaults to `scripts/paused_dags.txt`. |

`--pause` and `--unpause` are mutually exclusive. With neither flag, the script only reads from the API and changes nothing.

### Example output

```
Deployment: https://<deployment-airflow-url>/api/v2
Cron preview: 2026-10-17 (America/New_York)

member_load_ssh_demo                                 1 estimated run(s): 06:00 EDT
example_astronauts                                  24 estimated run(s): 00:00 EDT, 01:00 EDT, 02:00 EDT, 03:00 EDT, 04:00 EDT (+19 more)

2 DAG(s) have cron boundaries on 2026-10-17 (America/New_York).
Not predictable (asset/custom/manual/interval schedules): some_asset_triggered_dag
Cron estimates only; actual scheduling may differ. Pausing does not stop existing runs.
```

### How the pause/restore workflow works

1. `--pause` refuses to run if the restore list already has entries, so a previous pause is never overwritten. Restore or review that list first.
2. For each matching DAG, the script re-checks that it's still unpaused, records it in the restore list, *then* pauses it. DAGs already paused by someone else are skipped and not recorded, so they stay paused after a restore.
3. `--unpause` unpauses each DAG in the list and removes it from the file as it goes. When it finishes, the file is empty and ready for the next `--pause`.

Typical maintenance-window flow:

```bash
python3 scripts/tomorrows_dags.py              # 1. review what would be paused
python3 scripts/tomorrows_dags.py --pause      # 2. pause, type 'yes'
# ... maintenance ...
python3 scripts/tomorrows_dags.py --unpause    # 3. restore, type 'yes'
```

### Caveats

- **Only unpaused DAGs with a cron `schedule` are evaluated.** Asset-triggered, manual (`None`), `timedelta`/interval and custom-timetable DAGs are listed as "Not predictable" and are never paused by the script.
- **Times are estimates.** They're computed from the cron expression in each DAG's own timezone; `start_date`/`end_date`, catchup, concurrency limits and scheduler health can change what actually runs.
- **Pausing doesn't stop runs that are already queued or running.**
- **Unpausing a DAG with `catchup=True` may trigger runs for intervals missed while it was paused.**
- **Use the same `AIRFLOW_API` and state file for both pause and unpause**, and don't manually change those DAGs' pause states in between.
- **If the script is interrupted or a request fails** (exit code `130` or `1`), the restore list may include a DAG whose pause didn't succeed. Review `scripts/paused_dags.txt` against the Airflow UI before re-running.
- `--unpause` expects the state file to exist; if it doesn't, the script exits with an error and changes nothing.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.
