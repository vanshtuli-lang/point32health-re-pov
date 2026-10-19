"""
# Workflow 5 — Ops Control Plane

**Flow:** Calendar schedule → Gate → Prior Day Check → Conditional Bypass →
Fan-Out Reads (10) → Summarize → File Check

| Req | Step | What it shows |
|-----|------|---------------|
| R1  | Schedule | 22:00 Sun–Thu, `America/New_York` business calendar |
| R5  | `R5 · Gate` | Hold / Release via Variable `wf5_gate_state` |
| R2  | `R2 · Prior Day Check` | Previous business day's `file_check` must have succeeded (Sun looks back to Thu) |
| R9  | `R9 · Conditional Bypass` | Variable `wf5_bypass_enrichment` (default `false`) picks Enrich or Bypassed |
| R3  | `R3 · Read Claims` ×10 + `Summarize` | `.expand()` into 10 parallel reads of `claims_data_6k_copy2.csv`; Summarize waits for all |
| R11 | `R11 · File Check` | SFTP file must exist; fails after 10 min |

## Demo script

- **R1** — DAG header: schedule `0 22 * * 0-4`, timezone `America/New_York`, next run time.
- **R5** — Set Variable `wf5_gate_state=HOLD`, trigger: Gate waits (up_for_reschedule). Set `RELEASE`: continues within 30 s.
- **R2** — Seed once: trigger with logical date = previous business day 22:00 ET and `skip_prior_day_check=true`.
  Then trigger with the default logical date (now): Prior Day Check finds that run's successful
  `file_check` and passes. Never use a future logical date; Airflow won't start it until that time.
- **R9** — Set Variable `wf5_bypass_enrichment=true`: Enrich is skipped, Bypassed runs, reads continue.
- **R3** — Open `R3 · Read Claims`: 10 mapped tasks run in parallel, each logs the row count; Summarize logs all 10.
- **R11** — File Check confirms `claims_data_6k_copy2.csv` is on `sftp_claims`; remove the file to show it waiting.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import pendulum
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import CronTriggerTimetable, Param, TriggerRule, Variable, chain, dag, task
from airflow.sdk.exceptions import AirflowSkipException

log = logging.getLogger(__name__)

DAG_ID = "wf5_ops_control_plane"
SFTP_CONN_ID = "sftp_claims"
# claims_sftp_archive deletes claims_data_6k.csv and keeps copy2 as the stable backup.
CLAIMS_FILE = "claims_data_6k_copy2.csv"
READERS = list(range(1, 11))

BUSINESS_TZ = pendulum.timezone("America/New_York")
RUN_HOUR = 22
# Cron numbers Sun..Thu as 0-4; Python's date.weekday() numbers the same days 6,0,1,2,3.
BUSINESS_WEEKDAYS = frozenset({6, 0, 1, 2, 3})


def previous_business_day(day: date) -> date:
    day -= timedelta(days=1)
    while day.weekday() not in BUSINESS_WEEKDAYS:
        day -= timedelta(days=1)
    return day


def prior_run_logical_date(run_time: datetime) -> datetime:
    """Logical date of the previous business day's 22:00 run, so ad-hoc runs find it too."""
    prev = previous_business_day(pendulum.instance(run_time).in_timezone(BUSINESS_TZ).date())
    return pendulum.datetime(prev.year, prev.month, prev.day, RUN_HOUR, tz=BUSINESS_TZ)


@dag(
    dag_id=DAG_ID,
    schedule=CronTriggerTimetable(f"0 {RUN_HOUR} * * 0-4", timezone=BUSINESS_TZ),
    # Must precede every run's logical date (including back-dated demo runs): Airflow creates a
    # run with no task instances when its logical date is before start_date.
    start_date=pendulum.datetime(2026, 10, 1, tz=BUSINESS_TZ),
    catchup=False,
    dagrun_timeout=timedelta(hours=6),
    default_args={"owner": "platform-ops", "retries": 2, "retry_delay": timedelta(seconds=30)},
    params={
        "skip_prior_day_check": Param(
            False,
            type="boolean",
            title="R2 · Skip prior-day check",
            description="Use for the first run, when no previous business-day run exists.",
        ),
    },
    tags=["pov", "workflow-5", "autosys-migration"],
    doc_md=__doc__,
)
def wf5_ops_control_plane():
    # Reschedule mode frees the worker slot while on HOLD; the run timeout bounds the wait.
    @task.sensor(task_display_name="R5 · Gate", mode="reschedule", poke_interval=30, timeout=6 * 3600)
    def gate() -> bool:
        state = Variable.get("wf5_gate_state", default="RELEASE").strip().upper()
        log.info("Gate state: %s", state)
        return state == "RELEASE"

    @task.sensor(
        task_display_name="R2 · Prior Day Check",
        mode="reschedule",
        poke_interval=30,
        timeout=600,
    )
    def prior_day_check(params: dict, dag_run, ti) -> bool:
        if params["skip_prior_day_check"]:
            # Skipped rather than passed, so the override is visible in the Grid.
            raise AirflowSkipException("Prior-day check skipped via skip_prior_day_check=true.")
        prior = prior_run_logical_date(dag_run.logical_date or dag_run.run_after)
        log.info("Looking for a successful file_check in the run for %s", prior)
        return (
            ti.get_ti_count(dag_id=DAG_ID, task_ids=["file_check"], logical_dates=[prior], states=["success"])
            > 0
        )

    @task.branch(task_display_name="R9 · Conditional Bypass", trigger_rule=TriggerRule.NONE_FAILED)
    def conditional_bypass() -> str:
        bypass = Variable.get("wf5_bypass_enrichment", default="false").strip().lower() == "true"
        return "enrichment_bypassed" if bypass else "enrich"

    enrich = EmptyOperator(task_id="enrich", task_display_name="R9 · Enrich")
    bypassed = EmptyOperator(task_id="enrichment_bypassed", task_display_name="R9 · Bypassed")

    @task(
        task_display_name="R3 · Read Claims",
        map_index_template="reader {{ task.op_kwargs['reader'] }}",
        # One R9 branch is always skipped; the reads need only the other to succeed.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        # Each read loads pandas in its own process; 10 at once OOMKilled the 2Gi worker.
        max_active_tis_per_dagrun=5,
    )
    def read_claims(reader: int) -> int:
        import pandas as pd

        with SFTPHook(ssh_conn_id=SFTP_CONN_ID).get_conn() as sftp, sftp.open(CLAIMS_FILE) as f:
            rows = len(pd.read_csv(f))
        log.info("Reader %s: %s has %d rows", reader, CLAIMS_FILE, rows)
        return rows

    @task(task_display_name="R3 · Summarize")
    def summarize(row_counts: list[int]) -> None:
        for reader, rows in zip(READERS, row_counts):
            log.info("Reader %s: %d rows", reader, rows)
        log.info("All %d parallel reads finished.", len(READERS))

    file_check = SFTPSensor(
        task_id="file_check",
        task_display_name="R11 · File Check",
        sftp_conn_id=SFTP_CONN_ID,
        path=CLAIMS_FILE,
        poke_interval=30,
        timeout=600,
        deferrable=True,
    )

    row_counts = read_claims.expand(reader=READERS)
    chain(gate(), prior_day_check(), conditional_bypass(), [enrich, bypassed], row_counts)
    chain(summarize(row_counts), file_check)


wf5_ops_control_plane()
