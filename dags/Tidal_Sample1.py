"""airflow-legacy-graph: v1
file: dm_unx_check_semi.graph.yaml
"""

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow_legacy.extensions import run_window
from airflow_tidal.extensions import prerequisites, run_window
from pathlib import Path
from pendulum import DateTime, FixedTimezone, Time, Timezone
from pendulum.tz.timezone import FixedTimezone
import datetime

with DAG(
    dag_id="dm_unx_check_semi",
    schedule=None,
    start_date=DateTime(1970, 1, 1, 0, 0, 0, tzinfo=Timezone("UTC")),
    catchup=False,
    tags=["tidal", "migrated", "dm_unx_check_semi", "airflow-legacy:graph-v1"],
    default_args={"owner": "Schedulers"},
    params={
        "tidal_group_variables": {"-1140421726999987853": {"env_path_inf": "/app-san"}}
    },
    doc_md=Path(__file__)
    .parent.parent.joinpath("include/dm_unx_check_semi/_dag.md")
    .read_text(),
    dag_display_name="DM#UNX_CHECK_SEMI",
    max_active_runs=1,
) as dag:
    tidal_empty_group_completion_task = EmptyOperator(
        task_id="tidal_empty_group_completion",
        trigger_rule="all_success",
        task_display_name="Empty group completion",
    ).tidal(
        prerequisites(
            expressions=[],
            openings=[
                {
                    "opening_time": Time(2, 15, tzinfo=Timezone("UTC")),
                    "parent_dag_id": "dm_unx_check_semi",
                    "parent_timetable": None,
                    "timeout": 86400,
                }
            ],
            poll_interval=60.0,
            timeout=None,
        ),
        run_window(
            Time(2, 15, tzinfo=Timezone("UTC")),
            Time(23, 59, tzinfo=Timezone("UTC")),
            calendar_day=True,
        ),
    )
