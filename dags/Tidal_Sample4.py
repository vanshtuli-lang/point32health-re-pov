"""airflow-legacy-graph: v1
file: dm_hrp_membpaymtrn_week.graph.yaml
"""

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow_legacy.extensions import run_window
from airflow_tidal.extensions import prerequisites, run_window
from include.tidal_insertions.scope_e51c731024c67f9e24f8 import (
    TIDAL_INSERTION_DAG_IDS_E51C731024C67F9E24F8,
)
from pathlib import Path
from pendulum import DateTime, FixedTimezone, Time, Timezone
from pendulum.tz.timezone import FixedTimezone
import datetime

with DAG(
    dag_id="dm_hrp_membpaymtrn_week",
    schedule=None,
    start_date=DateTime(1970, 1, 1, 0, 0, 0, tzinfo=Timezone("UTC")),
    catchup=False,
    tags=["tidal", "migrated", "dm_hrp_membpaymtrn_week", "airflow-legacy:graph-v1"],
    default_args={"owner": "Schedulers"},
    params={"tidal_group_variables": {"-40290785607658022": {}}},
    doc_md=Path(__file__)
    .parent.parent.joinpath("include/dm_hrp_membpaymtrn_week/_dag.md")
    .read_text(),
    dag_display_name="DM#HRP_MEMBPAYMTRN_WEEK",
    max_active_runs=1,
) as dag:
    tidal_empty_group_completion_task = EmptyOperator(
        task_id="tidal_empty_group_completion",
        trigger_rule="all_success",
        task_display_name="Empty group completion",
    ).tidal(
        prerequisites(
            expressions=[
                {
                    "allowed_states": ["success"],
                    "external_dag_id": "dm_hrp_supppaymtrn_week",
                    "external_task_id": None,
                    "external_task_ids": None,
                    "failed_states": [],
                    "id": "dm_hrp_membpaymtrn_week:dag:dag:job:-194860903566907818",
                    "ignore_if_not_scheduled": True,
                    "insertion_dag_ids": TIDAL_INSERTION_DAG_IDS_E51C731024C67F9E24F8.copy(),
                    "kind": "job",
                    "occurrence_mode": "match",
                    "occurrence_origin_dag_id": None,
                    "occurrence_origin_timetable": None,
                    "occurrence_scope": "N",
                    "occurrence_scope_dag_id": None,
                    "occurrence_scope_timetable": None,
                    "producer_schedule_mode": "unresolved",
                    "producer_timetable": None,
                    "role": "predecessor",
                    "scope": {
                        "dag_id": "dm_hrp_membpaymtrn_week",
                        "endpoint": "dag",
                        "task_id": "dag",
                    },
                    "source": {
                        "tidal_dependency_consumer_dag_id": "dm_hrp_membpaymtrn_week",
                        "tidal_dependency_downstream_endpoint": "dag",
                        "tidal_dependency_downstream_id": "dag",
                        "tidal_dependency_id": -194860903566907818,
                        "tidal_dependency_targets": ["tidal_empty_group_completion"],
                        "tidal_dependency_type": "job",
                    },
                    "timeout": 86400,
                }
            ],
            openings=[
                {
                    "opening_time": Time(2, 0, tzinfo=Timezone("UTC")),
                    "parent_dag_id": "dm_hrp_membpaymtrn_week",
                    "parent_timetable": None,
                    "timeout": 86400,
                }
            ],
            poll_interval=60.0,
            timeout=None,
        ),
        run_window(
            Time(2, 0, tzinfo=Timezone("UTC")),
            Time(23, 59, tzinfo=Timezone("UTC")),
            calendar_day=True,
        ),
    )
