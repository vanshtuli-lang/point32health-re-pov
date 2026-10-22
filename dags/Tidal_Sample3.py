"""airflow-legacy-graph: v1
file: dm_odsstg_ods_supp_cntrct_ld_nite.graph.yaml
"""

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow_legacy.extensions import run_window
from airflow_legacy.timetable import DayOfWeek, PatternTimetable
from airflow_tidal.extensions import prerequisites, run_window
from include.tidal_insertions.scope_60efcd0fd8d429d9839c import (
    TIDAL_INSERTION_DAG_IDS_60EFCD0FD8D429D9839C,
)
from pathlib import Path
from pendulum import DateTime, FixedTimezone, Time, Timezone
from pendulum.tz.timezone import FixedTimezone
from zoneinfo import ZoneInfo
import datetime

with DAG(
    dag_id="dm_odsstg_ods_supp_cntrct_ld_nite",
    schedule=PatternTimetable(
        run_times=[Time(1, 0, tzinfo=Timezone("UTC"))],
        time_window=(
            Time(1, 0, tzinfo=Timezone("UTC")),
            Time(23, 59, tzinfo=Timezone("UTC")),
        ),
        tz=ZoneInfo("UTC"),
    ),
    start_date=DateTime(1970, 1, 1, 0, 0, 0, tzinfo=Timezone("UTC")),
    catchup=False,
    tags=[
        "tidal",
        "migrated",
        "dm_odsstg_ods_supp_cntrct_ld_nite",
        "airflow-legacy:graph-v1",
    ],
    default_args={"owner": "Schedulers"},
    params={"tidal_group_variables": {"-664901608494263677": {}}},
    doc_md=Path(__file__)
    .parent.parent.joinpath("include/dm_odsstg_ods_supp_cntrct_ld_nite/_dag.md")
    .read_text(),
    dag_display_name="DM#ODSSTG_ODS_SUPP_CNTRCT_LD_NITE",
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
                    "external_dag_id": "dm_odsstg_ods_suplr_netwk_ld_nite",
                    "external_task_id": None,
                    "external_task_ids": None,
                    "failed_states": [],
                    "id": "dm_odsstg_ods_supp_cntrct_ld_nite:dag:dag:job:-54431821342504901",
                    "ignore_if_not_scheduled": False,
                    "insertion_dag_ids": TIDAL_INSERTION_DAG_IDS_60EFCD0FD8D429D9839C.copy(),
                    "kind": "job",
                    "occurrence_mode": "match",
                    "occurrence_origin_dag_id": None,
                    "occurrence_origin_timetable": None,
                    "occurrence_scope": "N",
                    "occurrence_scope_dag_id": None,
                    "occurrence_scope_timetable": None,
                    "producer_schedule_mode": "calendar",
                    "producer_timetable": PatternTimetable(
                        run_times=[Time(1, 0, tzinfo=Timezone("UTC"))],
                        time_window=(
                            Time(1, 0, tzinfo=Timezone("UTC")),
                            Time(23, 59, tzinfo=Timezone("UTC")),
                        ),
                        tz=ZoneInfo("UTC"),
                    ),
                    "role": "predecessor",
                    "scope": {
                        "dag_id": "dm_odsstg_ods_supp_cntrct_ld_nite",
                        "endpoint": "dag",
                        "task_id": "dag",
                    },
                    "source": {
                        "tidal_dependency_consumer_dag_id": "dm_odsstg_ods_supp_cntrct_ld_nite",
                        "tidal_dependency_downstream_endpoint": "dag",
                        "tidal_dependency_downstream_id": "dag",
                        "tidal_dependency_id": -54431821342504901,
                        "tidal_dependency_targets": ["tidal_empty_group_completion"],
                        "tidal_dependency_type": "job",
                    },
                    "timeout": 86400,
                },
                {
                    "allowed_states": ["success"],
                    "external_dag_id": "dm_odsstg_ods_supp_pract_ld_nite",
                    "external_task_id": None,
                    "external_task_ids": None,
                    "failed_states": [],
                    "id": "dm_odsstg_ods_supp_cntrct_ld_nite:dag:dag:job:-7619934346068891",
                    "ignore_if_not_scheduled": False,
                    "insertion_dag_ids": TIDAL_INSERTION_DAG_IDS_60EFCD0FD8D429D9839C.copy(),
                    "kind": "job",
                    "occurrence_mode": "match",
                    "occurrence_origin_dag_id": None,
                    "occurrence_origin_timetable": None,
                    "occurrence_scope": "N",
                    "occurrence_scope_dag_id": None,
                    "occurrence_scope_timetable": None,
                    "producer_schedule_mode": "calendar",
                    "producer_timetable": PatternTimetable(
                        run_times=[Time(1, 0, tzinfo=Timezone("UTC"))],
                        time_window=(
                            Time(1, 0, tzinfo=Timezone("UTC")),
                            Time(23, 59, tzinfo=Timezone("UTC")),
                        ),
                        tz=ZoneInfo("UTC"),
                    ),
                    "role": "predecessor",
                    "scope": {
                        "dag_id": "dm_odsstg_ods_supp_cntrct_ld_nite",
                        "endpoint": "dag",
                        "task_id": "dag",
                    },
                    "source": {
                        "tidal_dependency_consumer_dag_id": "dm_odsstg_ods_supp_cntrct_ld_nite",
                        "tidal_dependency_downstream_endpoint": "dag",
                        "tidal_dependency_downstream_id": "dag",
                        "tidal_dependency_id": -7619934346068891,
                        "tidal_dependency_targets": ["tidal_empty_group_completion"],
                        "tidal_dependency_type": "job",
                    },
                    "timeout": 86400,
                },
            ],
            openings=[
                {
                    "opening_time": Time(1, 0, tzinfo=Timezone("UTC")),
                    "parent_dag_id": "dm_odsstg_ods_supp_cntrct_ld_nite",
                    "parent_timetable": PatternTimetable(
                        run_times=[Time(1, 0, tzinfo=Timezone("UTC"))],
                        time_window=(
                            Time(1, 0, tzinfo=Timezone("UTC")),
                            Time(23, 59, tzinfo=Timezone("UTC")),
                        ),
                        tz=ZoneInfo("UTC"),
                    ),
                    "timeout": 86400,
                }
            ],
            poll_interval=60.0,
            timeout=None,
        ),
        run_window(
            Time(1, 0, tzinfo=Timezone("UTC")),
            Time(23, 59, tzinfo=Timezone("UTC")),
            calendar_day=True,
        ),
    )
