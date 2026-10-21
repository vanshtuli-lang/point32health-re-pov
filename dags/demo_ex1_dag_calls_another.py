"""
Example 1: One DAG calls another DAG.

The parent DAG triggers the child DAG and waits for it to finish.
"""

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag
from datetime import timedelta

from pendulum import datetime


# Parent DAG: runs start, calls the child DAG, then runs end.
@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    is_paused_upon_creation=False,
    tags=["demo"],
)
def demo_parent_dag():
    start = EmptyOperator(task_id="start")

    # Call the child DAG and wait until it finishes.
    call_child = TriggerDagRunOperator(
        task_id="call_child",
        trigger_dag_id="demo_child_dag",
        wait_for_completion=True,
        # Check the child's status every 10s (default is 60s).
        poke_interval=10,
        # Wait in the triggerer instead of holding a worker slot.
        deferrable=True,
        # If the child DAG is paused, its run would sit in "queued" forever and
        # this task would hang. Fail fast with a clear error instead.
        fail_when_dag_is_paused=True,
        execution_timeout=timedelta(minutes=30),
    )

    end = EmptyOperator(task_id="end")

    start >> call_child >> end


# Child DAG: no schedule, so it only runs when the parent calls it.
# It must be unpaused, otherwise the triggered run is never started.
@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    is_paused_upon_creation=False,
    tags=["demo"],
)
def demo_child_dag():
    EmptyOperator(task_id="do_work")


demo_parent_dag()
demo_child_dag()
