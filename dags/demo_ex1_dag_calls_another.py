"""
Example 1: One DAG calls another DAG.

The parent DAG triggers the child DAG and waits for it to finish.
"""

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag
from pendulum import datetime


# Parent DAG: runs start, calls the child DAG, then runs end.
@dag(start_date=datetime(2025, 1, 1), schedule=None, tags=["demo"])
def demo_parent_dag():
    start = EmptyOperator(task_id="start")

    # Call the child DAG and wait until it finishes.
    call_child = TriggerDagRunOperator(
        task_id="call_child",
        trigger_dag_id="demo_child_dag",
        wait_for_completion=True,
    )

    end = EmptyOperator(task_id="end")

    start >> call_child >> end


# Child DAG: no schedule, so it only runs when the parent calls it.
@dag(start_date=datetime(2025, 1, 1), schedule=None, tags=["demo"])
def demo_child_dag():
    EmptyOperator(task_id="do_work")


demo_parent_dag()
demo_child_dag()
