"""
Example 2: Three DAGs that run one after another.

    demo_dag_1  ->  demo_dag_2  ->  demo_dag_3

When a DAG finishes, it marks an Asset as updated.
The next DAG is scheduled on that Asset, so it starts automatically.

To run the chain, trigger demo_dag_1. All three DAGs must be unpaused;
a paused DAG ignores the Asset update and the chain stops there.
"""

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, dag
from pendulum import datetime

# "Done" signals passed between the DAGs.
dag_1_done = Asset("dag_1_done")
dag_2_done = Asset("dag_2_done")


# DAG 1: starts the chain.
@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    is_paused_upon_creation=False,
    tags=["demo"],
)
def demo_dag_1():
    EmptyOperator(task_id="do_work", outlets=[dag_1_done])


# DAG 2: runs after DAG 1 finishes.
@dag(
    start_date=datetime(2025, 1, 1),
    schedule=[dag_1_done],
    is_paused_upon_creation=False,
    tags=["demo"],
)
def demo_dag_2():
    EmptyOperator(task_id="do_work", outlets=[dag_2_done])


# DAG 3: runs after DAG 2 finishes.
@dag(
    start_date=datetime(2025, 1, 1),
    schedule=[dag_2_done],
    is_paused_upon_creation=False,
    tags=["demo"],
)
def demo_dag_3():
    EmptyOperator(task_id="do_work")


demo_dag_1()
demo_dag_2()
demo_dag_3()
