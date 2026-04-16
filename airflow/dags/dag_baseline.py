from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

args = {
    "owner": "dved",
    "catchup": True,
    "retries": 3,
    "retry_delay": timedelta(hours=1),
    "max_active_runs": 1,
}


@dag(
    dag_id="revt_etl_baseline",
    default_args=args,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
)
def revt_etl_baseline():
    @task
    def print_something():
        print("something")

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")


revt_etl_baseline()
