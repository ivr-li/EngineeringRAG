import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.qdrant.hooks.qdrant import QdrantHook
from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    dag_id="connect_verify",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data_piline"],
)
def minio_to_qdrant():
    @task
    def verify_minio():
        hook = S3Hook(aws_conn_id="minio")

        buckets = hook.get_conn().list_buckets()
        logging.info(f"Minio buckets found: {len(buckets.get('Buckets'))}")
        return buckets

    @task
    def verify_qdrant():
        hook = QdrantHook(conn_id="qdrant")
        return hook.verify_connection()

    verify_minio() >> verify_qdrant()


minio_to_qdrant()
