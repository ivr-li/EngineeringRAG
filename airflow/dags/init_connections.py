from __future__ import annotations

from airflow.decorators import dag, task
from airflow.models import Connection
from pendulum import datetime

from airflow import settings

MINIO_CONN_ID = "minio"


@dag(
    dag_id="init_connections",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["init", "bootstrap"],
)
@task
def create_minio_connection():
    session = settings.Session()

    existing = (
        session.query(Connection)
        .filter(Connection.conn_id == MINIO_CONN_ID)
        .one_or_none()
    )
    if existing:
        return f"Connection {MINIO_CONN_ID} already exists"

    conn = Connection(
        conn_id=MINIO_CONN_ID,
        conn_type="aws",
        login="minioadmin",
        password="minioadmin",
        host="http://minio:9000",
        extra='{"endpoint_url": "http://0.0.0.0:9000"}',
    )
    session.add(conn)
    session.commit()
    return f"Connection {MINIO_CONN_ID} created"


def init_connections_dag():
    create_minio_connection()


init_connections_dag()
