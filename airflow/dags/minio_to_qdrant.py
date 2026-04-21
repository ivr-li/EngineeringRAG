import logging
from collections import defaultdict

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    dag_id="minio_to_qdrant",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data_piline"],
)
def minio_to_qdrant():

    @task
    def get_buckets_data(bucket_name_filter: str | list[str] | None = None):
        hook = S3Hook(aws_conn_id="minio")

        try:
            buckets_data = hook.get_conn().list_buckets()

            buckets = buckets_data.get("Buckets", [])

            if bucket_name_filter:
                """
                "Buckets": [
                    {
                        "Name": "...",
                        "CreationDate": "..."
                    }, ...
                ],
                """
                target = (
                    [bucket_name_filter]
                    if isinstance(bucket_name_filter, str)
                    else bucket_name_filter
                )
                names = [b["Name"] for b in buckets if b["Name"] in target]
            else:
                names = [b["Name"] for b in buckets]

            logging.info(f"Found buckets: {names}")

            all_prefs = defaultdict(list)

            for name in names:
                prefs = hook.list_prefixes(
                    bucket_name=name,
                    delimiter="/",
                )

                all_prefs[name].extend(prefs)

            logging.info(f"Found prefixes {all_prefs}")

            return all_prefs
        except Exception as ex:
            logging.exception(ex)

    @task
    def get_minio_files(bucket_name: str, prefix: str | None = None):
        hook = S3Hook(aws_conn_id="minio")

        keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        if not keys:
            return []

        try:
            urls = [
                hook.generate_presigned_url(
                    client_method="get_object", params={"Bucket": bucket_name, "Key": k}
                )
                for k in keys
            ]

            logging.info(f"Generated {len(urls)} URLs")

            return urls
        except Exception as ex:
            logging.exception(ex)

    get_buckets_data() >> get_minio_files("ragfiles", prefix="pdf/")


minio_to_qdrant()
