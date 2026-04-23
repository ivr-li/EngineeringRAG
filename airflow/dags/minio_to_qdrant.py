import logging
from collections import defaultdict

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import dag, task
from pendulum import datetime

MINERU_URL = "http://mineru-api:8000"
RAG_DATA_BUCKET = "ragfiles"
SUP_FORMATS = ["pdf/"]


@dag(
    dag_id="minio_to_qdrant",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data_piline"],
)

# async def fetch_file(url, session):
#     async with session.get(url) as response:
#         file = await response.read()


def minio_to_qdrant():

    @task
    def get_buckets_data(
        bucket_name_filter: str | list[str] | None = None,
    ) -> dict[str, list[str]]:
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
            data = defaultdict(list)

            for name in names:
                prefs = hook.list_prefixes(
                    bucket_name=name,
                    delimiter="/",
                )
                data[name].extend(prefs)

            logging.info(f"Found prefixes {data}")
            return data
        except Exception as ex:
            logging.exception(ex)

    @task
    def get_minio_files(bucket_data: dict[str, list[str]], bucket_name: str):
        hook = S3Hook(aws_conn_id="minio")
        all_urls = []

        try:
            prefixes = bucket_data[bucket_name]
            # if single_key:
            #     return single_key
            if not prefixes:
                logging.info(f"Bucket {bucket_name} is empty")
                return []

            for pref in prefixes:
                if pref not in SUP_FORMATS:
                    continue

                keys = hook.list_keys(bucket_name=bucket_name, prefix=pref)

                if not keys:
                    logging.info(f"No keys found for {bucket_name}/{pref or ''}")
                    continue

                urls = [
                    hook.generate_presigned_url(
                        client_method="get_object",
                        params={"Bucket": bucket_name, "Key": k},
                    )
                    for k in keys
                ]

                all_urls.extend(urls)
                logging.info(
                    f"Generated {len(urls)} URLs for {bucket_name}/{pref or ''}"
                )

            if not all_urls:
                raise TypeError("No files found in supported formats")

            logging.info(f"Total generated {len(all_urls)} URLs for {bucket_name}")

            # Load file data to minio
            temp_key = "temp/minio_urls_temp.txt"
            hook.load_bytes(
                bytes_data="\n".join(urls).encode("utf-8"),
                key=temp_key,
                bucket_name=bucket_name,
                replace=True,
            )

            # hook.load_file_obj(
            #     file_obj=byte, key=temp_key, bucket_name=bucket_name, replace=True
            # )

            logging.info(f"Temp key {temp_key} loaded")
            return temp_key
        except Exception as ex:
            logging.exception(ex)
            return []

    @task
    def mineru_health():
        hook_mineru = HttpHook(method="GET", http_conn_id="mineru")
        # req = requests.get(f"{MINERU_URL}/health")
        req = hook_mineru.run(endpoint="/health")
        return req.status_code

    # Dynamic Task Mapping
    @task
    def single_mineru_process(
        health_status: int,
        temp_file_key: str,
        bucket_name: str,
    ):
        if health_status != 200:
            raise ValueError(f"MinerU unhealthy: {health_status}")

        hook_mineru = HttpHook(method="POST", http_conn_id="mineru")
        hook_minio = S3Hook(aws_conn_id="minio")

        # ===== 0) Read temp_file_key =====
        pocessing_files = hook_minio.read_key(
            key=temp_file_key, bucket_name=bucket_name
        )

        print(pocessing_files)
        # ===== 1) Dowmloar file =====
        # with tempfile.NamedTemporaryFile(delete=False) as tmp:
        #     file_path = hook_minio.download_file(
        #         key=temp_file_key,
        #         bucket_name=bucket_name,
        #         local_path=tmp.name,
        #         preserve_file_name=True,
        #         use_autogenerated_subdir=False,
        #     )

        #     print(f"Downloaded to: {tmp.name}")
        #     tmp.flush()

    # @task
    # def mineru(health_status: int, file_urls: Tuple[str, str]):
    #     if health_status != 200:
    #         raise ValueError(f"MinerU unhealthy: {health_status}")

    #     hook_mineru = HttpHook(method="POST", http_conn_id="mineru")

    #     data = {
    #         "files": file_urls[0],
    #         "return_md": "true",
    #         "table_enable": "true",
    #         "lang_list": "cyrillic",
    #         "backend": "hybrid-auto-engine",
    #         "return_middle_json": "false",
    #         "return_model_output": "false",
    #         "return_images": "false",
    #         "server_url": "string",
    #         "return_content_list": "false",
    #         "response_format_zip": "false",
    #         "return_original_file": "false",
    #         "formula_enable": "true",
    #     }

    #     resp = hook_mineru.run(
    #         endpoint="/file_parse",
    #         data=data,
    #     )

    #     resp_out = resp.json()

    #     if resp_out.get("status", "") == "completed":
    #         file_name = resp_out["file_names"][0]

    #         return resp_out["results"][file_name]["md_content"]
    #     else:
    #         return resp_out
    mineru_health = mineru_health()

    buckets_data = get_buckets_data()
    files_task = get_minio_files(bucket_data=buckets_data, bucket_name=RAG_DATA_BUCKET)
    single_mineru_process(
        health_status=mineru_health,
        temp_file_key=files_task,
        bucket_name=RAG_DATA_BUCKET,
    )
    # mineru(health_status=mineru_health, file_urls=files_task)


minio_to_qdrant()
