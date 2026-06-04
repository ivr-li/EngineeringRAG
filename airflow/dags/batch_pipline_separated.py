from __future__ import annotations

import json
import logging
import uuid
from collections import defaultdict
from functools import lru_cache
from pathlib import Path

from airflow.sdk import dag, task
from pendulum import datetime

RAG_DATA_BUCKET = "ragfiles"
DEV_DATA = "dev_data"
DEV_DATA_MINERU_MD = f"{DEV_DATA}/mineru_md"
DEV_DATA_DOCLING_JSON = f"{DEV_DATA}/docling_jsons"
SUP_FORMATS = ["pdf/"]
BATCH_SIZE = 16

QDRANT_URL = "http://qdrant:6333"
QDRANT_COLLECTION = "construction_docs"
BGE_M3_MODEL = "BAAI/bge-m3"
QDRANT_VECTOR_SIZE = 1024
QDRANT_ENCODE_BATCH = 16
QDRANT_UPSERT_BATCH = 32
QDRANT_COLBERT_SIZE = 1024


# ============================================================
# Helpers
# ============================================================


def del_file(file: str | Path) -> None:
    """Delete a local file if it exists"""
    path = Path(file)
    if path.exists():
        path.unlink()
        logging.info(f">>> Removed existing: {path}")


def load_to_s3(
    hook: object,
    filepath: str | list[str],
    bucket_name: str = RAG_DATA_BUCKET,
    prefix: str = DEV_DATA_MINERU_MD,
) -> list[str] | None:
    filepath = filepath if isinstance(filepath, list) else [filepath]
    out = ""
    s3_keys = []
    try:
        for patch in filepath:
            s3_key = f"{DEV_DATA_MINERU_MD}/{Path(patch).name}"
            hook.load_file(
                filename=patch,
                bucket_name=bucket_name,
                key=f"{prefix}/{patch.split('/')[-1]}",
                replace=True,
            )
            out += f"\n{patch}"
            s3_keys.append(s3_key)
        logging.info(f">> File loaded to minio: {out}")
        return s3_keys
    except Exception as ex:
        logging.error(f">> Unknown error loading data to minio: {ex}")


def batch_list(items: list, batch_size: int = BATCH_SIZE) -> list[list]:
    """Split a flat list into fixed-size chunks"""
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


@lru_cache(maxsize=1)
def get_bge_m3():
    import torch
    from FlagEmbedding import BGEM3FlagModel

    return BGEM3FlagModel(
        BGE_M3_MODEL,
        use_fp16=torch.cuda.is_available(),  # fp16 for gpu, fp32 for cpu
        device="cuda" if torch.cuda.is_available() else "cpu",
    )


# ============================================================
# DAG
# ============================================================


def build_batch_pipeline(dag_id: str, mode: str):
    """Batch document processing pipeline — see module docstring for full description."""

    @dag(
        dag_id=dag_id,
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["data_pipeline", mode],
        doc_md=__doc__,
    )
    def batch_pipeline():

        # ------------------------------------------------------------------------------------------------
        # 1. Discovery
        # ------------------------------------------------------------------------------------------------

        @task
        def get_buckets_data(
            bucket_name_filter: str | list[str] | None = None,
        ) -> dict[str, list[str]]:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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
                    prefs = hook.list_prefixes(bucket_name=name, delimiter="/")
                    data[name].extend(prefs)

                logging.info(f"Found prefixes {data}")
                return data
            except Exception as ex:
                logging.exception(ex)
                return []

        @task
        def list_files_to_process(
            bucket: str,
            prefix: str | None = None,
            skip_process: bool = True,
        ) -> list[str]:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            hook = S3Hook(aws_conn_id="minio")

            if prefix:
                candidates = hook.list_keys(bucket_name=bucket, prefix=prefix) or []
            else:
                candidates = []
                for pref in hook.list_prefixes(bucket_name=bucket, delimiter="/"):
                    if pref in SUP_FORMATS:
                        candidates.extend(
                            hook.list_keys(bucket_name=bucket, prefix=pref) or []
                        )

            if not skip_process:
                return candidates

            to_process = []
            for key in candidates:
                stem = Path(key).stem
                md_key = f"{DEV_DATA_MINERU_MD}/{stem}.md"

                if hook.check_for_key(key=md_key, bucket_name=bucket):
                    logging.info(
                        f">>> Skipping already processed: {key} (found {md_key})"
                    )
                else:
                    to_process.append(key)

            logging.info(f">>> {len(candidates)} total, {len(to_process)} to process")
            return to_process

        @task
        def check_mineru_health() -> int:
            """Verify that the MinerU API is reachable and healthy"""
            from airflow.providers.http.hooks.http import HttpHook

            hook_mineru = HttpHook(method="GET", http_conn_id="mineru")
            req = hook_mineru.run(endpoint="/health")
            return req.status_code

        # ------------------------------------------------------------------------------------------------
        # 2. Batching
        # ------------------------------------------------------------------------------------------------

        @task
        def create_file_batches(
            files: list[str],
            batch_size: int = BATCH_SIZE,
        ) -> list[list[str]]:
            batches = batch_list(files, batch_size)
            logging.info(f">>> {len(files)} files split into {len(batches)} batches")

            for i, batch in enumerate(batches):
                logging.info(f"    Batch {i}: {len(batch)} files -> {batch}")
            return batches

        # ------------------------------------------------------------------------------------------------
        # 3. Submit batch to MinerU
        # ------------------------------------------------------------------------------------------------

        @task()
        def batch_mineru_submit(
            health_status: int,
            qerant_status: str,
            file_keys: list[str],
            bucket_name: str,
        ) -> list[str]:
            import requests
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            if health_status != 200:
                raise ValueError(f"MinerU unhealthy: {health_status}")

            if not qerant_status:
                raise ValueError("Qdrant unhealthy")

            hook_minio = S3Hook(aws_conn_id="minio")
            task_ids: list[str] = []

            for file_key in file_keys:
                # ----- download -----
                file_name = Path("/tmp") / file_key.split("/")[-1]
                del_file(file_name)

                file_path = hook_minio.download_file(
                    key=file_key,
                    bucket_name=bucket_name,
                    local_path="/tmp",
                    preserve_file_name=True,
                    use_autogenerated_subdir=False,
                )
                logging.info(f">>> Downloaded: {file_path}")

                # ----- submit -----
                file_size = Path(file_path).stat().st_size
                logging.info(
                    f">>> Submitting file to MinerU: {file_key} (size: {file_size} bytes)"
                )

                with open(file_path, "rb") as f:
                    resp = requests.post(
                        "http://mineru-api:8000/tasks",
                        files={"files": f},
                        data={
                            "lang_list": "east_slavic",
                            "backend": "pipeline",
                            "parse_method": "ocr",
                            "formula_enable": "true",
                            "table_enable": "true",
                            "return_md": "true",
                            "return_middle_json": "false",
                            "return_model_output": "false",
                            "return_content_list": "false",
                            "return_images": "false",
                        },
                        timeout=120,
                    )

                logging.info(f">>> MinerU response status: {resp.status_code}")
                logging.info(f">>> MinerU response headers: {dict(resp.headers)}")

                resp.raise_for_status()
                response_json = resp.json()
                logging.info(
                    f">>> MinerU full response: {json.dumps(response_json, ensure_ascii=False)}"
                )

                tid = response_json.get("task_id")
                if not tid:
                    raise ValueError(f"Task ID not found in response: {response_json}")
                task_ids.append(tid)
                logging.info(f">>> Submitted {file_key} -> task_id: {tid}")

                del_file(file_path)

            logging.info(f">>> Batch submitted: {task_ids}")
            return task_ids

        # ------------------------------------------------------------------------------------------------
        # 4. Save MinerU results
        # ------------------------------------------------------------------------------------------------

        @task()
        def save_mineru_results(mineru_task_ids: list[str]) -> list[str]:
            from airflow.providers.http.hooks.http import HttpHook

            logging.info(
                f">>> save_mineru_results() received {len(mineru_task_ids)} task_ids"
            )
            logging.info(f">>> Task IDs: {mineru_task_ids}")

            hook_mineru = HttpHook(method="GET", http_conn_id="mineru")
            all_paths: list[str] = []

            for task_id in mineru_task_ids:
                logging.info(f">>> Fetching result for task_id: {task_id}")
                req = hook_mineru.run(endpoint=f"/tasks/{task_id}/result")
                logging.info(f">>> Result response status: {req.status_code}")
                req.raise_for_status()
                results = req.json().get("results", {})
                logging.info(
                    f">>> Result response: {json.dumps(results, ensure_ascii=False)[:500]}..."
                )

                for name, payload in results.items():
                    file_path = f"/tmp/{name}.md"
                    del_file(file_path)

                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(payload["md_content"])
                    all_paths.append(file_path)
                    logging.info(f">>> Saved result to: {file_path}")

            logging.info(f">>> Saved {len(all_paths)} .md files from batch")
            return all_paths

        # ------------------------------------------------------------------------------------------------
        # 5. Upload .md to MinIO
        # ------------------------------------------------------------------------------------------------

        @task
        def load_md_to_minio(mineru_result: list[str]) -> list[str]:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            hook = S3Hook(aws_conn_id="minio")
            s3_keys = load_to_s3(hook, mineru_result, prefix=DEV_DATA_MINERU_MD)
            return s3_keys

        # ------------------------------------------------------------------------------------------------
        # 6. Chanking
        # ------------------------------------------------------------------------------------------------
        @task()
        def single_docling():
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            hook_minio = S3Hook(aws_conn_id="minio")

            keys = (
                hook_minio.list_keys(
                    bucket_name=RAG_DATA_BUCKET, prefix=DEV_DATA_MINERU_MD
                )
                or []
            )
            return [keys[i : i + BATCH_SIZE] for i in range(0, len(keys), BATCH_SIZE)]

        @task()
        def docling_chunk_submit(
            mineru_result: list[str],
            bucket_name: str,
        ) -> list[str]:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from airflow.providers.http.hooks.http import HttpHook
            from common.txt_feature.cleaner import attach_table_captions, strip_watermarks
            from common.txt_feature.table_repair import (
                expand_tables_for_docling,
                repair_split_tables,
            )

            chunk_task_ids: list[str] = []
            hook_docling = HttpHook(method="POST", http_conn_id="docling")
            hook_minio = S3Hook(aws_conn_id="minio")

            for filepath in mineru_result:
                file_name = Path("/tmp") / filepath.split("/")[-1]
                del_file(file_name)

                file_path = hook_minio.download_file(
                    key=filepath,
                    bucket_name=bucket_name,
                    local_path="/tmp",
                    preserve_file_name=True,
                    use_autogenerated_subdir=False,
                )
                logging.info(f">>> Downloaded: {file_path}")
                with open(file_path, "r", encoding="utf-8") as f:
                    raw_md = f.read()

                cleaned_md = strip_watermarks(raw_md)
                cleaned_md = attach_table_captions(cleaned_md)
                cleaned_md = repair_split_tables(cleaned_md)
                cleaned_md = expand_tables_for_docling(cleaned_md)

                if cleaned_md != raw_md:
                    logging.info(f">>> Normalized markdown before Docling: {file_path}")
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(cleaned_md)

                path = Path(file_path)
                with open(file_path, "rb") as f:
                    resp = hook_docling.run(
                        endpoint="/v1/chunk/hierarchical/file/async",
                        files={
                            "files": (
                                path.name,
                                f,
                                "text/markdown",
                            )
                        },
                        data={
                            "convert_from_formats": "json_docling",
                            "convert_do_ocr": "false",
                            "convert_do_table_structure": "false",
                            "convert_include_images": "false",
                            "convert_do_formula_enrichment": "false",
                            "target_type": "inbody",
                            "include_converted_doc": "false",
                            "chunking_use_markdown_tables": "true",
                            "chunking_include_raw_text": "false",
                        },
                    )
                    resp.raise_for_status()
                    task_id = resp.json()["task_id"]
                    chunk_task_ids.append(task_id)
                    logging.info(f">>> Chunk submit: {path.name} -> task_id: {task_id}")

            logging.info(f">>> Chunk batch submitted: {chunk_task_ids}")

            return chunk_task_ids

        @task
        def save_docling_results(docling_task_ids: list[str]) -> list[str]:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from airflow.providers.http.hooks.http import HttpHook
            from common.txt_feature.cleaner import process_chunks

            hook_docling = HttpHook(method="GET", http_conn_id="docling")
            hook_minio = S3Hook(aws_conn_id="minio")
            out_paths: list[str] = []

            for task_id in docling_task_ids:
                resp = hook_docling.run(endpoint=f"/v1/result/{task_id}")
                resp.raise_for_status()
                result = resp.json()

                all_chunks = result.get("chunks", [])
                documents = result.get("documents", [])

                if not documents:
                    logging.warning(f">>> No documents for task_id={task_id}. Skip")
                    continue

                for doc in documents:
                    file_name = doc.get("content", {}).get("filename")
                    stem = Path(file_name).stem if "." in file_name else file_name

                    doc_chunks = [
                        chunk
                        for chunk in all_chunks
                        if chunk.get("meta", {}).get("origin", {}).get("filename")
                        == file_name
                    ]
                    if not doc_chunks and len(documents) == 1:
                        doc_chunks = all_chunks

                    if not doc_chunks:
                        logging.warning(f">>> No chunks for {file_name}. Skip")
                        continue

                    text_chunks = [c for c in doc_chunks if c.get("type") != "table"]
                    table_chunks = [c for c in doc_chunks if c.get("type") == "table"]

                    for tc in table_chunks:
                        tc["is_table"] = True
                        # Префикс помогает модели понять контекст при энкодинге
                        tc["text"] = f"[ТАБЛИЦА] {tc.get('text', '')}"

                    all_enriched = process_chunks(text_chunks + table_chunks)

                    json_path = f"/tmp/{stem}.json"
                    del_file(json_path)
                    with open(json_path, "w", encoding="utf-8") as jf:
                        json.dump(all_enriched, jf, ensure_ascii=False, indent=2)

                    load_to_s3(
                        hook=hook_minio, filepath=json_path, prefix=DEV_DATA_DOCLING_JSON
                    )
                    out_paths.append(json_path)
                    logging.info(
                        f">>> Saved {len(all_enriched)} chunks (was {len(doc_chunks)}) -> {json_path}"
                    )

            return out_paths

        @task()
        def create_qdrant_collection() -> str:
            from qdrant_client import QdrantClient
            from qdrant_client.models import (
                Distance,
                HnswConfigDiff,
                Modifier,
                MultiVectorComparator,
                MultiVectorConfig,
                PayloadSchemaType,
                SparseVectorParams,
                VectorParams,
            )

            client = QdrantClient(url=QDRANT_URL, timeout=20)

            existing = {c.name for c in client.get_collections().collections}
            if QDRANT_COLLECTION not in existing:
                client.create_collection(
                    collection_name=QDRANT_COLLECTION,
                    vectors_config={
                        "dense": VectorParams(
                            size=QDRANT_VECTOR_SIZE,  # 1024
                            distance=Distance.COSINE,
                        ),
                        "colbert": VectorParams(
                            size=QDRANT_COLBERT_SIZE,  # 1024
                            distance=Distance.COSINE,
                            multivector_config=MultiVectorConfig(
                                comparator=MultiVectorComparator.MAX_SIM,
                            ),
                            hnsw_config=HnswConfigDiff(m=0),
                        ),
                    },
                    sparse_vectors_config={
                        "sparse": SparseVectorParams(modifier=Modifier.IDF),
                    },
                )
                logging.info(f">>> Collection '{QDRANT_COLLECTION}' created")
            else:
                logging.info(
                    f">>> Collection '{QDRANT_COLLECTION}' already exists — adding missing indices"
                )

            # Payload indices
            indices: list[tuple[str, PayloadSchemaType]] = [
                ("filename", PayloadSchemaType.TEXT),
                ("headings", PayloadSchemaType.KEYWORD),
                ("is_table", PayloadSchemaType.BOOL),
                ("man_refs", PayloadSchemaType.KEYWORD),
                ("cross_refs", PayloadSchemaType.KEYWORD),
                ("anchor_refs", PayloadSchemaType.KEYWORD),
                # hierarchy metadata
                ("section_level", PayloadSchemaType.INTEGER),
                ("section_path", PayloadSchemaType.KEYWORD),
                ("parent_heading", PayloadSchemaType.KEYWORD),
                ("leaf_heading", PayloadSchemaType.KEYWORD),
                # sliding-window markers
                ("is_overlap_window", PayloadSchemaType.BOOL),
                ("window_index", PayloadSchemaType.INTEGER),
                # table continuation metadata
                ("table_id", PayloadSchemaType.KEYWORD),
                ("table_caption", PayloadSchemaType.TEXT),
                ("table_part_index", PayloadSchemaType.INTEGER),
                ("table_part_total", PayloadSchemaType.INTEGER),
                ("table_window_index", PayloadSchemaType.INTEGER),
                ("table_window_total", PayloadSchemaType.INTEGER),
                ("table_orientation", PayloadSchemaType.KEYWORD),
            ]

            for field, schema in indices:
                try:
                    client.create_payload_index(QDRANT_COLLECTION, field, schema)
                    logging.info(f">>>   index ok: {field} ({schema.value})")
                except Exception as ex:
                    logging.debug(f">>>   index skip: {field} — {ex}")

            return QDRANT_COLLECTION

        @task()
        def save_to_qdrant(docling_json_paths: list[str]) -> int:
            import gc

            import torch
            from qdrant_client import QdrantClient
            from qdrant_client.models import PointStruct, SparseVector

            # Диагностика GPU
            providers = (
                ["CUDAExecutionProvider"]
                if torch.cuda.is_available()
                else ["CPUExecutionProvider"]
            )
            logging.info(f">>> PROVIDERS: {providers}")
            logging.info(f">>> CUDA available: {torch.cuda.is_available()}")
            logging.info(f">>> PROVIDERS: {providers}")
            if torch.cuda.is_available():
                logging.info(f">>> GPU: {torch.cuda.get_device_name(0)}")

            client = QdrantClient(
                url=QDRANT_URL,
                timeout=120,
            )
            model = get_bge_m3()
            total_upserted = 0

            for json_path in docling_json_paths:
                path = Path(json_path)
                with open(json_path, encoding="utf-8") as f:
                    chunks = json.load(f)

                if not chunks:
                    logging.warning(f">>> Empty chunk list in {json_path}, skip")
                    continue

                logging.info(f">>> Processing {len(chunks)} chunks from {path.name}")

                # -----Iterate in QDRANT_ENCODE_BATCH sized windows -----
                for enc_start in range(0, len(chunks), QDRANT_ENCODE_BATCH):
                    # ----- 1) Batching and Cheaning text into chanks -----
                    enc_batch = chunks[enc_start : enc_start + QDRANT_ENCODE_BATCH]

                    valid_chunks = []
                    texts = []
                    for c in enc_batch:
                        text = c.get("text") if isinstance(c, dict) else None
                        if isinstance(text, str) and text.strip():
                            valid_chunks.append(c)
                            texts.append(text.strip())

                    if not texts:
                        logging.warning(
                            f">>> No valid text payloads in file={path.name}, enc_offset={enc_start}; skip"
                        )
                        continue
                    # ----- 2) Vectors -----
                    output = model.encode(
                        texts,
                        batch_size=QDRANT_ENCODE_BATCH,
                        max_length=400,
                        return_dense=True,
                        return_sparse=True,
                        return_colbert_vecs=True,
                    )
                    dense_vecs = output["dense_vecs"]  # ndarray (B, 1024)
                    lexical_vecs = output[
                        "lexical_weights"
                    ]  # list[dict[token_id → weight]]
                    colbert_vecs = output["colbert_vecs"]  # list[ndarray(n_tok, 1024)]

                    # ----- 3) Build PointStruct list -----
                    points: list[PointStruct] = []
                    for i, chunk in enumerate(enc_batch):
                        # Deterministic UUID from filename + chunk_index so re-runs
                        # overwrite the same point instead of creating duplicates.
                        point_id = str(
                            uuid.uuid5(
                                uuid.NAMESPACE_URL,
                                f"{path.stem}:{chunk.get('chunk_index', enc_start + i)}",
                            )
                        )

                        lw = lexical_vecs[i]  # dict {token_id_str: weight}
                        sparse_indices = [int(k) for k in lw.keys()]
                        sparse_values = [float(v) for v in lw.values()]
                        points.append(
                            PointStruct(
                                id=point_id,
                                vector={
                                    "dense": dense_vecs[i].tolist(),
                                    "sparse": SparseVector(
                                        indices=sparse_indices, values=sparse_values
                                    ),
                                    "colbert": colbert_vecs[i].tolist(),
                                },
                                payload={
                                    # ----- Core -----
                                    "text": chunk.get("text", ""),
                                    "filename": chunk.get("filename", path.stem),
                                    "chunk_index": chunk.get("chunk_index"),
                                    "num_tokens": chunk.get("num_tokens", 0),
                                    "is_table": chunk.get("is_table", False),
                                    # -----References -----
                                    "man_refs": chunk.get("man_refs", []),
                                    "cross_refs": chunk.get("cross_refs", []),
                                    "anchor_refs": chunk.get("anchor_refs", []),
                                    # ----- Hierarchy -----
                                    "headings": chunk.get("headings", []),
                                    "doc_items": chunk.get("doc_items", []),
                                    # ----- Hierarchy (enrich_metadata fields)-----
                                    "section_level": chunk.get("section_level"),
                                    "section_path": chunk.get("section_path", ""),
                                    "parent_heading": chunk.get("parent_heading"),
                                    "leaf_heading": chunk.get("leaf_heading"),
                                    # ----- Sliding window markers -----
                                    "is_overlap_window": chunk.get(
                                        "is_overlap_window", False
                                    ),
                                    "window_index": chunk.get("window_index", 0),
                                    # ----- Table continuation markers -----
                                    "table_id": chunk.get("table_id"),
                                    "table_caption": chunk.get("table_caption"),
                                    "table_part_index": chunk.get("table_part_index"),
                                    "table_part_total": chunk.get("table_part_total"),
                                    "table_window_index": chunk.get("table_window_index"),
                                    "table_window_total": chunk.get("table_window_total"),
                                    "table_orientation": chunk.get("table_orientation"),
                                },
                            )
                        )

                    # ----- Upsert in sub-batches to control request size -----
                    for ups_start in range(0, len(points), QDRANT_UPSERT_BATCH):
                        sub = points[ups_start : ups_start + QDRANT_UPSERT_BATCH]
                        client.upsert(collection_name=QDRANT_COLLECTION, points=sub)
                        total_upserted += len(sub)
                        logging.info(
                            f">>> Upserted {len(sub)} points(file={path.name},"
                            "enc_offset={enc_start},"
                            "ups_offset={ups_start})"
                        )
                    del dense_vecs, lexical_vecs, colbert_vecs, points, enc_batch
                    gc.collect()
                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()

                logging.info(f">>> Done: {path.name}, running total={total_upserted}")

            return total_upserted

        # ==========================================================
        # Graph
        # ==========================================================
        def build_docling_graph() -> None:
            from common.sensors.docling_sensor import DoclingBatchStatusSensor

            md_loaded = single_docling()
            docling_task_ids = docling_chunk_submit.partial(
                bucket_name=RAG_DATA_BUCKET
            ).expand(mineru_result=md_loaded)
            docling_wait = DoclingBatchStatusSensor.partial(
                task_id="wait_docling_batch",
                docling_conn_id="docling",
                poll_interval=10,
            ).expand(external_task_ids=docling_task_ids)
            docling_chunks = save_docling_results.expand(
                docling_task_ids=docling_wait.output
            )
            qdrant_collection = create_qdrant_collection()
            qdrant_collection >> save_to_qdrant.expand(docling_json_paths=docling_chunks)

        def build_full_graph() -> None:
            from common.sensors.docling_sensor import DoclingBatchStatusSensor
            from common.sensors.mineru_sensor import MineruBatchStatusSensor

            # ------------------------------------------------------------
            # ----- 1) Discovery -----
            get_buckets_data()
            mineru_health = check_mineru_health()
            qdrant_health = create_qdrant_collection()
            files_task = list_files_to_process(bucket=RAG_DATA_BUCKET)
            # ----- 2) Split into batches of BATCH_SIZE -----
            file_batches = create_file_batches(files_task)
            # [["f1","f2",...,"f8"], ["f9","f10",...], ...]

            # ----- 3) Submit each batch to MinerU (one dynamic task per batch) -----
            mineru_task_ids = batch_mineru_submit.partial(
                bucket_name=RAG_DATA_BUCKET,
                health_status=mineru_health,
                qerant_status=qdrant_health,
            ).expand(file_keys=file_batches)
            # [["tid1","tid2",...], ["tid9",...], ...]

            # ----- 4) Deferrable sensor - async wait for each batch (frees workers) -----
            mineru_wait = MineruBatchStatusSensor.partial(
                task_id="wait_mineru_batch",
                mineru_conn_id="mineru",
                poll_interval=60,
            ).expand(external_task_ids=mineru_task_ids)
            # output: [["tid1","tid2",...], ["tid9",...], ...]

            # ----- 5) Save .md files per batch -----
            md_files = save_mineru_results.expand(mineru_task_ids=mineru_wait.output)

            # ----- 6) Upload .md to MinIO per batch -----
            md_loaded = load_md_to_minio.expand(mineru_result=md_files)

            # ----- 7) Chanking -----
            docling_task_ids = docling_chunk_submit.partial(
                bucket_name=RAG_DATA_BUCKET
            ).expand(mineru_result=md_loaded)

            # ----- 8) Deferrable sensor - async wait for each batch (frees workers) -----
            docling_wait = DoclingBatchStatusSensor.partial(
                task_id="wait_docling_batch",
                docling_conn_id="docling",
                poll_interval=30,
            ).expand(external_task_ids=docling_task_ids)

            # ----- 6) Upload .jsom to MinIO per batch -----
            docling_chunks = save_docling_results.expand(
                docling_task_ids=docling_wait.output
            )
            # ----- 8) Qdrant  -----
            save_to_qdrant.expand(docling_json_paths=docling_chunks)

        if mode == "docling_only":
            build_docling_graph()
        elif mode == "full":
            build_full_graph()
        else:
            raise ValueError(f"Unknown pipeline mode: {mode}")

    return batch_pipeline()


batch_pipeline_docling_only = build_batch_pipeline(
    dag_id="batch_pipeline_docling_only",
    mode="docling_only",
)
batch_pipeline_full = build_batch_pipeline(
    dag_id="batch_pipeline_full",
    mode="full",
)
