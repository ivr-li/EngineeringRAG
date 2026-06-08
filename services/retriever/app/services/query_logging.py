from __future__ import annotations

from io import BytesIO
from pathlib import PurePosixPath

from minio import Minio
from minio.error import S3Error
from starlette.concurrency import run_in_threadpool

from app.schemas import QueryTrace


class MinioTraceLogger:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        secure: bool = False,
        prefix: str = "retriever-traces",
    ) -> None:
        self.bucket_name = bucket_name
        self.prefix = prefix.strip("/")
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    async def ensure_bucket(self) -> None:
        exists = await run_in_threadpool(self.client.bucket_exists, self.bucket_name)
        if not exists:
            await run_in_threadpool(self.client.make_bucket, self.bucket_name)

    async def log(self, trace: QueryTrace) -> None:
        payload = trace.model_dump_json(ensure_ascii=False).encode("utf-8")
        object_name = self._build_object_name(trace)

        await run_in_threadpool(
            self.client.put_object,
            self.bucket_name,
            object_name,
            BytesIO(payload),
            len(payload),
            "application/json",
        )

    def _build_object_name(self, trace: QueryTrace) -> str:
        date_path = trace.created_at.strftime("%Y/%m/%d")
        return str(PurePosixPath(self.prefix) / date_path / f"{trace.query_id}.json")
