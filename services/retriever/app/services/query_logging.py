from __future__ import annotations

import asyncio
from io import BytesIO
from pathlib import PurePosixPath
from uuid import UUID

import structlog
from minio import Minio
from sqlalchemy import Column, DateTime, MetaData, Numeric, Table, Text, text
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.concurrency import run_in_threadpool

from app.schemas import QueryTrace

log = structlog.get_logger(__name__)
metadata = MetaData()

query_traces = Table(
    "query_traces",
    metadata,
    Column("query_id", PostgreSQLUUID(as_uuid=True), primary_key=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("query", Text, nullable=False),
    Column("latency_ms", Numeric, nullable=True),
    Column("rewrite_latency_ms", Numeric, nullable=True),
    Column("retrieval_latency_ms", Numeric, nullable=True),
    Column("generation_latency_ms", Numeric, nullable=True),
    # Column("metric_value", Numeric, nullable=True),
)


class TraceLogger:
    def __init__(
        self,
        minio_logger: MinioTraceLogger,
        trace_repository: PGTraceLogger,
    ) -> None:
        self.minio_logger = minio_logger
        self.trace_repository = trace_repository

    async def ensure_storage(self) -> None:
        await asyncio.gather(
            self.minio_logger.ensure_bucket(),
            self.trace_repository.ensure_tables(),
        )

    async def log(self, trace: QueryTrace) -> None:
        results = await asyncio.gather(
            self.minio_logger.log(trace),
            self.trace_repository.log(trace),
            return_exceptions=True,
        )

        self._log_errors(trace.query_id, results)

    def _log_errors(
        self,
        query_id: str,
        results: list[object],
    ) -> None:
        storages = ("minio", "postgres")

        for storage, result in zip(storages, results, strict=True):
            if isinstance(result, Exception):
                log.error(
                    "trace_logging_failed",
                    storage=storage,
                    query_id=query_id,
                    error=str(result),
                )


class MinioTraceLogger:
    def __init__(
        self,
        client: Minio,
        bucket_name: str,
        prefix: str = "retriever-traces",
    ) -> None:
        self.client = client
        self.bucket_name = bucket_name
        self.prefix = prefix.strip("/")

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


class PGTraceLogger:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    async def ensure_tables(self) -> None:
        database_url = self.engine.url.render_as_string(hide_password=True)
        log.info(
            "pg_tables_init_started",
            database_url=database_url,
            tables=list(metadata.tables),
        )

        try:
            async with self.engine.begin() as connection:
                database, schema = (
                    await connection.execute(
                        text("SELECT current_database(), current_schema()")
                    )
                ).one()
                await connection.run_sync(metadata.create_all)
        except Exception:
            log.exception(
                "pg_tables_init_failed",
                database_url=database_url,
            )
            raise

        log.info(
            "pg_tables_init_completed",
            database_url=database_url,
            database=database,
            schema=schema,
            tables=list(metadata.tables),
        )

    async def log(self, trace: QueryTrace) -> None:
        statement = query_traces.insert().values(
            query_id=UUID(trace.query_id),
            created_at=trace.created_at,
            query=trace.query,
            latency_ms=trace.latency_ms,
            rewrite_latency_ms=trace.rewrite_latency_ms,
            retrieval_latency_ms=trace.retrieval_latency_ms,
            generation_latency_ms=trace.generation_latency_ms,
        )

        async with self.engine.begin() as connection:
            await connection.execute(statement)

        log.info(
            "postgres_trace_logged",
            query_id=trace.query_id,
        )
