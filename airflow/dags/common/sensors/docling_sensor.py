from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import BaseSensorOperator, Context
from airflow.triggers.base import BaseTrigger, TriggerEvent
from asgiref.sync import sync_to_async


class DoclingBatchTrigger(BaseTrigger):
    """
    Deferrable trigger that polls Docling-serve until all tasks in a batch complete.

    Runs inside the Airflow triggerer process. Yields a single
    ``TriggerEvent`` when every task ID reaches a terminal status
    (``success`` / ``partial_success`` / ``skipped``), or immediately on
    the first ``failure`` result.

    Mirror of ``MineruBatchTrigger`` — uses the same concurrency pattern:
    ``asyncio.gather`` polls all task IDs in the batch in parallel.

    Parameters
    ----------
    task_ids : list of str
        Docling-serve task IDs to monitor.
    conn_id : str, optional
        Airflow connection ID for the Docling API. Default is ``"docling"``.
    poll_interval : int, optional
        Seconds to sleep between polling rounds. Default is ``60``.
    """

    def __init__(
        self,
        task_ids: list[str],
        conn_id: str = "docling",
        poll_interval: int = 60,
    ):
        super().__init__()
        self.task_ids = task_ids
        self.conn_id = conn_id
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialise the trigger so the triggerer process can reconstruct it.

        Returns
        -------
        tuple of (str, dict)
            Fully-qualified class path and a dict of constructor kwargs.
        """
        return (
            "common.sensors.docling_sensor.DoclingBatchTrigger",
            {
                "task_ids": self.task_ids,
                "conn_id": self.conn_id,
                "poll_interval": self.poll_interval,
            },
        )

    @sync_to_async
    def _check_status(self, task_id: str) -> dict:
        """
        Fetch the current status of a single Docling task (sync → async bridge).

        Wraps a synchronous ``HttpHook`` call with ``sync_to_async`` so it
        can be awaited inside ``asyncio.gather``.

        Parameters
        ----------
        task_id : str
            Docling task ID to query via ``GET /v1/status/poll/{task_id}``.

        Returns
        -------
        dict
            Parsed JSON response containing at minimum ``task_status`` and
            optionally ``errors``.
        """
        hook = HttpHook(method="GET", http_conn_id=self.conn_id)
        resp = hook.run(f"/v1/status/poll/{task_id}")
        self.log.info(f">->- Resp info{resp.json()}")
        return resp.json()

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Poll all task IDs concurrently until the batch is done or fails.

        Runs ``_check_status`` for every pending task in parallel via
        ``asyncio.gather``. Tasks that reach a terminal status are removed
        from the pending list. A single ``failure`` immediately yields a
        failure event and stops iteration. When all tasks complete, yields
        one success event carrying ``task_ids``.

        Yields
        ------
        TriggerEvent
            ``{"status": "completed", "task_ids": [...]}`` on full success,
            or ``{"status": "failed", "error": "..."}`` on the first failure.
        """
        pending = list(self.task_ids)
        self.log.info(f">->- pending {pending}")
        while pending:
            checks = await asyncio.gather(
                *[self._check_status(tid) for tid in pending],
                return_exceptions=True,
            )

            for task_id, result in zip(list(pending), checks):
                self.log.info(f">->- result111{result}")
                if isinstance(result, Exception):
                    self.log.error(f"----- Polling error for {task_id}: {result}")
                    continue

                status = result.get("task_status")

                if status == "failure":
                    error = result.get("errors", "unknown")
                    yield TriggerEvent(
                        {
                            "status": "failed",
                            "error": f"Docling task {task_id} failed: {error}",
                        }
                    )
                    return

                if status in ("success", "partial_success", "skipped"):
                    pending.remove(task_id)
                    self.log.info(
                        f"----- Batch task completed: {task_id}, erros {result.get('errors', '')}"
                    )

            if pending:
                self.log.info(
                    f"----- Waiting for {len(pending)}/{len(self.task_ids)} Docling tasks. Sleeping {self.poll_interval}s."
                )
                await asyncio.sleep(self.poll_interval)

        self.log.info(f"----- Batch fully completed: {self.task_ids}")
        yield TriggerEvent(
            {
                "status": "completed",
                "task_ids": self.task_ids,
            }
        )


class DoclingBatchStatusSensor(BaseSensorOperator):
    """
    Sensor that waits for a batch of Docling-serve tasks to reach terminal status.

    Mirror of ``MineruBatchStatusSensor``. In deferrable mode (default) the
    operator suspends itself via ``DoclingBatchTrigger``, freeing the worker
    slot while polling runs inside the triggerer process. A non-deferrable
    fallback is provided for environments without a triggerer.

    Returns ``list[str]`` of completed task IDs → passed to
    ``save_docling_results`` via XCom.

    Parameters
    ----------
    external_task_ids : list of str
        Docling-serve task IDs to monitor (``/v1/status/poll/{id}``).
    docling_conn_id : str, optional
        Airflow connection ID for the Docling API. Default is ``"docling"``.
    poll_interval : int, optional
        Seconds between status checks. Default is ``60``.
    deferrable : bool, optional
        Use the async deferrable path when ``True`` (default).
        Set to ``False`` to fall back to synchronous polling.
    **kwargs
        Passed through to ``BaseSensorOperator``.
    """

    ui_color = "#a3d9ff"

    template_fields = ("external_task_ids", "poll_interval")

    def __init__(
        self,
        *,
        external_task_ids: list[str],
        docling_conn_id: str = "docling",
        poll_interval: int = 60,
        deferrable: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.external_task_ids = external_task_ids
        self.docling_conn_id = docling_conn_id
        self.poll_interval = poll_interval
        self.deferrable = deferrable

    def execute(self, context: Context):
        """
        Start monitoring the batch, deferring or blocking based on ``deferrable``.

        In deferrable mode, suspends the task via ``DoclingBatchTrigger`` and
        resumes at ``execute_complete`` once the trigger fires. In fallback
        mode, synchronously polls ``GET /v1/status/poll/{id}`` in a loop.

        Parameters
        ----------
        context : Context
            Airflow task execution context.

        Returns
        -------
        list of str
            ``external_task_ids`` (fallback mode only; deferrable mode
            returns via ``execute_complete``).

        Raises
        ------
        RuntimeError
            If any task reports ``failure`` status (fallback mode).
        """
        if self.deferrable:
            self.log.info(
                f"----- Deferring DoclingBatchTrigger for {len(self.external_task_ids)} tasks"
            )
            self.defer(
                trigger=DoclingBatchTrigger(
                    task_ids=self.external_task_ids,
                    conn_id=self.docling_conn_id,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )
        else:
            import time

            hook = HttpHook(method="GET", http_conn_id=self.docling_conn_id)
            pending = list(self.external_task_ids)
            while pending:
                for task_id in pending:
                    resp = hook.run(f"//v1/status/poll/{task_id}")
                    status = resp.json().get("task_status")

                    if status in ("success", "partial_success", "skipped"):
                        pending.remove(task_id)
                    elif status == "failure":
                        raise RuntimeError(f"Docling task {task_id} failed")

                if pending:
                    time.sleep(self.poll_interval)
            return self.external_task_ids

    def execute_complete(
        self,
        context: Context,
        event: dict[str, Any] | None = None,
    ) -> list[str]:
        """
        Resume execution after ``DoclingBatchTrigger`` fires.

        Called automatically by Airflow when the deferred trigger yields a
        ``TriggerEvent``. Raises on failure; returns task IDs on success so
        they can be consumed by ``save_docling_results`` via XCom.

        Parameters
        ----------
        context : Context
            Airflow task execution context.
        event : dict or None
            Payload from ``TriggerEvent``. Expected keys:
            ``status`` (``"completed"`` | ``"failed"``),
            ``task_ids`` (list of str), ``error`` (str, failure only).

        Returns
        -------
        list of str
            Completed Docling task IDs passed to the next task.

        Raises
        ------
        RuntimeError
            If ``event`` is ``None`` or ``event["status"] == "failed"``.
        """
        if not event or event.get("status") == "failed":
            raise RuntimeError(event.get("error", "Unknown DoclingBatchTrigger error"))

        task_ids = event["task_ids"]
        self.log.info(f"----- DoclingBatch completed: {len(task_ids)} tasks")
        return task_ids
