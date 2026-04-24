from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import BaseOperator, BaseSensorOperator, Context  # noqa: F401
from airflow.triggers.base import BaseTrigger, TriggerEvent
from asgiref.sync import sync_to_async


class UnstrictTrigger(BaseTrigger):
    def __init__(
        self,
        task_id: str,
        conn_id: str = "unstructured",
        poll_interval: int = 600,
    ):
        super().__init__()
        self.task_id = task_id
        self.conn_id = conn_id
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "common.sensors.mineru_sensor.UnstrictTrigger",
            {
                "task_id": self.task_id,
                "conn_id": self.conn_id,
                "poll_interval": self.poll_interval,
            },
        )

    @sync_to_async
    def _check_status(self) -> dict:
        hook = HttpHook(method="GET", http_conn_id=self.conn_id)
        resp = hook.run(f"/tasks/{self.task_id}")
        return resp.json()

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            try:
                js = await self._check_status()
                status = js.get("status")

                if status == "completed":
                    yield TriggerEvent(
                        {
                            "status": "completed",
                            "task_id": self.task_id,
                            "result": js.get("result"),
                        }
                    )
                    return

                elif status == "failed":
                    yield TriggerEvent(
                        {
                            "status": "failed",
                            "error": f"Task {self.task_id} failed",
                        }
                    )
                    return

            except Exception as e:
                self.log.error(f"Polling error for {self.task_id}: {e}")

            await asyncio.sleep(self.poll_interval)


class UnstrictStatusSensor(BaseSensorOperator):
    ui_color = "#73deff"

    template_fields = ("external_task_id", "poll_interval")

    def __init__(
        self,
        *,
        external_task_id: str,
        conn_id: str = "unstructured",
        poll_interval: int = 15,
        deferrable: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.external_task_id = external_task_id
        self.conn_id = conn_id
        self.poll_interval = poll_interval
        self.deferrable = deferrable

    def execute(self, context: Context):

        if self.deferrable:
            self.log.info(f"Deferring Unstructured task {self.external_task_id}")

            self.defer(
                trigger=UnstrictTrigger(
                    task_id=self.external_task_id,
                    conn_id=self.conn_id,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )

        else:
            hook = HttpHook(method="GET", http_conn_id=self.conn_id)

            while True:
                self.log.info("Polling Unstructured task")

                resp = hook.run(f"/tasks/{self.external_task_id}")
                status = resp.json().get("status")

                if status == "completed":
                    return self.external_task_id

                elif status == "failed":
                    raise RuntimeError(
                        f"Unstructured task {self.external_task_id} failed"
                    )

    def execute_complete(
        self,
        context: Context,
        event: dict[str, Any] | None = None,
    ) -> Any:

        if not event or event.get("status") == "failed":
            raise RuntimeError(event.get("error", "Unknown error in UnstrictTrigger"))

        self.log.info(f"Unstructured task {event['task_id']} completed")

        # можно вернуть либо task_id, либо result
        return event.get("result", event["task_id"])
