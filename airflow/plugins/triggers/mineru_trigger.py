from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

from airflow.configuration import conf
from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import BaseSensorOperator, Context
from airflow.triggers.base import BaseTrigger, TriggerEvent
from asgiref.sync import sync_to_async


class MineruTrigger(BaseTrigger):
    """
    This is an example of a custom trigger that waits for a binary random choice
    between 0 and 1 to be 1.
    Args:
        poll_interval (int): How many seconds to wait between async polls.
        my_kwarg_passed_into_the_trigger (str): A kwarg that is passed into the trigger.
    Returns:
        my_kwarg_passed_out_of_the_trigger (str): A kwarg that is passed out of the trigger.
    """

    def __init__(
        self,
        task_id: str,
        conn_id: str = "mineru",
        poll_interval: int = 600,
    ):
        super().__init__()
        self.task_id = task_id
        self.conn_id = conn_id
        self.poll_interval = poll_interval
        self.hook = HttpHook(method="GET", http_conn_id=conn_id)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialize MyTrigger arguments and classpath.
        All arguments must be JSON serializable.
        This will be returned by the trigger when it is complete and passed as `event` to the
        `execute_complete` method of the deferrable operator.
        """

        return (
            "triggers.mineru_trigger.MineruStatusTrigger",
            {
                "task_id": self.task_id,
                "mineru_conn_id": self.mineru_conn_id,
                "poke_interval": self.poke_interval,
            },
        )

    @sync_to_async
    def _check_status(self) -> dict:
        """Dont block event-loop"""
        hook = HttpHook(method="GET", http_conn_id=self.mineru_conn_id)
        resp = hook.run(f"/tasks/{self.task_id}")
        return resp.json()

    # The run method is an async generator that yields TriggerEvents when the desired condition is met
    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            try:
                js = self._check_status()
                status = js.get("status")

                if status == "completed":
                    file_names = js.get("file_names")

                    self.log.info(
                        f"Files complited: {'\n'.join(file_names)}\nTriggering event."
                    )

                    # Fire the trigger event! This gets a worker to execute the operator's `execute_complete` method
                    yield TriggerEvent(self.serialize())
                    return  # The return statement prevents the trigger from running again
                elif status == "failed":
                    yield TriggerEvent(
                        {
                            "status": "failed",
                            "error": f"Mineru task {self.task_id} failed",
                        }
                    )
                    return
            except Exception as e:
                self.log.error(f"Polling error for {self.task_id}: {e}")

            self.log.info(
                f"Task {self.task_id} processing. Sleeping for {self.poll_interval} seconds."
            )
            # If the condition is not met, the trigger sleeps for the poll_interval
            # this code can run multiple times until the condition is met
            await asyncio.sleep(self.poll_interval)


class MineruStatusSensor(BaseSensorOperator):
    """
    Deferrable operator that waits for a binary random choice between 0 and 1 to be 1.
    Args:
        wait_for_completion (bool): Whether to wait for the trigger to complete.
        poke_interval (int): How many seconds to wait between polls,
            both in deferrable or sensor mode.
        deferrable (bool): Whether to defer the operator. If set to False,
            the operator will act as a sensor.
    Returns:
        str: A kwarg that is passed through the trigger and returned by the operator.
    """

    ui_color = "#73deff"

    template_fields = ("external_task_id", "poke_interval")

    def __init__(
        self,
        *,
        external_task_id: str,  # task_id from MinerU-endpoint /tasks/{id}
        mineru_conn_id: str = "mineru",
        poke_interval: int = 15,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.external_task_id = external_task_id
        self.mineru_conn_id = mineru_conn_id
        self.poke_interval = poke_interval
        self.deferrable = deferrable

    def execute(self, context: Context):

        # Add code you want to be executed before the deferred part here (this code only runs once)

        # Starting the deferral process
        if self._defer:
            self.log.info(f"Deferring to trigger for task {self.external_task_id}")
            self.defer(
                trigger=MineruTrigger(
                    task_id=self.mineru_task_id,
                    conn_id=self.mineru_conn_id,
                    poke_interval=self.poke_interval,
                ),
                method_name="execute_complete",
            )
        else:  # regular sensor part
            from airflow.providers.http.hooks.http import HttpHook

            hook = HttpHook(method="GET", http_conn_id=self.mineru_conn_id)
            while True:
                self.log.info("Operator in sensor mode. Polling.")
                resp = hook.run(f"/tasks/{self.external_task_id}")
                status = resp.json().get("status")

                if status == "done":
                    return self.external_task_id
                elif status == "failed":
                    raise RuntimeError(f"Mineru task {self.external_task_id} failed")
        # Add code you want to be executed after the deferred part here (this code only runs once)
        # you can have as many deferred parts as you want in an operator

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> str:
        """Execute when the trigger is complete. This code only runs once."""

        if not event or event.get("status") == "failed":
            raise RuntimeError(event.get("error", "Unknown error in MineruTrigger"))

        self.log.info(f"Task {event['task_id']} completed")
        return event["task_id"]
