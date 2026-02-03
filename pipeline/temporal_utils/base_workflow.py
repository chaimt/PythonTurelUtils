from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from temporal_utils.base_activity import TemporalActivityInfo
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.types import CallableAsyncNoParam, ReturnType


@dataclass
class BaseWorkflowStatus:
    """Workflow execution status for queries"""

    phase: str = "initializing"
    error: Optional[str] = None


class BaseWorkflow:
    def __init__(self, status: BaseWorkflowStatus):
        self._status = status
        self._workflow_id: Optional[str] = None

    @workflow.query
    def get_status(self) -> dict:
        """Query current workflow status"""
        return {
            "phase": self._status.phase,
            "error": self._status.error,
        }

    async def create_execute_activity(
        self,
        activity: CallableAsyncNoParam[ReturnType],
        data: TemporalActivityInfo,
        start_to_close_timeout: timedelta,
        retry_policy: RetryPolicy,
        heartbeat_timeout: timedelta,
        task_queue: str,
    ):
        return await workflow.execute_activity(
            activity,
            data,
            start_to_close_timeout=start_to_close_timeout,
            retry_policy=retry_policy,
            heartbeat_timeout=heartbeat_timeout,
            task_queue=task_queue,
        )
