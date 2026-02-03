"""
Context Propagation Interceptor for Temporal.

This module provides interceptors that automatically propagate OurRitualContext
between workflows and activities using headers, similar to OpenTelemetry tracing.

The interceptor:
1. Captures the current OurRitualContext when an activity is called from a workflow
2. Serializes it using to_header() and injects it into the activity headers
3. Restores the context in the activity using from_header() before execution

Context Propagation Between Activities:
---------------------------------------
When activities modify OurRitualContext (e.g., via set_custom_info() or add_to_context()),
those changes need to be explicitly returned to the workflow. This is because:
1. Activities run in separate worker threads/processes with isolated contextvar contexts
2. Workflows run in a sandboxed environment that cannot access external mutable state
3. The workflow sandbox isolation prevents sharing state between activity and workflow workers

To propagate context updates from activities back to workflows, use the
ActivityResultWithContext helper:

    from src.temporal.interceptors.context_propagator import (
        ActivityResultWithContext,
        capture_context_header,
    )

    @activity.defn
    async def my_activity(data: MyData) -> ActivityResultWithContext:
        # ... do work ...
        SessionInformation(...).add_to_context()
        return ActivityResultWithContext(
            result=my_result,
            context_header=capture_context_header()
        )

Then in the workflow, restore context from the result:

    from src.temporal.interceptors.context_propagator import restore_context_from_header

    result = await workflow.execute_activity(my_activity, data, ...)
    if result.context_header:
        restore_context_from_header(result.context_header)
    actual_result = result.result
"""

import logging
from dataclasses import dataclass, replace
from typing import Any, Dict, Mapping, Optional, Type

from log_helper import OurRitualContext
from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Interceptor as ClientInterceptor
from temporalio.client import OutboundInterceptor as ClientOutboundInterceptor
from temporalio.client import StartWorkflowInput
from temporalio.converter import PayloadConverter
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    ExecuteWorkflowInput,
)
from temporalio.worker import Interceptor as WorkerInterceptor
from temporalio.worker import (
    StartActivityInput,
    StartChildWorkflowInput,
    StartLocalActivityInput,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)

logger = logging.getLogger(__name__)

# Header key for OurRitualContext propagation
CONTEXT_HEADER_KEY = "ourritual-context"


@dataclass
class ActivityResultWithContext:
    """
    Helper dataclass for activities that need to return context updates to workflows.

    When an activity modifies OurRitualContext (e.g., via add_to_context()), wrap the
    result with this class to propagate context changes back to the workflow.

    Usage in activity:
        session_info.add_to_context()
        return ActivityResultWithContext(
            result=zoom_info,
            context_header=capture_context_header()
        )

    Usage in workflow:
        result = await workflow.execute_activity(my_activity, data, ...)
        if result.context_header:
            restore_context_from_header(result.context_header)
        actual_result = result.result

    Attributes:
        result: The actual result of the activity
        context_header: Optional base64url-encoded context header string
    """

    result: Any
    context_header: Optional[str] = None


def capture_context_header() -> str:
    """
    Capture the current OurRitualContext as a header string.

    This utility function can be used by activities that modify context
    and need to return the updated context to the workflow.

    Returns:
        Base64url-encoded context header string
    """
    return OurRitualContext().to_header()


def restore_context_from_header(header_value: str) -> None:
    """
    Restore OurRitualContext from a header string.

    This utility function can be used by workflows to restore context
    from activity results.

    Args:
        header_value: Base64url-encoded context header string
    """
    if header_value:
        OurRitualContext().from_header(header_value)


class ContextActivityInboundInterceptor(ActivityInboundInterceptor):
    """
    Inbound interceptor for activities that restores OurRitualContext from headers.

    This interceptor:
    1. Extracts the context header from incoming activity calls
    2. Restores the OurRitualContext before the activity executes

    Note: Context updates made during activity execution are NOT automatically
    propagated back to the workflow. Activities that modify context should use
    ActivityResultWithContext to explicitly return context updates.
    """

    # Use PayloadConverter like OpenTelemetry interceptor does
    payload_converter = PayloadConverter.default

    def _restore_context_from_dict(self, context_dict: Dict[str, str]) -> None:
        """
        Restore OurRitualContext from a dictionary.

        Args:
            context_dict: Dictionary with keys like 'ourritual/topic/name', 'ourritual/key', etc.
        """
        ctx = OurRitualContext()
        for key, value in context_dict.items():
            if key == "ourritual/topic/name":
                ctx.set_topic_info(value)
            elif key.startswith("ourritual/") and key != "ourritual/test":
                # Strip 'ourritual/' prefix for custom info
                custom_key = key[len("ourritual/") :]
                ctx.set_custom_info(custom_key, value)

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        """
        Execute the activity after restoring OurRitualContext from headers.

        Args:
            input: The activity execution input containing headers

        Returns:
            The result of the activity execution
        """
        try:
            activity_info = activity.info()

            # Extract context header from INPUT headers (not activity_info.headers!)
            # This matches the OpenTelemetry interceptor pattern
            if input.headers and CONTEXT_HEADER_KEY in input.headers:
                header_payload = input.headers[CONTEXT_HEADER_KEY]
                if header_payload:
                    try:
                        # Decode using PayloadConverter like OpenTelemetry interceptor
                        context_dict = self.payload_converter.from_payloads([header_payload])[0]
                        if context_dict and isinstance(context_dict, dict):
                            self._restore_context_from_dict(context_dict)
                            logger.debug(f"Restored OurRitualContext in activity {activity_info.activity_type}")
                    except Exception as e:
                        logger.warning(f"Failed to restore OurRitualContext from header: {e}")

        except Exception as e:
            logger.debug(f"Could not extract context header: {e}")

        # Execute the actual activity
        return await super().execute_activity(input)


class ContextWorkflowOutboundInterceptor(WorkflowOutboundInterceptor):
    """
    Outbound interceptor for workflows that injects OurRitualContext into headers.

    This interceptor captures the current OurRitualContext and injects it into headers
    when starting activities or child workflows.

    Note: To propagate context updates from activities back to workflows, activities
    must use ActivityResultWithContext to explicitly return context updates, and
    workflows must call restore_context_from_header() with the returned context.
    """

    # Use PayloadConverter like OpenTelemetry interceptor does for proper encoding
    payload_converter = PayloadConverter.default

    def _get_context_headers(self) -> Mapping[str, Payload]:
        """
        Get the current OurRitualContext as headers.

        Returns:
            Dictionary with context header key and Payload value
        """
        try:
            context = OurRitualContext()
            # Use to_list() to get a dict - matches OpenTelemetry pattern
            context_dict = context.to_list()
            if context_dict:
                # Use PayloadConverter like OpenTelemetry interceptor does
                return {CONTEXT_HEADER_KEY: self.payload_converter.to_payloads([context_dict])[0]}
        except Exception as e:
            logger.debug(f"Could not serialize OurRitualContext: {e}")
        return {}

    def _merge_headers(self, existing_headers: Optional[Mapping[str, Payload]]) -> Mapping[str, Payload]:
        """
        Merge context headers with existing headers.

        Always uses the latest context, overriding any existing context header
        to ensure context updates between activities are propagated.

        Args:
            existing_headers: Optional existing headers to merge with

        Returns:
            Merged headers dictionary with latest context
        """
        context_headers = self._get_context_headers()
        if existing_headers:
            merged = dict(existing_headers)
            # Override with latest context headers to propagate updates
            merged.update(context_headers)
            return merged
        return context_headers if context_headers else {}

    def start_activity(self, input: StartActivityInput) -> workflow.ActivityHandle[Any]:
        """
        Start an activity with OurRitualContext injected into headers.
        """
        # Create new input with merged headers using dataclass replace for forward compatibility
        new_input = replace(input, headers=self._merge_headers(input.headers))
        return super().start_activity(new_input)

    def start_local_activity(self, input: StartLocalActivityInput) -> workflow.ActivityHandle[Any]:
        """
        Start a local activity with OurRitualContext injected into headers.
        """
        # Create new input with merged headers using dataclass replace for forward compatibility
        new_input = replace(input, headers=self._merge_headers(input.headers))
        return super().start_local_activity(new_input)

    def start_child_workflow(self, input: StartChildWorkflowInput) -> workflow.ChildWorkflowHandle[Any, Any]:
        """
        Start a child workflow with OurRitualContext injected into headers.
        """
        # Create new input with merged headers using dataclass replace for forward compatibility
        new_input = replace(input, headers=self._merge_headers(input.headers))
        return super().start_child_workflow(new_input)


class ContextWorkflowInboundInterceptor(WorkflowInboundInterceptor):
    """
    Inbound interceptor for workflows that restores OurRitualContext and
    sets up outbound context propagation.
    """

    # Use PayloadConverter like OpenTelemetry interceptor does
    payload_converter = PayloadConverter.default

    def _restore_context_from_dict(self, context_dict: Dict[str, str]) -> None:
        """
        Restore OurRitualContext from a dictionary.

        Args:
            context_dict: Dictionary with keys like 'ourritual/topic/name', 'ourritual/key', etc.
        """
        ctx = OurRitualContext()
        for key, value in context_dict.items():
            if key == "ourritual/topic/name":
                ctx.set_topic_info(value)
            elif key.startswith("ourritual/") and key != "ourritual/test":
                # Strip 'ourritual/' prefix for custom info
                custom_key = key[len("ourritual/") :]
                ctx.set_custom_info(custom_key, value)

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        """
        Execute the workflow after restoring OurRitualContext from headers.
        """
        try:
            workflow_info = workflow.info()

            # Extract and restore context from workflow headers using PayloadConverter
            if workflow_info.headers and CONTEXT_HEADER_KEY in workflow_info.headers:
                header_payload = workflow_info.headers[CONTEXT_HEADER_KEY]
                if header_payload:
                    try:
                        # Decode using PayloadConverter like OpenTelemetry interceptor
                        context_dict = self.payload_converter.from_payloads([header_payload])[0]
                        if context_dict and isinstance(context_dict, dict):
                            self._restore_context_from_dict(context_dict)
                            logger.debug(f"Restored OurRitualContext in workflow {workflow_info.workflow_type}")
                    except Exception as e:
                        logger.warning(f"Failed to restore OurRitualContext from header: {e}")
        except Exception as e:
            logger.debug(f"Could not extract context header from workflow: {e}")

        return await super().execute_workflow(input)

    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        """
        Initialize the outbound interceptor chain.

        This wraps the outbound interceptor with our context propagation interceptor.
        Following the OpenTelemetry interceptor pattern: wrap outbound FIRST,
        then pass to super().init().
        """
        super().init(ContextWorkflowOutboundInterceptor(outbound))


class ContextClientOutboundInterceptor(ClientOutboundInterceptor):
    """
    Client outbound interceptor that injects OurRitualContext into workflow headers.

    This interceptor captures the current OurRitualContext when workflows are started
    from the client and injects it into the headers.
    """

    # Use PayloadConverter like OpenTelemetry interceptor does for proper encoding
    payload_converter = PayloadConverter.default

    def _get_context_headers(self) -> Mapping[str, Payload]:
        """
        Get the current OurRitualContext as headers.

        Returns:
            Dictionary with context header key and Payload value
        """
        try:
            context = OurRitualContext()
            # Use to_list() to get a dict - this matches the OpenTelemetry pattern
            # of storing a carrier dict instead of an encoded string
            context_dict = context.to_list()
            if context_dict:
                # Use PayloadConverter like OpenTelemetry interceptor does
                # This properly encodes the dict as JSON with correct metadata
                return {CONTEXT_HEADER_KEY: self.payload_converter.to_payloads([context_dict])[0]}
        except Exception as e:
            logger.debug(f"Could not serialize OurRitualContext: {e}")
        return {}

    def _merge_headers(self, existing_headers: Optional[Mapping[str, Payload]]) -> Mapping[str, Payload]:
        """
        Merge context headers with existing headers.

        Always uses the latest context, overriding any existing context header
        to ensure context updates are propagated.

        Args:
            existing_headers: Optional existing headers to merge with

        Returns:
            Merged headers dictionary with latest context
        """
        context_headers = self._get_context_headers()
        if existing_headers:
            merged = dict(existing_headers)
            # Override with latest context headers to propagate updates
            merged.update(context_headers)
            return merged
        return context_headers if context_headers else {}

    async def start_workflow(self, input: StartWorkflowInput) -> Any:
        """
        Start a workflow with OurRitualContext injected into headers.
        """
        merged_headers = self._merge_headers(input.headers)
        new_input = replace(input, headers=merged_headers)
        return await super().start_workflow(new_input)


class ContextPropagatorInterceptor(ClientInterceptor, WorkerInterceptor):
    """
    Main interceptor class for OurRitualContext propagation.

    This interceptor provides both workflow and activity interceptors that work
    together to propagate OurRitualContext across Temporal workflow boundaries.

    It implements both client and worker interceptor interfaces so it can be passed
    to Client.connect() and will automatically be used for workers as well.

    Usage:
        client = await Client.connect(
            "localhost:7233",
            interceptors=[ContextPropagatorInterceptor()]
        )

        worker = Worker(
            client,
            task_queue="my-queue",
            workflows=[MyWorkflow],
            activities=[my_activity],
        )
    """

    def intercept_client(self, next: ClientOutboundInterceptor) -> ClientOutboundInterceptor:
        """
        Return the client outbound interceptor for context propagation.

        This wraps the outbound interceptor to inject OurRitualContext
        when workflows are started from the client.
        """
        return ContextClientOutboundInterceptor(next)

    def workflow_interceptor_class(self, input: WorkflowInterceptorClassInput) -> Optional[Type[WorkflowInboundInterceptor]]:
        """
        Return the workflow interceptor class for context propagation.
        """
        return ContextWorkflowInboundInterceptor

    def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
        """
        Return the activity interceptor for context restoration.
        """
        return ContextActivityInboundInterceptor(next)
