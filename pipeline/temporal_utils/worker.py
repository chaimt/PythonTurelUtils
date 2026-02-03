"""
Temporal Worker Class for Recording Consumer Service.

This module provides a class-based approach to running Temporal workers
that execute workflows and activities for processing recordings.
"""

import asyncio
import logging
import signal
import threading
import uuid
from typing import Any, Callable, List, Mapping, Optional, Sequence, Type

from temporal_utils.config import TemporalWorkerSettings
from temporalio import activity
from temporalio.client import Client, Interceptor
from temporalio.common import RetryPolicy
from temporalio.contrib.opentelemetry import TracingInterceptor
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)

logger = logging.getLogger(__name__)


class TemporalWorker:
    """
    Temporal Worker class for executing workflows and activities.

    This class manages the lifecycle of a Temporal worker, including:
    - Connecting to Temporal server
    - Registering workflows and activities
    - Handling graceful shutdown
    - Error handling and logging

    Example:
        ```python
        # Basic usage
        worker = TemporalWorker()
        await worker.run()

        # Custom configuration
        config = TemporalWorkerConfig(
            temporal_url="temporal.example.com:7233",
            namespace="production",
            task_queue="my-queue"
        )
        worker = TemporalWorker(config=config)
        await worker.run()

        # Add custom workflows and activities
        worker = TemporalWorker()
        worker.add_workflow(MyCustomWorkflow)
        worker.add_activity(my_custom_activity)
        await worker.run()
        ```
    """

    def __init__(
        self,
        temporal_worker_settings: TemporalWorkerSettings,
        workflows: Optional[List[Type]] = None,
        activities: Optional[List] = None,
        interceptors: Optional[List[Interceptor]] = None,
    ):
        """
        Initialize the Temporal worker.

        Args:
            config: Worker configuration (uses defaults if not provided)
            workflows: List of workflow classes to register
            activities: List of activity functions to register
        """
        self.temporal_worker_settings = temporal_worker_settings
        self.client: Optional[Client] = None
        self.worker: Optional[Worker] = None
        self.interceptors = interceptors or []
        self._shutdown_event = asyncio.Event()

        # Initialize with default workflows and activities
        self.workflows = workflows or []
        self.activities = activities or []

        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()

        logger.info(
            f"🔧 Temporal Worker initialized with config: "
            f"URL={temporal_worker_settings.temporal_url}, "
            f"Namespace={temporal_worker_settings.temporal_namespace}, "
            f"TaskQueue={temporal_worker_settings.temporal_task_queue}"
        )

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"📡 Received signal {signum}, initiating graceful shutdown...")
        self._shutdown_event.set()

    def add_workflow(self, workflow_class: Type):
        """
        Add a workflow class to the worker.

        Args:
            workflow_class: Workflow class decorated with @workflow.defn
        """
        if workflow_class not in self.workflows:
            self.workflows.append(workflow_class)
            logger.info(f"✅ Added workflow: {workflow_class.__name__}")

    def add_activity(self, activity_func):
        """
        Add an activity function to the worker.

        Args:
            activity_func: Activity function decorated with @activity.defn
        """
        if activity_func not in self.activities:
            self.activities.append(activity_func)
            logger.info(f"✅ Added activity: {activity_func.__name__}")

    def add_activities(self, activity_funcs: List):
        """
        Add multiple activity functions to the worker.

        Args:
            activity_funcs: List of activity functions decorated with @activity.defn
        """
        for activity_func in activity_funcs:
            if activity_func not in self.activities:
                self.activities.append(activity_func)
                logger.info(f"✅ Added activity: {activity_func.__name__}")

    async def connect(self) -> Client:
        """
        Connect to Temporal server.

        Returns:
            Connected Temporal client

        Raises:
            Exception: If connection fails
        """
        try:
            logger.info(f"🔌 Connecting to Temporal server at {self.temporal_worker_settings.temporal_url}...")

            # Prepare TLS config if enabled
            # tls_config = None
            # if Settings().temporal_enable_tls:
            #     if not Settings().temporal_tls_cert_path or not Settings().temporal_tls_key_path:
            #         raise ValueError("TLS enabled but cert/key paths not provided")

            #     with open(Settings().temporal_tls_cert_path, "rb") as f:
            #         client_cert = f.read()
            #     with open(Settings().temporal_tls_key_path, "rb") as f:
            #         client_key = f.read()

            #     tls_config = TLSConfig(
            #         client_cert=client_cert,
            #         client_private_key=client_key,
            #     )
            #     logger.info("🔒 TLS enabled for Temporal connection")

            # Connect to Temporal
            self.client = await Client.connect(
                self.temporal_worker_settings.temporal_url,
                namespace=self.temporal_worker_settings.temporal_namespace,
                api_key=self.temporal_worker_settings.temporal_api_key,
                tls=self.temporal_worker_settings.temporal_enable_tls,
                interceptors=self.interceptors + [TracingInterceptor()],
                data_converter=pydantic_data_converter,
            )

            logger.info(f"✅ Connected to Temporal server (namespace: {self.temporal_worker_settings.temporal_namespace})")
            return self.client

        except Exception as e:
            logger.error(f"❌ Failed to connect to Temporal server: {e}")
            raise

    async def create_worker(self) -> Worker:
        """
        Create and configure the Temporal worker.

        Returns:
            Configured Temporal worker

        Raises:
            Exception: If worker creation fails
        """
        if not self.client:
            await self.connect()

        try:
            logger.info(f"🏗️  Creating worker for task queue: {self.temporal_worker_settings.temporal_task_queue}")

            # Configure workflow runner based on debug mode
            worker_kwargs = {
                "task_queue": self.temporal_worker_settings.temporal_task_queue,
                "workflows": self.workflows,
                "activities": self.activities,
                "max_concurrent_activities": self.temporal_worker_settings.temporal_max_concurrent_activities,
                "max_concurrent_workflow_tasks": self.temporal_worker_settings.temporal_max_concurrent_workflow_tasks,
            }

            if self.temporal_worker_settings.temporal_debug_mode:
                logger.warning("⚠️  Running with relaxed sandbox restrictions for debugging - DO NOT USE IN PRODUCTION")
                workflow_runner = SandboxedWorkflowRunner(
                    restrictions=SandboxRestrictions.default.with_passthrough_modules(
                        "_pydevd_bundle",
                        "_pydev_bundle",
                        "debugpy",
                        "pydevd",
                    )
                )
                worker_kwargs["workflow_runner"] = workflow_runner
            else:
                logger.info("✅ Running with default sandbox restrictions (production mode)")

            self.worker = Worker(
                self.client,
                **worker_kwargs,
                # activity_executor
                # workflow_task_executor
            )

            logger.info(f"✅ Worker created with {len(self.workflows)} workflow(s) " f"and {len(self.activities)} activity(ies)")
            logger.info(f"📋 Registered workflows: {[w.__name__ for w in self.workflows]}")
            logger.info(f"📋 Registered activities: {[a.__name__ for a in self.activities]}")

            return self.worker

        except Exception as e:
            logger.error(f"❌ Failed to create worker: {e}")
            raise

    async def run(self):
        """
        Run the Temporal worker.

        This method will block until the worker is shut down via signal
        or exception. It handles graceful shutdown and cleanup.
        """
        try:
            # Create worker if not already created
            if not self.worker:
                await self.create_worker()

            logger.info("🚀 Starting Temporal worker...")
            logger.info(f"👂 Listening for tasks on queue: {self.temporal_worker_settings.temporal_task_queue}")

            # Run worker in background task
            worker_task = asyncio.create_task(self.worker.run())

            # Wait for shutdown signal
            await self._shutdown_event.wait()

            logger.info("🛑 Shutdown signal received, stopping worker...")

            # Cancel worker task
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                logger.info("✅ Worker task cancelled")

        except Exception as e:
            logger.error(f"❌ Worker error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Cleanup resources and shutdown gracefully."""
        logger.info("🧹 Cleaning up resources...")

        if self.worker:
            try:
                # Worker cleanup is handled by cancelling the run task
                logger.info("✅ Worker stopped")
            except Exception as e:
                logger.error(f"⚠️  Error stopping worker: {e}")

        if self.client:
            try:
                await self.client.close()
                logger.info("✅ Temporal client connection closed")
            except Exception as e:
                logger.error(f"⚠️  Error closing client: {e}")

        logger.info("👋 Temporal worker shutdown complete")

    async def run_until_complete(self):
        """
        Run the worker until completion (for testing or one-off execution).

        This is useful for testing scenarios where you want the worker to
        process a specific number of tasks and then stop.
        """
        try:
            if not self.worker:
                await self.create_worker()

            logger.info("🚀 Running Temporal worker until complete...")
            await self.worker.run()

        except Exception as e:
            logger.error(f"❌ Worker error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    def _run_worker_in_thread(self, worker: Worker):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def worker_main():
            logger.info("Temporal worker started in separate thread")
            # Run until stop event is set
            try:
                await worker.run()
            except Exception as e:
                logger.error(f"Worker error: {e}")

        try:
            loop.run_until_complete(worker_main())
        finally:
            loop.close()

    def run_worker_in_thread(self, worker: Worker):
        """Run Temporal worker in a separate thread with its own event loop"""
        worker_thread = threading.Thread(target=self._run_worker_in_thread, daemon=True, args=(worker,))
        worker_thread.start()

    async def create_and_run_worker_in_thread(self):
        worker = await self.create_worker()
        """Run Temporal worker in a separate thread with its own event loop"""
        worker_thread = threading.Thread(target=self._run_worker_in_thread, daemon=True, args=(worker,))
        worker_thread.start()

    async def run_worker_in_task(self, task_name: str):
        worker = await self.create_worker()
        asyncio.create_task(worker.run(), name=task_name),

    @staticmethod
    def generate_id(domain: str, service: str, feature: str, action: str, version: str) -> str:
        """
        Generate a unique ID for a workflow.

        This generates a deterministic prefix with a random suffix for uniqueness.
        For true idempotency, use a deterministic ID based on input data instead.

        Args:
            domain: Domain name (e.g., "ritual")
            service: Service name (e.g., "recording-consumer")
            feature: Feature name (e.g., "zoom-recording")
            action: Action name (e.g., "process")
            version: Version string (e.g., "1")

        Returns:
            Generated workflow ID

        Example:
            >>> TemporalWorker.generate_id("ritual", "recording", "zoom", "process", "1")
            'ritual.recording.zoom.process.v1-a1b2c3d4'
        """
        short_id = uuid.uuid4().hex[:8]
        return f"{domain}.{service}.{feature}.{action}.v{version}-{short_id}"

    @staticmethod
    def log_info(message: str):
        activity.logger.info(message)

    @staticmethod
    def log_debug(message: str):
        activity.logger.debug(message)

    @staticmethod
    def log_warning(message: str):
        activity.logger.warning(message)

    @staticmethod
    def log_error(message: str):
        activity.logger.error(message)

    async def start_workflow(
        self,
        workflow_method,
        id: str,
        args: Sequence[Any] = [],
        headers: Optional[dict] = None,
        retry_policy: Optional[RetryPolicy] = None,
        memo: Optional[Mapping[str, Any]] = None,
    ):
        """Start a workflow with idempotent ID generation

        Args:
            workflow_method: The workflow method to execute (e.g., RecordingConsumerWorkflow.run)
            id: Workflow ID (should be deterministic for idempotency)
            args: Arguments to pass to the workflow
            headers: Optional dictionary of headers to pass to the workflow (e.g., traceparent for tracing)

        Returns:
            WorkflowHandle for the started workflow

        Note:
            For idempotency, ensure the ID is deterministic based on the input data.
            For example: f"recording-{message_id}-{timestamp}"
        """
        if not self.client:
            await self.connect()

        if not self.client:
            raise RuntimeError("Failed to connect to Temporal server")

        logger.info(f"🚀 Starting workflow with ID: {id}")
        if headers:
            logger.debug(f"📤 Headers provided but not passed to start_workflow (tracing handled via OpenTelemetry interceptor): {headers}")

        handle = await self.client.start_workflow(
            workflow_method,
            args=args,
            id=id,
            task_queue=self.temporal_worker_settings.temporal_task_queue,
            retry_policy=retry_policy,
            memo=memo,
        )

        logger.info(f"✅ Workflow started: {id}")

        return handle

    @staticmethod
    def has_running_loop() -> bool:
        """
        Check if there's a running event loop.

        Returns:
            bool: True if an event loop is currently running, False otherwise.
        """
        try:
            asyncio.get_running_loop()
            return True
        except RuntimeError:
            return False

    def execute_workflow(
        self,
        workflow_func: Callable,
        workflow_id: str,
        args: List[Any],
        headers: Optional[dict] = None,
    ) -> None:
        """
        Execute a Temporal workflow with proper async handling.

        This method handles both cases:
        - When called from within an existing event loop (uses create_task)
        - When called without an event loop (uses asyncio.run)

        Args:
            workflow_func: The workflow function to execute (e.g., AIPipelineWorkflow.run)
            workflow_id: Unique identifier for the workflow execution
            args: List of arguments to pass to the workflow
            headers: Optional dictionary of headers to pass to the workflow (e.g., traceparent for tracing)

        Raises:
            RuntimeError: If temporal worker is not initialized
        """
        if TemporalWorker.has_running_loop():
            # We're already in an event loop, create a task
            asyncio.create_task(self.start_workflow(workflow_func, id=workflow_id, args=args, headers=headers))
        else:
            # No event loop, create one
            asyncio.run(self.start_workflow(workflow_func, id=workflow_id, args=args, headers=headers))
