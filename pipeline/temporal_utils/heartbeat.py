"""
Temporal Heartbeat Utilities for Long-Running Operations.
This module provides utilities for sending heartbeats during long-running
API requests and operations to prevent Temporal activity timeouts.
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Callable, Optional

from temporalio import activity

logger = logging.getLogger(__name__)


class HeartbeatManager:
    """
    Manages periodic heartbeats for long-running Temporal activities.

    This class provides both synchronous and asynchronous context managers
    for automatically sending heartbeats at regular intervals during
    long-running operations.

    Example:
        ```python
        # Async usage
        async with HeartbeatManager(interval=5.0, details="Processing data"):
            result = await long_running_api_call()

        # Sync usage
        with HeartbeatManager.sync(interval=5.0, details="Downloading file"):
            result = download_large_file()
        ```
    """

    def __init__(
        self,
        interval: float = 10.0,
        details: Optional[Any] = None,
        on_heartbeat: Optional[Callable[[Any], None]] = None,
    ):
        """
        Initialize heartbeat manager.

        Args:
            interval: Seconds between heartbeats (default: 10.0)
            details: Optional details to include with each heartbeat
            on_heartbeat: Optional callback function called on each heartbeat
        """
        self.interval = interval
        self.details = details
        self.on_heartbeat = on_heartbeat
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._heartbeat_count = 0
        self._start_time = 0.0

    async def _heartbeat_loop(self):
        """Internal loop that sends periodic heartbeats."""
        try:
            while not self._stop_event.is_set():
                try:
                    # Calculate elapsed time
                    elapsed = time.time() - self._start_time
                    self._heartbeat_count += 1

                    # Prepare heartbeat details
                    heartbeat_details = {
                        "count": self._heartbeat_count,
                        "elapsed_seconds": round(elapsed, 2),
                    }

                    if self.details:
                        if isinstance(self.details, dict):
                            heartbeat_details |= self.details
                        else:
                            heartbeat_details["message"] = str(self.details)

                    # Send heartbeat
                    activity.heartbeat(heartbeat_details)

                    logger.debug(f"💓 Heartbeat #{self._heartbeat_count} sent " f"(elapsed: {elapsed:.1f}s)")

                    # Call custom callback if provided
                    if self.on_heartbeat:
                        self.on_heartbeat(heartbeat_details)

                except Exception as e:
                    logger.warning(f"⚠️  Heartbeat error: {e}")

                # Wait for next interval or stop event
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval)
                except asyncio.TimeoutError:
                    # Timeout is expected, continue loop
                    pass

        except asyncio.CancelledError:
            logger.debug("Heartbeat loop cancelled")
            raise

    async def __aenter__(self):
        """Start heartbeat loop when entering async context."""
        self._start_time = time.time()
        self._stop_event.clear()
        self._task = asyncio.create_task(self._heartbeat_loop())

        # Send initial heartbeat
        activity.heartbeat({"status": "started", "message": str(self.details) if self.details else "Operation started"})
        logger.debug(f"💓 Started heartbeat manager (interval: {self.interval}s)")

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Stop heartbeat loop when exiting async context."""
        self._stop_event.set()

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        # Send final heartbeat
        elapsed = time.time() - self._start_time
        final_status = "completed" if exc_type is None else "failed"
        activity.heartbeat(
            {
                "status": final_status,
                "total_heartbeats": self._heartbeat_count,
                "total_elapsed_seconds": round(elapsed, 2),
            }
        )

        logger.debug(f"💓 Stopped heartbeat manager " f"(sent {self._heartbeat_count} heartbeats over {elapsed:.1f}s)")

        return False  # Don't suppress exceptions

    @classmethod
    @contextmanager
    def sync(
        cls,
        interval: float = 10.0,
        details: Optional[Any] = None,
        on_heartbeat: Optional[Callable[[Any], None]] = None,
    ):
        """
        Synchronous context manager for heartbeats.

        Uses a background thread to send heartbeats for synchronous operations.

        Args:
            interval: Seconds between heartbeats
            details: Optional details to include with each heartbeat
            on_heartbeat: Optional callback function called on each heartbeat

        Example:
            ```python
            with HeartbeatManager.sync(interval=5.0, details="Processing"):
                result = synchronous_long_operation()
            ```
        """
        import threading

        stop_event = threading.Event()
        heartbeat_count = [0]  # Use list for mutability in closure
        start_time = time.time()

        def heartbeat_thread():
            """Thread function that sends periodic heartbeats."""
            while not stop_event.is_set():
                try:
                    elapsed = time.time() - start_time
                    heartbeat_count[0] += 1

                    heartbeat_details = {
                        "count": heartbeat_count[0],
                        "elapsed_seconds": round(elapsed, 2),
                    }

                    if details:
                        if isinstance(details, dict):
                            heartbeat_details.update(details)
                        else:
                            heartbeat_details["message"] = str(details)

                    activity.heartbeat(heartbeat_details)

                    logger.debug(f"💓 Heartbeat #{heartbeat_count[0]} sent " f"(elapsed: {elapsed:.1f}s)")

                    if on_heartbeat:
                        on_heartbeat(heartbeat_details)

                except Exception as e:
                    logger.warning(f"⚠️  Heartbeat error: {e}")

                # Wait for interval or stop event
                stop_event.wait(timeout=interval)

        # Send initial heartbeat
        activity.heartbeat({"status": "started", "message": str(details) if details else "Operation started"})

        # Start heartbeat thread
        thread = threading.Thread(target=heartbeat_thread, daemon=True)
        thread.start()
        logger.debug(f"💓 Started sync heartbeat manager (interval: {interval}s)")

        try:
            yield
        finally:
            # Stop heartbeat thread
            stop_event.set()
            thread.join(timeout=interval + 1.0)

            # Send final heartbeat
            elapsed = time.time() - start_time
            activity.heartbeat(
                {
                    "status": "completed",
                    "total_heartbeats": heartbeat_count[0],
                    "total_elapsed_seconds": round(elapsed, 2),
                }
            )

            logger.debug(f"💓 Stopped sync heartbeat manager " f"(sent {heartbeat_count[0]} heartbeats over {elapsed:.1f}s)")


@asynccontextmanager
async def heartbeat_context(
    interval: float = 10.0,
    details: Optional[Any] = None,
    on_heartbeat: Optional[Callable[[Any], None]] = None,
):
    """
    Async context manager for sending periodic heartbeats.

    This is a convenience function that creates and manages a HeartbeatManager.

    Args:
        interval: Seconds between heartbeats
        details: Optional details to include with each heartbeat
        on_heartbeat: Optional callback function called on each heartbeat

    Example:
        ```python
        async with heartbeat_context(interval=5.0, details="API call in progress"):
            result = await long_api_request()
        ```
    """
    async with HeartbeatManager(interval, details, on_heartbeat) as manager:
        yield manager


def send_progress_heartbeat(
    current: int,
    total: int,
    operation: str = "Processing",
    extra_details: Optional[dict] = None,
):
    """
    Send a heartbeat with progress information.

    Args:
        current: Current progress value
        total: Total value for completion
        operation: Description of the operation
        extra_details: Optional additional details to include

    Example:
        ```python
        for i, item in enumerate(items):
            send_progress_heartbeat(i + 1, len(items), "Processing items")
            process_item(item)
        ```
    """
    percentage = (current / total * 100) if total > 0 else 0

    details = {
        "operation": operation,
        "current": current,
        "total": total,
        "percentage": round(percentage, 2),
    }

    if extra_details:
        details |= extra_details

    activity.heartbeat(details)
    logger.debug(f"💓 Progress: {current}/{total} ({percentage:.1f}%)")


async def with_heartbeat(
    func: Callable,
    *args,
    interval: float = 10.0,
    details: Optional[Any] = None,
    **kwargs,
):
    """
    Execute an async function with automatic heartbeats.

    Args:
        func: Async function to execute
        *args: Positional arguments for the function
        interval: Seconds between heartbeats
        details: Optional details to include with each heartbeat
        **kwargs: Keyword arguments for the function

    Returns:
        Result of the function execution

    Example:
        ```python
        result = await with_heartbeat(
            long_api_call,
            url,
            timeout=300,
            interval=5.0,
            details="Calling external API"
        )
        ```
    """
    async with heartbeat_context(interval=interval, details=details):
        return await func(*args, **kwargs)
