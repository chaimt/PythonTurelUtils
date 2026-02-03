import logging
import os
import sys

from app_config import BaseSettings

logger = logging.getLogger(__name__)


def _is_debugger_attached() -> bool:
    """Check if a debugger is currently attached to the process."""
    # Check if gettrace() returns a trace function (most reliable - indicates debugger is attached)
    if sys.gettrace() is not None:
        return True
    # Check for debugpy (VS Code/Cursor debugger) in sys.modules
    if "debugpy" in sys.modules:
        return True
    # Check for pydevd (PyCharm debugger) in sys.modules
    if "pydevd" in sys.modules:
        return True
    # Check for common debugger environment variables
    if os.getenv("PYTHONBREAKPOINT") or os.getenv("PYCHARM_HOSTED"):
        return True
    # Try to detect debugpy by checking if it's importable (for lazy imports)
    try:
        import debugpy

        if debugpy.is_client_connected():
            return True
    except (ImportError, AttributeError):
        pass
    return False


class TemporalWorkerSettings(BaseSettings):
    """Configuration for Temporal Worker."""

    temporal_url: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "ai-pipeline-queue"
    temporal_api_key: str = ""
    temporal_max_concurrent_activities: int = 1
    temporal_max_concurrent_workflow_tasks: int = 1
    temporal_enable_tls: bool = False
    temporal_tls_cert_path: str = ""
    temporal_tls_key_path: str = ""
    temporal_debug_mode: bool = False

    def __init__(self):
        super().__init__()
        # Auto-enable debug mode if debugger is attached or TEMPORAL_DEBUG_MODE env var is set
        if _is_debugger_attached() or os.getenv("TEMPORAL_DEBUG_MODE", "").lower() in ("true", "1", "yes"):
            self.temporal_debug_mode = True
            logger.info("🐛 Debug mode enabled: Debugger detected or TEMPORAL_DEBUG_MODE is set")
