import json
import logging
import os
import sys
import traceback
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv
from global_config import GlobalConfig
from log_helper import OurRitualContext
from opentelemetry import trace
from opentelemetry.trace import format_trace_id

logger = logging.getLogger(__name__)
load_dotenv()


class StdoutFilter(logging.Filter):
    """Filter to allow INFO and below (DEBUG, INFO, WARNING) to pass through to stdout."""

    def filter(self, record):
        return record.levelno < logging.ERROR


class StderrFilter(logging.Filter):
    """Filter to allow ERROR and above (ERROR, CRITICAL) to pass through to stderr."""

    def filter(self, record):
        return record.levelno >= logging.ERROR


class JsonFormatter(logging.Formatter):
    # Map Python log levels to GCP severity levels
    SEVERITY_MAP = {
        "DEBUG": "DEBUG",
        "INFO": "INFO",
        "WARNING": "WARNING",
        "ERROR": "ERROR",
        "CRITICAL": "CRITICAL",
        "NOTSET": "DEFAULT",
    }

    def format(self, record):
        # Map Python log level to GCP severity
        severity = self.SEVERITY_MAP.get(record.levelname, record.levelname)

        log_message = {
            "severity": severity,
            "message": record.getMessage(),
            "timestamp": self.formatTime(record),
            "pid": record.process,
        }
        # Add trace_id if present in the record
        if hasattr(record, "trace_id") and record.trace_id:
            log_message["trace_id"] = record.trace_id

        # Add OurRitualContext as JSON under "ourritual"
        try:
            ourritual_dict = OurRitualContext().to_list()
            if ourritual_dict:
                log_message["ourritual"] = ourritual_dict
        except Exception as e:
            logger.warning(f"Error adding OurRitualContext to record: {e}")
            ourritual_dict = {}

        return json.dumps(log_message)


def get_console_handler():
    """Returns handlers that route INFO and below to stdout, ERROR and above to stderr."""
    handlers = []

    if os.getenv("CONSOLE_JSON") == "True":
        # Handler for INFO and below -> stdout
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(JsonFormatter())
        stdout_handler.addFilter(StdoutFilter())
        stdout_handler.setLevel(logging.DEBUG)
        handlers.append(stdout_handler)

        # Handler for ERROR and above -> stderr
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(JsonFormatter())
        stderr_handler.addFilter(StderrFilter())
        stderr_handler.setLevel(logging.ERROR)
        handlers.append(stderr_handler)

        return handlers
    elif os.getenv("CONSOLE") == "True":
        # Handler for INFO and below -> stdout
        stdout_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(message)s")
        stdout_handler.setFormatter(formatter)
        stdout_handler.addFilter(StdoutFilter())
        stdout_handler.setLevel(logging.DEBUG)
        handlers.append(stdout_handler)

        # Handler for ERROR and above -> stderr
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(formatter)
        stderr_handler.addFilter(StderrFilter())
        stderr_handler.setLevel(logging.ERROR)
        handlers.append(stderr_handler)

        return handlers

    return None


def setup_local_logging():
    # Handler for INFO and below -> stdout
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(level=os.environ.get("LOGLEVEL", "NOTSET"))
    formatter = logging.Formatter("%(message)s")
    stdout_handler.setFormatter(formatter)
    stdout_handler.addFilter(StdoutFilter())

    # Handler for ERROR and above -> stderr
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(level=logging.ERROR)
    stderr_handler.setFormatter(formatter)
    stderr_handler.addFilter(StderrFilter())

    console_handlers = get_console_handler()

    logs_dir = "logs"

    os.makedirs(logs_dir, exist_ok=True)
    file_handler = RotatingFileHandler(logs_dir + "/event_publisher.log", maxBytes=10**6, backupCount=5)
    file_handler.setLevel(level=os.environ.get("FILE_LOGLEVEL", "DEBUG"))
    formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(name)s : %(message)s")
    file_handler.setFormatter(formatter)

    log_to_file_logger = logging.getLogger(__name__)
    log_to_file_logger.setLevel(level="DEBUG")
    log_to_file_logger.addHandler(file_handler)

    return file_handler, stdout_handler, stderr_handler, console_handlers


is_debug = GlobalConfig().get_os_boolean("DEBUG", "False")


def setup_logger(v, log_level, handlers: list[logging.Handler | None]):
    v.setLevel(level=log_level)
    # Ensure propagation is enabled so child loggers use root logger's handlers
    v.propagate = True
    for handler in handlers:
        v.removeHandler(handler)
        v.addHandler(handler)


def setup_log(log_level, main_folder, handlers: list[logging.Handler | None]):
    handlers = [h for h in handlers if h is not None]
    for v in logging.Logger.manager.loggerDict.values():
        if type(v) is logging.Logger and v.name.startswith(f"{main_folder}."):
            setup_logger(v, log_level, handlers)


def setup_log_level(log_level, main_folders: list[str], package_names: list[str], handlers: list[logging.Handler] | None = None):
    for v in logging.Logger.manager.loggerDict.values():
        if type(v) is not logging.Logger:
            continue

        # Set default level to INFO for all loggers when not in debug mode
        if not is_debug:
            v.setLevel(level="INFO")
            v.propagate = True

        # Check if logger matches any main_folder or package_name
        matches_folder = any(v.name == folder or v.name.startswith(f"{folder}.") for folder in main_folders)
        matches_package = v.name in package_names

        if matches_folder or matches_package:
            v.setLevel(level=log_level)
            v.propagate = True
            if handlers:
                for handler in handlers:
                    v.removeHandler(handler)
                    v.addHandler(handler)


def log_exception(exc_type, exc_value, exc_traceback):
    """handle all exceptions"""
    filename, line, dummy, dummy = traceback.extract_tb(exc_traceback).pop()
    filename = os.path.basename(filename)
    error = "%s: %s" % (exc_type.__name__, exc_value)

    # Get trace_id from current span
    span = trace.get_current_span()
    span_context = span.get_span_context()
    trace_id = None
    if span_context.trace_id != 0:  # Check if we have a valid trace_id
        trace_id = format_trace_id(span_context.trace_id)

    # Add trace_id as an extra attribute to all log messages
    extra = {"trace_id": trace_id} if trace_id else {}

    logger.error(":woman_facepalming: " + error, extra=extra)
    logger.error(f"filename: {filename} : {line}", extra=extra)
    if trace_id:
        logger.error(f"trace_id: {trace_id}", extra=extra)
    logger.error(traceback.format_exc(), extra=extra)
