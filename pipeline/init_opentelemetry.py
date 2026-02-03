import logging
import sys
from typing import Optional

from app_config import AppSettings
from cloud_env import (
    HealthCheckFilter,
    OTELJSONFormatter,
    configure_cloud_logging,
    configure_cloud_monitoring,
    configure_cloud_trace,
    instance_id,
)
from global_config import GlobalConfig
from log_handlers import setup_log_level
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.trace import SpanKind

logger = logging.getLogger(__name__)


def setup_loggers(log_level: str, gcp_project_id: str):
    root_logger = logging.getLogger()
    GlobalConfig().root_logger = root_logger
    root_logger.setLevel(log_level)  # Set root logger level before adding handler
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(log_level)
    stdout_handler.addFilter(lambda record: record.levelno <= logging.INFO)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.ERROR)
    stderr_handler.addFilter(lambda record: record.levelno >= logging.ERROR)

    root_logger.handlers = []
    root_logger.addHandler(stdout_handler)
    root_logger.addHandler(stderr_handler)

    formatter = None
    if AppSettings().running_locally:
        formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] [%(filename)s:%(lineno)d] [pid:%(process)d] - %(message)s")
    else:
        formatter = OTELJSONFormatter(project_id=gcp_project_id)

    stdout_handler.setFormatter(formatter)
    stderr_handler.setFormatter(formatter)

    for logger_name in ["uvicorn", "uvicorn.error", "sentry_sdk", "tenacity"]:
        uv_logger = logging.getLogger(logger_name)
        uv_logger.handlers = []  # Remove any existing handlers first
        uv_logger.setLevel(log_level)  # Set logger level
        # Let these loggers propagate to root logger instead of adding handlers directly
        # This prevents duplicate log messages
        uv_logger.propagate = True

    uvicorn_logger = logging.getLogger("uvicorn.access")
    if uvicorn_logger:
        uvicorn_logger.disabled = True
        uvicorn_logger.addFilter(HealthCheckFilter())

    httpx_logger = logging.getLogger("httpx")
    httpx_logger.handlers = []  # Remove any existing handlers
    httpx_logger.propagate = True  # Let it propagate to root logger
    httpx_logger.setLevel(log_level)  # Set appropriate level

    main_folders = ["src", "users", "app"]
    package_names = [
        "pipeline_utils",
        "fastapi_config",
        "app_config",
        "cloud_env",
        "gcp_files",
        "file_helper",
        "error_handling",
        "llm_utils",
        "pubsub_utils",
        "data_structure",
        "ai_process.insight_process",
    ]

    setup_log_level(
        log_level.upper(),
        main_folders,
        package_names,
        [stdout_handler, stderr_handler],
    )

    # Prevent duplicate logs: loggers that got handlers from setup_log_level should not propagate to root
    # Iterate through all existing loggers and disable propagation for those matching our patterns
    # Get all existing logger names first to avoid modifying dict during iteration
    existing_logger_names = list(logging.Logger.manager.loggerDict.keys())
    for logger_pattern in main_folders:
        # Get the top-level logger for this pattern
        top_logger = logging.getLogger(logger_pattern)
        if top_logger.handlers:  # If it has handlers from setup_log_level
            top_logger.propagate = False  # Don't propagate to avoid duplicates
        # Also check all existing loggers that match this pattern
        for existing_logger_name in existing_logger_names:
            if existing_logger_name.startswith(logger_pattern + ".") or existing_logger_name == logger_pattern:
                existing_logger = logging.getLogger(existing_logger_name)
                if existing_logger.handlers:  # If it has handlers from setup_log_level
                    existing_logger.propagate = False  # Don't propagate to avoid duplicates

    # Also disable propagation for package_names loggers that have handlers
    for pkg_name in package_names:
        pkg_logger = logging.getLogger(pkg_name)
        if pkg_logger.handlers:
            pkg_logger.propagate = False

    # Configure init_opentelemetry logger AFTER setup_log_level to ensure our settings take precedence
    logger.setLevel(log_level)  # Set logger level
    logger.handlers = []  # Remove any existing handlers (including any added by setup_log_level)
    logger.propagate = True  # Let it propagate to root logger to use root logger's handlers (avoids duplicates)


def setup_opentelemetry(service_name: str, service_version: str, log_type: str, log_level: str, gcp_project_id: str):
    setup_loggers(log_level, gcp_project_id)

    logger.info(
        f"🚀 Starting OpenTelemetry setup - Service: {service_name}, Version: {service_version}, Log Type: {log_type}, Log Level: {log_level}"
    )
    is_debug = GlobalConfig().get_os_boolean("DEBUG", "False")
    logger.info(f"🔍 Debug mode: {is_debug}")

    # Setup local tracer when running locally, otherwise use Cloud Trace
    if AppSettings().running_locally:
        logger.info("🏠 Running locally - configuring silent TracerProvider")
        # Use SDK TracerProvider without any span processor for local development
        # This allows Langfuse (or other libraries) to add their own span processor
        # while keeping local trace output silent
        existing_provider = trace.get_tracer_provider()
        if existing_provider is None or isinstance(existing_provider, (trace.NoOpTracerProvider, trace.ProxyTracerProvider)):
            resource = Resource.create({SERVICE_NAME: service_name})
            local_tracer_provider = TracerProvider(resource=resource)
            trace.set_tracer_provider(local_tracer_provider)
        tracer = trace.get_tracer(service_name)
        trace_exporter = None
    else:
        tracer, trace_exporter = configure_cloud_trace(gcp_project_id, service_name, service_version)

    with tracer.start_as_current_span("setup_opentelemetry", kind=SpanKind.SERVER, attributes={"service.name": service_name}):
        GlobalConfig().trace_attributes = {
            "service.name": service_name,
            "service.version": service_version,
            "service.instance.id": instance_id,
        }
        _, logs_exporter, _ = configure_cloud_logging(gcp_project_id, service_name)
        # Initialize metrics only if ENABLE_METRICS is true
        meter = None
        metrics_exporter = None
        logger.info(
            f"🔍 Checking metrics configuration - System Metrics: {AppSettings().enable_system_metrics}, App Metrics: {AppSettings().enable_app_metrics}"
        )
        if AppSettings().enable_system_metrics or AppSettings().enable_app_metrics:
            meter, metrics_exporter = configure_cloud_monitoring(gcp_project_id, service_name, service_version)
        else:
            logger.info("🚫 Metrics disabled - skipping Cloud Monitoring initialization")

        if not is_debug:
            logging.getLogger("opentelemetry").setLevel(logging.INFO)
            logging.getLogger("opentelemetry.sdk").setLevel(logging.INFO)
            logging.getLogger("opentelemetry.sdk.trace").setLevel(logging.INFO)
            logging.getLogger("opentelemetry.exporter.cloud_monitoring").setLevel(logging.WARNING)

        if not AppSettings().running_locally:
            GlobalConfig().trace_exporter = trace_exporter
            GlobalConfig().metrics_exporter = metrics_exporter
            GlobalConfig().logs_exporter = logs_exporter
        else:
            logger.info("🚫 GCP Logging disabled - running locally")

        return tracer, meter


def init_cloud(
    service_name: Optional[str] = None,
    service_version: Optional[str] = None,
    log_type: Optional[str] = "gcp",
    log_level: Optional[str] = None,
    gcp_project_id: Optional[str] = None,
):

    if service_name is None:
        service_name = GlobalConfig().settings.service_name

    if service_version is None:
        service_version = GlobalConfig().settings.version

    if log_type is None:
        log_type = GlobalConfig().settings.log_type

    if log_level is None:
        log_level = GlobalConfig().settings.log_level

    if gcp_project_id is None:
        gcp_project_id = GlobalConfig().settings.gcp_project_id

    GlobalConfig().tracer, GlobalConfig().meter = setup_opentelemetry(
        service_name,
        service_version,
        log_type,
        log_level,
        gcp_project_id,
    )

    # Only create metric counters if metrics are enabled and meter is available
    if GlobalConfig().meter is not None and AppSettings().enable_system_metrics:
        GlobalConfig().request_counter = GlobalConfig().meter.create_counter(
            name=f"{service_name}_requests_total", description="Total number of HTTP requests", unit="requests"
        )

        GlobalConfig().error_counter = GlobalConfig().meter.create_counter(
            name=f"{service_name}_errors_total", description="Total number of Errors", unit="requests"
        )

        GlobalConfig().authentication_error_counter = GlobalConfig().meter.create_counter(
            name=f"{service_name}_authentication_errors_total",
            description="Total number of Authentication Errors",
            unit="requests",
        )
    # PubSubInstrumentationProvider().instrument()
