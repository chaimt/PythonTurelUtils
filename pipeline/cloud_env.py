import json
import logging
import os
import uuid
from typing import TYPE_CHECKING, Any, List, Optional, Union

from llm_utils import format_prompt as pipeline_format_prompt
from pydantic import BaseModel

if TYPE_CHECKING:
    from langfuse import Langfuse

from app_config import AppSettings
from google.api_core import client_options
from google.auth import default
from google.cloud import logging as gcp_logging
from google.cloud import monitoring_v3, trace_v2
from google.cloud.logging_v2.handlers import CloudLoggingHandler
from log_helper import OurRitualContext
from opentelemetry import metrics, trace

try:
    from opentelemetry.exporter.cloud_logging import CloudLoggingExporter
except ImportError:
    CloudLoggingExporter = None
from opentelemetry.exporter.cloud_monitoring import CloudMonitoringMetricsExporter
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.metrics import (
    MeterProvider,
)
from opentelemetry.sdk.metrics.export import (  # noqa: F401
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import (
    SERVICE_NAME,
    SERVICE_VERSION,
    Resource,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import format_trace_id

logger = logging.getLogger(__name__)

# Log warning if CloudLoggingExporter is not available
if CloudLoggingExporter is None:
    logger.warning("CloudLoggingExporter is not available. Cloud logging exporter will be disabled.")

instance_id = str(uuid.uuid4())
os.environ["instance_id"] = instance_id
os.environ["OTEL_RESOURCE_ATTRIBUTES"] = f"service.instance.id={instance_id}"


class HealthCheckFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return "GET / HTTP" not in message and "GET /health" not in message


class OTELCloudLoggingHandler(CloudLoggingHandler):
    # Map Python log levels to GCP severity levels
    SEVERITY_MAP = {
        "DEBUG": "DEBUG",
        "INFO": "INFO",
        "WARNING": "WARNING",
        "ERROR": "ERROR",
        "CRITICAL": "CRITICAL",
        "NOTSET": "DEFAULT",
    }

    def __init__(self, client, project_id=None, **kwargs):
        super().__init__(client, **kwargs)
        self.project_id = project_id or client.project

    def emit(self, record):
        # Add OTEL trace info to the record
        span = trace.get_current_span()
        span_context = span.get_span_context()

        # Always try to add trace context, even for unsampled traces
        if span_context.trace_id != 0:  # Check if we have a valid trace_id
            if not hasattr(record, "extra"):
                record.extra = {}
            record.extra = record.extra or {}
            record.extra["otelTraceID"] = format_trace_id(span_context.trace_id)
            record.extra["otelSpanID"] = format(span_context.span_id, "016x")
            record.extra["otelTraceSampled"] = span_context.trace_flags.sampled
            record.extra["otelTraceValid"] = span_context.is_valid
            if hasattr(record, "otelServiceName"):
                record.extra["otelServiceName"] = record.otelServiceName

            # Add Google Cloud trace field for proper trace-log correlation
            trace_id = format_trace_id(span_context.trace_id)
            if not hasattr(record, "_trace"):
                record._trace = f"projects/{self.project_id}/traces/{trace_id}"

            try:
                ourritual_dict = OurRitualContext().to_list()
                if ourritual_dict:
                    record.extra["ourritual"] = ourritual_dict
            except Exception as e:
                logger.warning(f"Error adding OurRitualContext to record: {e}")

        # Ensure severity is set on the record for GCP Cloud Logging
        # Map Python log level to GCP severity
        if not hasattr(record, "severity"):
            record.severity = self.SEVERITY_MAP.get(record.levelname, record.levelname)

        super().emit(record)


class OTELJSONFormatter(logging.Formatter):
    # Map Python log levels to GCP severity levels
    SEVERITY_MAP = {
        "DEBUG": "DEBUG",
        "INFO": "INFO",
        "WARNING": "WARNING",
        "ERROR": "ERROR",
        "CRITICAL": "CRITICAL",
        "NOTSET": "DEFAULT",
    }

    def __init__(self, project_id=None, **kwargs):
        super().__init__(**kwargs)
        self.project_id = project_id

    def format(self, record):
        # Map Python log level to GCP severity
        severity = self.SEVERITY_MAP.get(record.levelname, record.levelname)

        log_entry = {
            "severity": severity,
            "message": record.getMessage(),
            "timestamp": self.formatTime(record),
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "filename": record.filename,
            "pathname": record.pathname,
            "pid": record.process,
        }

        span = trace.get_current_span()
        span_context = span.get_span_context()
        if span_context.trace_id != 0:  # Check if we have a valid trace_id
            trace_id = format_trace_id(span_context.trace_id)
            log_entry["otelTraceID"] = trace_id
            log_entry["otelSpanID"] = format(span_context.span_id, "016x")
            log_entry["otelTraceSampled"] = span_context.trace_flags.sampled
            log_entry["otelTraceValid"] = span_context.is_valid

            # Add GCP-compatible trace field for proper trace-log correlation in GCP Console
            if self.project_id:
                log_entry["logging.googleapis.com/trace"] = f"projects/{self.project_id}/traces/{trace_id}"

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add extra fields if present
        if hasattr(record, "extra") and record.extra:
            log_entry["extra"] = record.extra

        # Add service instance ID if available
        if "instance_id" in os.environ:
            log_entry["service_instance_id"] = os.environ["instance_id"]

        # Add OurRitualContext as JSON under "ourritual"
        try:
            ourritual_dict = OurRitualContext().to_list()
            if ourritual_dict:
                log_entry["ourritual"] = ourritual_dict
        except Exception as e:
            logger.warning(f"Error adding OurRitualContext to record: {e}")
            ourritual_dict = {}

        return json.dumps(log_entry)


class ServiceNameFormatter(logging.Formatter):
    """Custom formatter that replaces the logger name with the service name."""

    def __init__(self, service_name: str, fmt=None, datefmt=None, style="%"):
        super().__init__(fmt, datefmt, style)
        self.service_name = service_name

    def format(self, record):
        # Store the original name
        original_name = record.name
        # Replace the name with service name
        record.name = self.service_name
        # Format the record
        result = super().format(record)
        # Restore the original name
        record.name = original_name
        return result


# Define a custom reader with timestamp-sorted metrics
class ChronologicalMetricReader(PeriodicExportingMetricReader):
    def _receive_metrics(self, *args, **kwargs):
        metrics_data = super()._receive_metrics(*args, **kwargs)
        if metrics_data and hasattr(metrics_data, "resource_metrics"):
            for rm in metrics_data.resource_metrics:
                for scope_metrics in rm.scope_metrics:
                    for metric in scope_metrics.metrics:
                        if hasattr(metric, "data") and hasattr(metric.data, "points"):
                            # Sort the points chronologically
                            metric.data.points.sort(key=lambda p: p.timestamp)
        return metrics_data


def configure_cloud_logging(project_id: str, service_name: str) -> tuple[logging.Logger, Any, logging.Handler]:
    """Configure Cloud Logging with OpenTelemetry.

    Args:
        log_level: The logging level to set
        project_id: The GCP project ID
        service_name: The name of the service
        service_version: The version of the service
        labels: Optional dictionary of labels to include in resource attributes
    """
    logger = logging.getLogger(service_name)

    logger.debug("🔧 Configuring Cloud Logging with OpenTelemetry...")

    # Set up GCP native logging (for structured logs with severity)
    # Use proper credentials configuration to avoid ALTS warnings
    credentials, _ = default()
    client_opts = client_options.ClientOptions()

    # Disable ALTS for local development to avoid warning messages
    if AppSettings().running_locally:
        client_opts.api_endpoint = "logging.googleapis.com:443"

    gcp_client = gcp_logging.Client(project=project_id, credentials=credentials, client_options=client_opts)
    gcp_handler = OTELCloudLoggingHandler(gcp_client, project_id=project_id)

    # Apply the formatter to the root logger and all existing handlers
    if not AppSettings().running_locally and CloudLoggingExporter is not None:
        cloud_logging_exporter = CloudLoggingExporter(
            project_id=project_id,
        )
    else:
        cloud_logging_exporter = None
        if CloudLoggingExporter is None:
            logger.warning("CloudLoggingExporter is not available. Skipping OpenTelemetry cloud logging exporter setup.")

    # Add a handler to the root logger to log to console
    logger.debug("✅ Cloud Logging configured successfully.")
    return logger, cloud_logging_exporter, gcp_handler


def configure_cloud_trace(project_id: str, service_name: str, service_version: str) -> tuple[trace.Tracer, CloudTraceSpanExporter]:
    """Configure Cloud Trace with OpenTelemetry.

    Args:
        project_id: The GCP project ID
        service_name: The name of the service
        service_version: The version of the service
    """
    logger.debug("🔧 Configuring Cloud Trace with OpenTelemetry...")

    try:
        # Create Cloud Trace client with proper credentials configuration to avoid ALTS warnings
        credentials, _ = default()
        client_opts = client_options.ClientOptions()

        # Disable ALTS for local development to avoid warning messages
        if AppSettings().running_locally:
            client_opts.api_endpoint = "cloudtrace.googleapis.com:443"

        trace_client = trace_v2.TraceServiceClient(credentials=credentials, client_options=client_opts)

        # Configure OpenTelemetry trace provider with more detailed resource attributes
        attributes = {
            SERVICE_NAME: service_name,
            SERVICE_VERSION: service_version,
            "instance_id": instance_id,
        }

        resource = Resource.create(attributes)

        # Configure sampling - use 100% sampling for development, adjust for production
        # Set OTEL_TRACE_SAMPLING_RATIO=1.0 for 100% sampling, 0.1 for 10% sampling, etc.

        tracer_provider = TracerProvider(resource=resource)
        # # Only set if not already configured to avoid "Overriding of current TracerProvider is not allowed" error
        existing_provider = trace.get_tracer_provider()
        if existing_provider is None or isinstance(existing_provider, trace.ProxyTracerProvider):
            logger.info("Setting tracer provider")
            trace.set_tracer_provider(tracer_provider)

        # Create and register Cloud Trace exporter with error handling
        try:
            cloud_trace_exporter = CloudTraceSpanExporter(project_id=project_id, client=trace_client)
            span_processor = BatchSpanProcessor(cloud_trace_exporter)
            tracer_provider.add_span_processor(span_processor)  # Export every 5 seconds)

            # Instrument the Pub/Sub client library
            # GoogleCloudPubSubInstrumentor().instrument()

        except Exception as e:
            logger.error(f"❌ Error creating Cloud Trace exporter: {str(e)}")
            raise

        logger.debug("✅ Cloud Trace configured successfully.")
        return tracer_provider.get_tracer(service_name, attributes=attributes), cloud_trace_exporter
    except Exception as e:
        logger.error(f"❌ Error configuring Cloud Trace: {str(e)}")
        raise


def configure_cloud_monitoring(project_id: str, service_name: str, service_version: str) -> tuple[metrics.Meter, CloudMonitoringMetricsExporter]:
    """Configure Cloud Monitoring with OpenTelemetry."""
    logger.debug("🔧 Configuring Cloud Monitoring with OpenTelemetry...")

    # Create Cloud Monitoring client with proper credentials configuration to avoid ALTS warnings
    credentials, _ = default()
    client_opts = client_options.ClientOptions()

    # Disable ALTS for local development to avoid warning messages
    if AppSettings().running_locally:
        client_opts.api_endpoint = "monitoring.googleapis.com:443"

    # Configure longer timeout for gRPC calls to prevent deadline exceeded errors
    # Default gRPC timeout is often 60 seconds, but we'll set it to 120 seconds for Celery workers
    from google.api_core import timeout

    timeout_config = timeout.ExponentialTimeout(initial=120.0, maximum=120.0, multiplier=1.0)
    client_opts.timeout = timeout_config

    monitoring_client = monitoring_v3.MetricServiceClient(credentials=credentials, client_options=client_opts)

    # Configure OpenTelemetry metrics
    attributes = {
        SERVICE_NAME: service_name,
        SERVICE_VERSION: service_version,
        "instance_id": instance_id,
    }

    resource = Resource.create(attributes)

    # Create the Cloud Monitoring exporter
    exporter = CloudMonitoringMetricsExporter(
        project_id=project_id,
        client=monitoring_client,
    )

    # Create the metric reader with longer timeout to prevent deadline exceeded errors
    # Increased timeout to 120 seconds for Celery workers that may have slower network connections
    reader = ChronologicalMetricReader(exporter, export_interval_millis=60000, export_timeout_millis=120000)

    # reader = PeriodicExportingMetricReader(exporter, export_interval_millis=60000, export_timeout_millis=30000)

    # Set up the meter provider
    meter_provider = MeterProvider(metric_readers=[reader], resource=resource)
    metrics.set_meter_provider(meter_provider)

    logger.debug("✅ Cloud Monitoring configured successfully.")
    return meter_provider.get_meter(service_name, attributes=attributes), exporter


class PromptConfig(BaseModel):
    model: str
    temperature: float


class PromptInfo(BaseModel):
    name: str
    prompt: Union[str, list]
    labels: list
    version: int
    config: dict

    def get_config(self) -> PromptConfig:
        return PromptConfig(model=self.config.get("model"), temperature=self.config.get("temperature"))

    def format_prompt(self, kwargs: dict) -> Union[str, list[Any]]:
        return pipeline_format_prompt(self.prompt, kwargs)


def get_langfuse_prompts(tag: str, label: Optional[str] = None, version: Optional[int] = None) -> List[PromptInfo]:
    from langfuse import get_client

    prompts = []
    if AppSettings().env == "dev" and not label:
        label = "latest"
    if AppSettings().env == "prod" and not label:
        label = "production"

    logger.info(f"Getting {tag} tag: Label: {label} / Version: {version} [env: {AppSettings().env}]")
    if AppSettings().env == "dev" and not label:
        label = "latest"
    if version == 0:
        version = None

    langfuse_client = get_client()
    response = langfuse_client.api.prompts.list(label=label, tag=tag)
    for prompt in response.data:
        prompt_info = get_langfuse_prompt(prompt.name, label=label, version=version)
        prompts.append(prompt_info)
        # PromptInfo(name = prompt.name, prompt: Union[str, list]

    logger.info(f"{len(prompts)} prompts found for {tag} tag: Labels: {prompt_info.labels} / Version: {prompt_info.version}")
    return prompts


def get_langfuse_prompt(name: str, label: Optional[str] = None, version: Optional[int] = None) -> PromptInfo:
    from langfuse import get_client

    logger.info(f"Getting {name} prompt: Label: {label} / Version: {version} [env: {AppSettings().env}]")
    if AppSettings().env == "dev" and not label:
        label = "latest"
    if AppSettings().env == "prod" and not label:
        label = "production"
    if version == 0:
        version = None

    langfuse_client = get_client()
    prompt_info = langfuse_client.get_prompt(name, label=label, version=version, cache_ttl_seconds=AppSettings().cache_ttl_seconds)
    logger.info(f"{name} prompt: Labels: {prompt_info.labels} / Version: {prompt_info.version}")
    return PromptInfo(
        name=name, prompt=prompt_info.get_langchain_prompt(), labels=prompt_info.labels, version=prompt_info.version, config=prompt_info.config
    )


def init_langfuse_client(
    public_key: str, secret_key: str, base_url: str, environment: str, blocked_instrumentation_scopes=None
) -> tuple["Langfuse", TracerProvider]:
    from langfuse import Langfuse

    # Create an isolated TracerProvider for Langfuse
    # This ensures only spans explicitly sent to this provider go to Langfuse
    # Do NOT register this as the global provider
    langfuse_tracer_provider = TracerProvider()
    if not blocked_instrumentation_scopes:
        blocked_instrumentation_scopes = [
            "opentelemetry.instrumentation.requests",
            "opentelemetry.instrumentation.google_cloud_pubsub",
            "opentelemetry.instrumentation.fastapi",
            "google.cloud.pubsub_v1",
            "grpc",
            "init_opentelemetry",
            "langfuse.opentelemetry",
            "opentelemetry.sdk",
        ]
    langfuse_client = Langfuse(
        public_key=public_key,
        secret_key=secret_key,
        base_url=base_url,
        environment=environment,
        tracer_provider=langfuse_tracer_provider,  # Use isolated provider
        blocked_instrumentation_scopes=blocked_instrumentation_scopes,
    )

    return langfuse_client, langfuse_tracer_provider
