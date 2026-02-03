import logging
import os
from typing import Any, Optional

from google.cloud import pubsub_v1
from google.cloud.storage.bucket import Bucket
from opentelemetry import metrics, trace
from opentelemetry.exporter.cloud_monitoring import CloudMonitoringMetricsExporter
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.metrics import Counter
from opentelemetry.trace import format_trace_id
from single_instance_metaclass import singleton

logger = logging.getLogger(__name__)


def get_safe_trace_id() -> str:
    """
    Safely get the current span's trace ID to avoid "Calling end() on an ended span" errors.

    Returns:
        str: The trace ID as a string, or empty string if not available
    """
    try:
        current_span = trace.get_current_span()
        if current_span and current_span.get_span_context().is_valid:
            sc = current_span.get_span_context()
            version = "00"
            trace_id = f"{sc.trace_id:032x}"  # 32-char lowercase hex
            span_id = f"{sc.span_id:016x}"  # 16-char lowercase hex
            trace_flags = f"{int(sc.trace_flags):02x}"  # 2-char lowercase hex

            traceparent = f"{version}-{trace_id}-{span_id}-{trace_flags}"
            return traceparent
    except Exception as e:
        logger.warning(f"⚠️ Could not get trace ID from current span: {e}")
    return ""


@singleton
class GlobalConfig:
    settings = None
    authentication_token: str = "undefined"
    bucket: Bucket = None
    request_counter: Counter = None
    error_counter: Counter = None
    authentication_error_counter: Counter = None
    tracer: trace.Tracer = None
    meter: metrics.Meter = None
    subscriber: Optional[pubsub_v1.SubscriberClient] = None
    incoming_topic_path: Optional[str] = None
    incoming_replay_topic_path: Optional[str] = None
    publisher: Optional[pubsub_v1.PublisherClient] = None
    outgoing_topic_path: Optional[str] = None
    management_topic_path: Optional[str] = None
    dlq_subscription: Optional[str] = None
    trace_exporter: Optional[CloudTraceSpanExporter] = None
    metrics_exporter: Optional[CloudMonitoringMetricsExporter] = None
    logs_exporter: Optional[logging.Handler] = None
    trace_attributes: Optional[dict] = None
    temporal_client: Optional[Any] = None
    root_logger: Optional[logging.Logger] = None

    def get_trace_attributes(self):
        span_context = trace.get_current_span().get_span_context()
        self.trace_attributes["trace_id"] = format_trace_id(span_context.trace_id)
        self.trace_attributes["traceparent"] = get_safe_trace_id()
        return self.trace_attributes or {}

    @staticmethod
    def get_os_boolean(key, default=None):
        return os.getenv(key, default).strip().lower() in {"true", "1", "yes"}

    def to_dict(self):
        return {
            "authentication_token": self.authentication_token,
            "bucket": self.bucket.name if self.bucket else None,
            "incoming_topic_path": self.incoming_topic_path,
            "incoming_replay_topic_path": self.incoming_replay_topic_path,
            "outgoing_topic_path": self.outgoing_topic_path,
            "management_topic_path": self.management_topic_path,
            "dlq_subscription": self.dlq_subscription,
        }
